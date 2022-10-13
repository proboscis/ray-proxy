import asyncio
import uuid
from abc import ABC, abstractmethod
from asyncio import Event, Future, Task
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime
from pickle import PickleError
from typing import List, Generic, TypeVar, Dict, Union, Set, Callable, Awaitable, Any, Tuple, Coroutine, Optional

import pandas as pd
import ray
from ray.actor import ActorHandle
from returns.result import Failure

from pinject_design import Injected
from pinject_design.di.ast import Expr, Object, GetItem, Attr, Call

T = TypeVar("T")


class IResourceHandle(Generic[T], ABC):
    @abstractmethod
    def free(self):
        pass

    @property
    @abstractmethod
    def value(self) -> T:
        pass

    @property
    @abstractmethod
    def id(self) -> uuid.UUID:
        pass

    @property
    @abstractmethod
    def consuming_resources(self) -> Dict[str, List["IResourceHandle"]]:
        pass


Resources = Dict[str, List[IResourceHandle]]


@dataclass
class LambdaResourceHandle(IResourceHandle[T]):
    _id: uuid.UUID
    _value: T
    _consuming_resources: Resources
    _free: Callable[[T], None] = field(default=lambda value: None)

    @property
    def value(self) -> T:
        return self._value

    @property
    def id(self) -> uuid.UUID:
        return self._id

    def free(self):
        return self._free(self.value)

    @property
    def consuming_resources(self) -> Resources:
        return self._consuming_resources


class ClusterTask(ABC):

    @property
    @abstractmethod
    def required_resources(self) -> Dict[str, int]:
        pass

    @abstractmethod
    def run(self, resources: dict) -> Awaitable:
        pass


class ClusterFunc(ABC):
    @property
    @abstractmethod
    def required_resources(self) -> Dict[str, int]:
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs) -> ClusterTask:
        pass


U = TypeVar("U")


@dataclass
class InjectedClusterFunc(ClusterFunc):
    src: Injected[Callable[[T], U]]
    ray_options: dict = field(default_factory=dict)

    @property
    def required_resources(self) -> Dict[str, int]:
        return {k: 1 for k in self.src.dependencies()}

    def __call__(self, *args, **kwargs) -> ClusterTask:
        return LambdaClusterTask(
            self.required_resources,
            lambda res, *_args, **_kwargs: self.src.get_provider()(**{k: v[0] for k, v in res.items()})(*_args,
                                                                                                        **_kwargs),
            args,
            kwargs,
            self.ray_options
        )


@dataclass
class LambdaClusterTask(ClusterTask):
    _required_resources: Dict[str, int]
    f: Callable[[Resources], Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    ray_options: dict = field(default_factory=dict)

    @property
    def required_resources(self) -> Dict[str, int]:
        return self._required_resources

    def run(self, resources: Resources) -> Awaitable:
        print(f"calling lambda cluster task:{resources},{self.args},{self.kwargs}")
        values = {k: [item.value for item in v] for k, v in resources.items()}
        from archpainter.picklability_checker import assert_picklable
        assert_picklable(
            dict(func=self.f, values=values)
        )
        remote_f = ray.remote(self.f).options(**self.ray_options)
        # print(f"created task:{remote_f}")
        res = remote_f.remote(values, *self.args, **self.kwargs)
        # print(f"ray task is submitted as :{res}")
        return res


@dataclass
class QueuedTask:
    task: ClusterTask
    future: Future


class IResourceFactory(Generic[T], ABC):
    @abstractmethod
    async def create(self, resources: dict, executor: ThreadPoolExecutor) -> IResourceHandle[T]:
        pass

    @property
    @abstractmethod
    def required_resources(self):
        pass

    @abstractmethod
    def num_available(self) -> int:
        pass


@dataclass
class LambdaResourceFactory(IResourceFactory[T]):
    _required_resources: Dict[str, int]
    _factory: Callable[[Resources, ThreadPoolExecutor], Awaitable[T]]
    remaining: int  # TODO move this state to scheduler
    destructor: Optional[Callable[[T], None]] = field(default=None)

    async def create(self, resources: Resources, executor: ThreadPoolExecutor) -> IResourceHandle[T]:
        value = await self._factory(resources, executor)
        return LambdaResourceHandle(
            uuid.uuid4(),
            value,
            resources,
            self._destructor,
        )

    def num_available(self) -> int:
        return self.remaining

    @property
    def required_resources(self):
        return self._required_resources

    def _destructor(self, value: T):
        if self.destructor is not None:
            self.destructor(value)


class ResourceScope(ABC):
    @abstractmethod
    def to_keep(self, handle: IResourceHandle) -> bool:
        pass


class OnDemandScope(ResourceScope):

    def to_keep(self, handle: IResourceHandle) -> bool:
        return False


class ReservedScope(ResourceScope):
    def to_keep(self, handle: IResourceHandle) -> bool:
        return True


@dataclass
class ScopedResourceFactory:
    factory: IResourceFactory
    scope: ResourceScope


@dataclass
class ClusterTaskScheduler:
    resource_pool: Dict[str, List[IResourceHandle]] = field(default_factory=lambda: defaultdict(list))
    resource_scopes: Dict[str, ResourceScope] = field(default_factory=dict)
    resource_sources: Dict[str, IResourceFactory] = field(default_factory=dict)
    resource_in_use_count: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    # TODO combined these states into single dataclass
    job_queue: List[QueuedTask] = field(default_factory=list)
    background_internal_tasks: Set[Task] = field(default_factory=set)
    running_tasks: Set[Task] = field(default_factory=set)
    last_task_submission_time: datetime = field(default=None)

    def __post_init__(self):
        self.reschedule_event = Event()
        self.scheduling_task = None
        # WARNING: this executor is introduced due to the fact that remote resource creation results in resource leak.
        # instead we run multiple resource factory functions simultaneously inside thread.
        # so the number of running simultaneous resource creation task is limited by this executor.
        self.resource_creation_executor = ThreadPoolExecutor()

    def status(self):
        resource_counts = {k: len(v) for k, v in self.resource_pool.items()}
        num_availables = {k: v.num_available() for k, v in self.resource_sources.items()}
        import pandas as pd
        return pd.DataFrame([resource_counts, self.resource_in_use_count, num_availables],
                            index=["in_pool", "in_use", "issuable"]).T

    def all_status(self):
        return dict(
            resource_pool=self.resource_pool,
            resource_scopes=self.resource_scopes,
            resource_sources=self.resource_sources,
            resource_in_use_count=self.resource_in_use_count,
            job_queue=[repr(t) for t in self.job_queue],
            background_tasks=[repr(t) for t in self.background_internal_tasks],
            last_task_submission_time=self.last_task_submission_time,
        )

    def register_resource_factory(self, key, factory, scope):
        self.resource_sources[key] = factory
        self.resource_scopes[key] = scope
        self.resource_pool[key] = []
        self.resource_in_use_count[key] = 0

    def find_missing_resources(self, reqs: Dict[str, int]):
        resource_counts = {k: len(v) for k, v in self.resource_pool.items()}
        num_availables = {k: v.num_available() for k, v in self.resource_sources.items()}

        def gather(key, amt):
            # print(f"trying to gather {amt} {key}")
            in_pool = resource_counts[key]
            missing = max(0, amt - in_pool)
            # print(f"found {missing} {key} missing from pool")
            fac = self.resource_sources[key]
            while missing > 0:
                reqs = fac.required_resources
                if num_availables[key]:
                    num_availables[key] -= 1
                    for key, amt in reqs.items():
                        gather(key, amt)
                else:
                    # print(f"insufficient availability. by {key}")
                    resource_counts[key] -= 1
                missing -= 1
            resource_counts[key] -= min(in_pool, amt)

        for k, amt in reqs.items():
            gather(k, amt)
        # print(f"resource count: {resource_counts}")
        return {k: v for k, v in resource_counts.items() if v < 0}

    def _get_resource(self, reqs: Dict[str, int]) -> Task:
        """
        obtain resources in blocking manner.
        resource number calculation is done in this thread
        creation is done on the executor.
        returns a task for asyncio
        :param reqs:
        :return:
        """
        # obtaining a resource is a blocking operation
        missings = self.find_missing_resources(reqs)
        if missings:
            print(f"missing resources: {missings}")
            raise RuntimeError(f"missing resources:{missings}")

        # these complicated implementation here is to let the resource allocated as a task without waiting.
        # can I do this without this complication?

        async def pure(x):
            return x

        async def get_dict(keys, tasks: List[Task]):
            vals = await asyncio.gather(*tasks)
            return dict(zip(keys, vals))

        def resolve_request(req: dict) -> Task:
            keys, nums = req.keys(), req.values()
            tasks = []
            for key, amt in zip(keys, nums):
                tasks.append(resolve(key, amt))
            return self.run_background(get_dict(keys, tasks))

        async def retrieve_resource(fac: IResourceFactory, resource_task: Task) -> IResourceHandle:
            res = await resource_task
            # TODO avoid making resource on remote to prevent resource leakage.
            # TODO awaiting the resource creation here caused resource leak.
            try:
                return await fac.create(res, self.resource_creation_executor)
            except Exception as e:
                fac: LambdaResourceFactory
                fac.remaining += 1
                raise e

        def get_resource_background(fac) -> Task:
            resource_task: Task = resolve_request(fac.required_resources)
            fac: LambdaResourceFactory
            fac.remaining -= 1
            return self.run_background(retrieve_resource(fac, resource_task))

        async def to_coro(task):
            return await task

        async def wait_all_tasks(tasks, res_key, amt):
            failures = []
            succeeded = []
            for t in tasks:
                try:
                    res: IResourceHandle = await t
                    succeeded.append(res)
                except Exception as e:
                    import traceback
                    traceback_str = ray._private.utils.format_error_message(traceback.format_exc())
                    failures.append((e,traceback_str))
            if failures:

                for r in succeeded:
                    self.free_single_resource(res_key, r)
                self.resource_in_use_count[res_key] -= len(failures)
                for e,trc in failures:
                    print(f"-------resource allocation failure due to :{e}------------------")
                    print(trc)
                    print(f"----------------------------------------------------------------")
                raise RuntimeError(f"could not allocate requested {amt} resource ({res_key}) for task: {[t[0] for t in failures]}")
            return succeeded

        def resolve(key: str, amt: int) -> Task:
            self.resource_in_use_count[key] += amt
            pool = self.resource_pool[key]
            obtained = [self.run_background(pure(x)) for x in pool[:amt]]
            self.resource_pool[key] = pool[len(obtained):]
            missing = max(0, amt - len(obtained))

            fac = self.resource_sources[key]
            for i in range(missing):
                # create a task to create a missing things
                resource_task = get_resource_background(fac)
                obtained.append(resource_task)
            return self.run_background(wait_all_tasks(obtained, key, amt))

        return resolve_request(reqs)

    async def get_resource(self, req: Dict[str, int]) -> Resources:
        """this is for external use."""
        return await(self._get_resource(req))

    async def start(self):
        if self.scheduling_task is None:
            self.scheduling_task = asyncio.create_task(self._schedule_loop())

            async def periodic_check():
                while True:
                    await asyncio.sleep(3)
                    if self.job_queue:
                        if self.last_task_submission_time is None:
                            self.reschedule_event.set()
                            self._check_remaining_jobs()
                        elif datetime.now() - self.last_task_submission_time > pd.Timedelta("10 seconds"):
                            self.reschedule_event.set()
                            self._check_remaining_jobs()

            self.periodic_check_task = asyncio.create_task(periodic_check())

    async def schedule_task(self, task: ClusterTask):
        future = Future()
        queued = QueuedTask(task, future)
        self.job_queue.append(queued)
        self.reschedule_event.set()
        res = await future
        return res

    async def schedule_flow(self, flow: Expr[Union[ClusterFunc, Any]]):
        """
        so, we can schedule a flow of cluster task.
        now what we need to do is to convert the flow of injected functions and values to
        ClusterTask/Argument
        """

        async def eval_ast(ast: Expr[ClusterFunc]):
            async def eval_tuple(args):
                return await asyncio.gather(*[eval_ast(a) for a in args])

            async def eval_dict(kwargs: dict):
                keys, values = kwargs.keys(), kwargs.values()
                values = await eval_tuple(values)
                return zip(keys, values)

            match ast:
                case Object(ClusterFunc() as task):
                    return task
                case Object(o):
                    return o
                case GetItem(data, key):
                    data, key = await asyncio.gather(eval_ast(data), eval_ast(key))
                    if isinstance(data, InjectedClusterFunc):
                        return replace(data, src=data.src.map(lambda x: x[key]))
                    return data[key]
                case Attr(data, str() as name):
                    data = await eval_ast(data)
                    if isinstance(data, InjectedClusterFunc):
                        return replace(data, src=data.src.map(lambda x: getattr(x, name)))
                    return getattr(data, name)
                case Call(func, args, kwargs):
                    func, args, values = await asyncio.gather(eval_ast(func), eval_tuple(args), eval_dict(kwargs))
                    if isinstance(func, ClusterFunc):
                        task = func(*args,
                                    **kwargs)  # function call happens here. but I think it should be done remotely.
                        return await self.schedule_task(task)
                    else:
                        @ray.remote
                        def remote_call(func, args, kwargs):
                            return func(*args, **kwargs)

                        return await remote_call.remote(func, args, kwargs)

        return await eval_ast(flow)

    def free_resources(self, used_resources: Resources):
        for key, resources in used_resources.items():
            for r in resources:
                self.free_single_resource(key, r)

    def free_single_resource(self, key, handle: IResourceHandle):
        r = handle
        if not self.resource_scopes[key].to_keep(r):
            fac: LambdaResourceFactory = self.resource_sources[key]
            fac.remaining += 1
            r.free()
            self.free_resources(r.consuming_resources)
        else:
            self.resource_pool[key].append(r)
        self.resource_in_use_count[key] -= 1

    def _check_remaining_jobs(self):
        for item in self.job_queue:
            missings = self.find_missing_resources(item.task.required_resources)
            if missings:
                print(f"job not run due to missing resource:{missings}")

    def run_background(self, task: Coroutine, group: set = None) -> Task:
        if group is None:
            group = self.background_internal_tasks
        _task = asyncio.create_task(task)
        group.add(_task)
        _task.add_done_callback(group.discard)
        return _task

    async def invoke_task(self, item, resources_task):
        resource = None
        try:
            resource = await resources_task
            # TODO free failed resource allocations
            result = await item.task.run(resource)
            item.future.set_result(result)
        except Exception as e:
            item.future.set_exception(e)
        finally:
            if resource is not None:
                print(f"freeing resources")
                self.free_resources(resource)
            self.reschedule_event.set()

    async def _schedule_loop(self):
        print(f"cluster task scheduler started")
        while True:
            # print(f"waiting for an event to check runnable tasks")
            await self.reschedule_event.wait()
            print(f"--- job check ({len(self.running_tasks)}/{len(self.job_queue)}) ---")
            print(self.status())
            submitted = []
            for item in self.job_queue:
                if not self.find_missing_resources(item.task.required_resources):
                    print(f"_______________resource allocation_________________")
                    resources_task = self._get_resource(item.task.required_resources)
                    print(self.status())
                    print(f"_______________resource allocation done________________")
                    self.run_background(self.invoke_task(item, resources_task), self.running_tasks)
                    self.last_task_submission_time = datetime.now()
                    submitted.append(item)
            for item in submitted:
                self.job_queue.remove(item)

            print(f"-----------------")
            self.reschedule_event.clear()

    def __repr__(self):
        return f"ClusterTaskScheduler({len(self.running_tasks)}/{len(self.job_queue)})"

    def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None):
        pool = self.resource_pool[key]
        for i in range(amt):
            if pool:
                r = pool.pop()
                fac: LambdaResourceFactory = self.resource_sources[key]
                fac.remaining += 1
                if manual_destructor is not None:
                    manual_destructor(r)
                r.free()
                self.free_resources(r.consuming_resources)


class RemoteTaskScheduler:
    actor: ActorHandle

    def __init__(self, src_actor: ActorHandle):
        self.actor = src_actor

    @staticmethod
    def create(factories: Dict[str, Tuple[IResourceFactory, ResourceScope]] = None) -> "RemoteTaskScheduler":
        factories = factories or {}
        actor = ray.remote(ClusterTaskScheduler).remote()
        sch = RemoteTaskScheduler(actor)
        for key, (fac, scope) in factories.items():
            sch.add_factory(key, fac, scope)
        ray.get(actor.start.remote())
        return sch

    def add_factory(self, name, factory: IResourceFactory, scope: ResourceScope):
        return ray.get(self.actor.register_resource_factory.remote(name, factory, scope))

    def add_factory_lambda(self, name, reqs: Dict[str, int],
                           factory: Callable[[Resources, ThreadPoolExecutor], Awaitable],
                           n_issuable: int,
                           scope: Union[ResourceScope, str],
                           destructor: Optional[Callable[[T], None]] = None
                           ):
        match scope:
            case "ondemand":
                scope = OnDemandScope()
            case "reserved":
                scope = ReservedScope()
            case ResourceScope():
                pass
            case _:
                raise ValueError(f"unknown resource scope:{scope}")
        self.add_factory(name, LambdaResourceFactory(reqs, factory, n_issuable, destructor), scope)

    def submit(self, resources: Dict[str, int], task):
        return self.actor.schedule_task.remote(LambdaClusterTask(resources, task))

    def status(self):
        return ray.get(self.actor.status.remote())

    def get_resources(self, **request: int):
        return ray.get(self.actor.get_resource.remote(request))

    def free_resources(self, resources: Resources):
        return ray.get(self.actor.free_resources.remote(resources))

    def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None):
        return ray.get(self.actor.free_pool.remote(key, amt, manual_destructor))
