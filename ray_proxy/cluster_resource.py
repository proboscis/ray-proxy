import asyncio
import uuid
from abc import ABC, abstractmethod
from asyncio import Event, Future, Task
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Generic, TypeVar, Dict, Union, Set, Callable, Awaitable, Any

import ray
from expression import Result
from ray import ObjectRef

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
    def consuming_resources(self)->Dict[str,List["IResourceHandle"]]:
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



@dataclass
class ClusterTask(ABC):

    @property
    @abstractmethod
    def required_resources(self) -> Dict[str, int]:
        pass

    @abstractmethod
    def run(self, resources: dict) -> Awaitable:
        pass


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
        # print(f"trying to make a remote function:{self.f}")
        remote_f = ray.remote(self.f).options(**self.ray_options)
        # print(f"created task:{remote_f}")
        values = {k: [item.value for item in v] for k, v in resources.items()}
        res = remote_f.remote(values, *self.args, **self.kwargs)
        # print(f"ray task is submitted as :{res}")
        return res


@dataclass
class QueuedTask:
    task: ClusterTask
    future: Future


class IResourceFactory(Generic[T], ABC):
    @abstractmethod
    def create(self, resources: dict) -> IResourceHandle[T]:
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
    _factory: Callable[[Resources], T]
    remaining: int
    destructor: Callable[[T], None] = field(default=lambda x: None)

    def create(self, resources: dict) -> IResourceHandle[T]:
        value = self._factory(resources)
        self.remaining -= 1
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

    def _destructor(self, value:T):
        self.destructor(value)
        self.remaining += 1


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
    job_queue: List[QueuedTask] = field(default_factory=list)
    background_tasks: Set[Task] = field(default_factory=set)

    def __post_init__(self):
        self.reschedule_event = Event()
        self.scheduling_task = None

    def status(self):
        resource_counts = {k: len(v) for k, v in self.resource_pool.items()}
        num_availables = {k: v.num_available() for k, v in self.resource_sources.items()}
        import pandas as pd
        return pd.DataFrame([resource_counts,self.resource_in_use_count,num_availables],index=["in_pool","in_use","issuable"])



    def register_resource_factory(self, key, factory, scope):
        self.resource_sources[key] = factory
        self.resource_scopes[key] = scope
        self.resource_pool[key] = []

    def find_missing_resources(self, reqs: Dict[str, int]):
        resource_counts = {k: len(v) for k, v in self.resource_pool.items()}
        num_availables = {k: v.num_available() for k, v in self.resource_sources.items()}

        def gather(key, amt):
            in_pool = resource_counts[key]
            missing = max(0, amt - in_pool)
            fac = self.resource_sources[key]
            while missing > 0:
                # print(f'checking for missing:{key}:{missing}')
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

    def get_resource(self, reqs: Dict[str, int]):
        missings = self.find_missing_resources(reqs)
        if missings:
            print(f"missing resources: {missings}")
            raise RuntimeError(f"missing resources:{missings}")

        def resolve(key: str, amt: int):
            self.resource_in_use_count[key] += amt
            if len(self.resource_pool[key]) >= amt:
                pool: List = self.resource_pool[key]
                obtained = pool[:amt]
                self.resource_pool[key] = pool[amt:]
                return obtained
            else:  # we are missing some resources...
                missing = max(0, amt - len(self.resource_pool[key]))
                obtained: List = self.resource_pool[key][:amt]
                self.resource_pool[key] = self.resource_pool[key][amt:]
                fac = self.resource_sources[key]
                res = obtained
                for i in range(missing):
                    resources = {k: resolve(k, n_res) for k, n_res in fac.required_resources.items()}
                    resource = fac.create(resources)
                    res.append(resource)
                return res

        return {key: resolve(key, n_res) for key, n_res in reqs.items()}

    async def start(self):
        if self.scheduling_task is None:
            self.scheduling_task = asyncio.create_task(self._schedule_loop())

            async def periodic_check():
                while True:
                    await asyncio.sleep(3)
                    print(f"checking task schedule periodically...")
                    self.reschedule_event.set()

            self.periodic_check_task = asyncio.create_task(periodic_check())

    async def schedule_task(self, task: ClusterTask):
        future = Future()
        queued = QueuedTask(task, future)
        self.job_queue.append(queued)
        self.reschedule_event.set()
        res = await future
        return res

    async def set_future_result(self, coroutine, dest: Future, used_resources: Resources):
        try:
            res = await coroutine
            dest.set_result(res)
        except Exception as e:
            dest.set_exception(e)
        finally:
            self._free_resources(used_resources)
            self.reschedule_event.set()

    def _free_resources(self,used_resources:Resources):
        for key, resources in used_resources.items():
            for r in resources:
                if not self.resource_scopes[key].to_keep(r):
                    r.free()
                    self._free_resources(r.consuming_resources)
                else:
                    self.resource_pool[key].append(r)
                self.resource_in_use_count[key] -= 1

    async def _schedule_loop(self):
        print(f"cluster task scheduler started")
        while True:
            # print(f"waiting for an event to check runnable tasks")
            await self.reschedule_event.wait()
            print(f"--- job check ({len(self.background_tasks)}/{len(self.job_queue)}) ---")
            print(self.status())
            submitted = []
            for item in self.job_queue:
                if not self.find_missing_resources(item.task.required_resources):
                    resources = self.get_resource(item.task.required_resources)
                    result = item.task.run(resources)
                    task = asyncio.create_task(self.set_future_result(result, item.future, resources))
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.discard)
                    submitted.append(item)
            for item in submitted:
                print(f"submitted job:{item}")
                self.job_queue.remove(item)
            for item in self.job_queue:
                print(
                    f"job not run due to missing resource:{self.find_missing_resources(item.task.required_resources)}")

            print(f"-----------------")
            self.reschedule_event.clear()



@dataclass
class TaskResult:
    id: uuid.UUID
    value: Result
