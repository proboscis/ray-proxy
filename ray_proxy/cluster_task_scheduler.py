import asyncio
from asyncio import Task, Event, Future
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Coroutine, Optional, Union, Any, Callable, Awaitable

import pandas as pd
import ray
from cytoolz import valmap, groupby
from ray import ObjectRef
from ray.actor import ActorHandle

from pinject_design.di.ast import Expr, Object, GetItem, Attr, Call
from ray_proxy.cluster_resource import ResourceState, ResourceRequest, Resources, IResourceHandle, ClusterTask, \
    ClusterFunc, InjectedClusterFunc, IResourceFactory, ResourceScope, T, OnDemandScope, ReservedScope, \
    LambdaResourceFactory, LambdaClusterTask


@dataclass
class ClusterTaskScheduler:
    resource_states: Dict[str, ResourceState] = field(default_factory=dict)
    request_queue: List[ResourceRequest] = field(default_factory=list)
    background_internal_tasks: Set[Task] = field(default_factory=set)
    running_tasks: Set[Task] = field(default_factory=set)
    last_task_submission_time: datetime = field(default=None)

    running_flows: List = field(default_factory=list)

    def __post_init__(self):
        self.reschedule_event = Event()
        self.scheduling_task = None
        # WARNING: this executor is introduced due to the fact that remote resource creation results in resource leak.
        # instead we run multiple resource factory functions simultaneously inside thread.
        # so the number of running simultaneous resource creation task is limited by this executor.
        self.resource_creation_executor = ThreadPoolExecutor()

    def status(self):
        num_in_pools = self.pool_counts()
        num_issuables = self.issuables()
        use_counts = valmap(lambda v: v.use_count, self.resource_states)
        import pandas as pd
        return pd.DataFrame([num_in_pools, use_counts, num_issuables],
                            index=["in_pool", "in_use", "issuable"]).T

    def all_status(self):
        return dict(
            states=self.resource_states,
            job_queue=[repr(t) for t in self.request_queue],
            background_tasks=[repr(t) for t in self.background_internal_tasks],
            last_task_submission_time=self.last_task_submission_time,
        )

    def register_resource_factory(self, key, factory, scope):
        self.resource_states[key] = ResourceState(factory, scope)

    def pool_counts(self):
        return {k: v.num_pooled() for k, v in self.resource_states.items()}

    def issuables(self):
        return {k: v.num_issuable() for k, v in self.resource_states.items()}

    def find_missing_resources(self, request: Dict[str, int]):
        resource_counts = self.pool_counts()
        num_availables = self.issuables()

        def gather(key, amt):
            # print(f"trying to gather {amt} {key}")
            in_pool = resource_counts[key]
            missing = max(0, amt - in_pool)
            # print(f"found {missing} {key} missing from pool")
            fac = self.resource_states[key].source
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

        for k, amt in request.items():
            gather(k, amt)
        # print(f"resource count: {resource_counts}")
        return {k: v for k, v in resource_counts.items() if v < 0}

    def _get_resource(self, reqs: Dict[str, int]) -> Task:
        """
        obtain resources in blocking manner.
        resource number calculation is done in this thread
        creation is done on the executor.
        returns a task for asyncio.
        raises an error immidiately if no sufficient resources are present.
        :param reqs:
        :return:
        """
        # obtaining a resource is a blocking operation
        missings = self.find_missing_resources(reqs)
        if missings:
            print(f"missing resources: {missings}")
            raise RuntimeError(f"missing resources:{missings}")

        async def wait_key_coro_pairs(pairs: List[Tuple[str, Coroutine]]) -> Resources:
            keys, coros = [t[0] for t in pairs], [t[1] for t in pairs]
            tasks = [asyncio.create_task(c) for c in coros]
            succeeded = []
            failures = []
            for key, t in zip(keys, tasks):
                try:
                    res: IResourceHandle = await t
                    succeeded.append((key, res))
                except Exception as e:
                    import traceback
                    traceback_str = ray._private.utils.format_error_message(traceback.format_exc())
                    failures.append((key, e, traceback_str))
            if failures:
                for k, r in succeeded:
                    self.free_single_resource(k, r)
                for k, e, trc in failures:
                    print(f"-------resource allocation of {k} failed due to :{e}------------------")
                    print(trc)
                    print(f"----------------------------------------------------------------")
                raise RuntimeError(
                    f"could not allocate requested resources ({[k[:2] for k in failures]})")
            resources = valmap(lambda tuples: [t[1] for t in tuples],
                               groupby(lambda x: x[0], zip(keys, [v[1] for v in succeeded])))
            return resources

        def resolve_request(request: Dict[str, int]) -> Coroutine:
            key_coro_pairs = []
            for key, amt in request.items():
                creations = [(key, self.resource_states[key].obtain(resolve_request, self.resource_creation_executor))
                             for _ in range(amt)]
                key_coro_pairs += creations
            return wait_key_coro_pairs(key_coro_pairs)

        return self.run_background(resolve_request(reqs))

    async def get_resource(self, req: Dict[str, int]) -> Resources:
        """this is for external use."""
        return await(self._get_resource(req))

    async def start(self):
        if self.scheduling_task is None:
            self.scheduling_task = asyncio.create_task(self._schedule_loop())

            async def periodic_check():
                while True:
                    await asyncio.sleep(3)
                    if self.request_queue:
                        if self.last_task_submission_time is None:
                            self.reschedule_event.set()
                            self._check_remaining_jobs()
                        elif datetime.now() - self.last_task_submission_time > pd.Timedelta("10 seconds"):
                            self.reschedule_event.set()
                            self._check_remaining_jobs()

            self.periodic_check_task = asyncio.create_task(periodic_check())

    async def schedule_task(self, task: ClusterTask):
        """schedule a resource, run the task after acquisition, free the resources"""
        resources = await self.schedule_resource(
            task.required_resources)  # resource allocation errors are handled in parent.
        try:
            res = task.run(resources)
        finally:
            self.free_resources(resources)
            self.reschedule_event.set()
        return res

    def _parse_timeout(self, timeout: Optional[Union[str, timedelta, float]]) -> Optional[float]:
        match timeout:
            case str():
                return self._parse_timeout(pd.Timedelta(timeout))
            case timedelta():
                return timeout.total_seconds()
            case float():
                return timeout
            case None:
                return None

    async def schedule_resource(self, request: Dict[str, int],
                                timeout: Optional[Union[str, timedelta, float]] = None) -> Resources:
        """
        schedule for resources in this scheduler.
        a user must free the resources after use!
        :param request:
        :return:
        """
        request = ResourceRequest(request, Future())
        self.request_queue.append(request)
        self.reschedule_event.set()
        return await asyncio.wait_for(request.result, self._parse_timeout(timeout))

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

        self.running_flows.append(flow)
        try:
            return await eval_ast(flow)
        finally:
            self.running_flows.remove(flow)

    def _free_resources(self, used_resources: Resources):
        for key, resources in used_resources.items():
            for r in resources:
                self.free_single_resource(key, r)

    def free_resources(self, used_resources: Resources):
        self._free_resources(used_resources)
        self.reschedule_event.set()

    def free_single_resource(self, key, handle: IResourceHandle):
        if key in self.resource_states:
            unstocked = self.resource_states[key].stock(handle)
            if unstocked is not None:
                self.free_resources(unstocked.consuming_resources)
        else:
            self.free_resources(handle.consuming_resources)



    def run_background(self, task: Coroutine, group: set = None) -> Task:
        if group is None:
            group = self.background_internal_tasks
        _task = asyncio.create_task(task)
        group.add(_task)
        _task.add_done_callback(group.discard)
        return _task

    async def assign_resources(self, request: ResourceRequest, resource_task: Task):
        res = await resource_task
        request.result.set_result(res)

    async def _schedule_loop(self):
        print(f"cluster task scheduler started")
        while True:
            # print(f"waiting for an event to check runnable tasks")
            await self.reschedule_event.wait()
            print(
                f"--- job check ({len(self.running_tasks)}/{len(self.request_queue)}/{len(self.running_flows)}) == running/queued/flows ---")
            print(self.status())
            submitted = []
            if not self.request_queue:
                print(f"---- no job queued for execution ----")
            for item in self.request_queue:
                if not self.find_missing_resources(item.request):
                    # print(f"_______________resource allocation_________________")
                    resources_task = self._get_resource(item.request)
                    # print(self.status())
                    # print(f"_______________resource allocation done________________")
                    self.run_background(self.assign_resources(item, resources_task), self.running_tasks)
                    self.last_task_submission_time = datetime.now()
                    submitted.append(item)
            for item in submitted:
                self.request_queue.remove(item)
            print(f"-----------------")
            self.reschedule_event.clear()

    def __repr__(self):
        return f"ClusterTaskScheduler({len(self.running_tasks)}/{len(self.request_queue)})"

    def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None):
        state = self.resource_states[key]
        for i in range(amt):
            freed = state.deallocate_pool(manual_destructor)
            if freed is not None:
                self.free_resources(freed.consuming_resources)


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

    def status(self) -> ObjectRef:
        return self.actor.status.remote()

    def get_resources(self, **request: int) -> ObjectRef:
        """
        return the resource or throw error immidiately if no sufficient resources are available.
        :param request:
        :return:
        """
        return self.actor.get_resource.remote(request)

    def free_resources(self, resources: Resources) -> ObjectRef:
        return self.actor.free_resources.remote(resources)

    def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None) -> ObjectRef:
        return self.actor.free_pool.remote(key, amt, manual_destructor)

    def acquire(self, request: Dict[str, int]) -> ObjectRef:
        return self.actor.schedule_resource(request)

    def schedule_resources(self, request: Dict[str, int], timeout: Optional[Union[timedelta, str]]) -> ObjectRef:
        timeout = pd.Timedelta(timeout) if not isinstance(timeout, timedelta) else timeout
        return self.actor.schedule_resource.remote(request, timeout)
