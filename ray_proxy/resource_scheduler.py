import asyncio
from abc import ABC, abstractmethod
from asyncio import Task, Event, Future
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Coroutine, Optional, Union, Callable, Awaitable, TypeVar, Generic, Iterable

import pandas as pd
import ray
from cytoolz import valmap, groupby
from ray import ObjectRef
from ray.actor import ActorHandle

from pinject_design import Injected
from ray_proxy.cluster_resource import ResourceState, ResourceRequest, Resources, IResourceHandle, IResourceFactory, \
    ResourceScope, ClusterTaskScheduler, OnDemandScope, ReservedScope, LambdaResourceFactory
from ray_proxy.resource_design import ResourceDesign
from ray_proxy.injected_resource import InjectedResource
from ray_proxy.util import run_injected


async def assign_resources(request: ResourceRequest, resource_task: Task):
    res = await resource_task
    request.result.set_result(res)


T = TypeVar("T")


@dataclass
class ClusterResourceScheduler:
    resource_states: Dict[str, ResourceState] = field(default_factory=dict)
    request_queue: List[ResourceRequest] = field(default_factory=list)
    background_internal_tasks: Set[Task] = field(default_factory=set)
    last_task_submission_time: datetime = field(default=None)
    reschedule_event: Event = field(default_factory=Event)
    scheduling_task: Task = field(default=None)
    resource_creation_executor: ThreadPoolExecutor = field(default_factory=ThreadPoolExecutor)
    periodic_check_task: Task = field(default=None)

    def status(self):
        num_in_pools = self.pool_counts()
        num_issuables = self.issuables()
        use_counts = valmap(lambda v: v.use_count, self.resource_states)
        return pd.DataFrame([num_in_pools, use_counts, num_issuables],
                            index=["in_pool", "in_use", "issuable"]).T

    def all_status(self):
        return dict(
            states=self.resource_states,
            request_queue=[repr(t) for t in self.request_queue],
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

        def gather(key, amount):
            # print(f"trying to gather {amt} {key}")
            in_pool = resource_counts[key]
            missing = max(0, amount - in_pool)
            # print(f"found {missing} {key} missing from pool")
            fac = self.resource_states[key].source
            while missing > 0:
                reqs = fac.required_resources
                if num_availables[key]:
                    num_availables[key] -= 1
                    for key, amount in reqs.items():
                        gather(key, amount)
                else:
                    # print(f"insufficient availability. by {key}")
                    resource_counts[key] -= 1
                missing -= 1
            resource_counts[key] -= min(in_pool, amount)

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
        :param timeout:
        :param request:
        :return:
        """
        request = ResourceRequest(request, Future())
        self.request_queue.append(request)
        self.reschedule_event.set()
        return await asyncio.wait_for(request.result, self._parse_timeout(timeout))

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
                self.free_resources(handle.consuming_resources)
                handle.free()

        else:
            self.free_resources(handle.consuming_resources)

    def _check_remaining_jobs(self):
        for item in self.request_queue:
            missings = self.find_missing_resources(item.request)
            if missings:
                print(f"job not run due to missing resource:{missings}")

    def run_background(self, task: Coroutine, group: set = None) -> Task:
        if group is None:
            group = self.background_internal_tasks
        _task = asyncio.create_task(task)
        group.add(_task)
        _task.add_done_callback(group.discard)
        return _task

    async def _schedule_loop(self):
        print(f"cluster task scheduler started")
        while True:
            # print(f"waiting for an event to check runnable tasks")
            await self.reschedule_event.wait()
            print(
                f"--- queue check ({len(self.request_queue)}) == queued_requests ---")
            print(self.status())
            submitted = []
            if not self.request_queue:
                print(f"---- no job queued for execution ----")
            for item in self.request_queue:
                if not self.find_missing_resources(item.request):
                    print(f"_______________resource allocation_________________")
                    print(self.status())
                    resources_task = self._get_resource(item.request)
                    print(self.status())
                    print(f"_______________resource allocation done________________")
                    self.run_background(assign_resources(item, resources_task))
                    self.last_task_submission_time = datetime.now()
                    submitted.append(item)
            for item in submitted:
                self.request_queue.remove(item)
            print(f"-----------------")
            self.reschedule_event.clear()

    def __repr__(self):
        return f"ClusterTaskScheduler({len(self.request_queue)})"

    def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None):
        state = self.resource_states[key]
        for i in range(amt):
            freed = state.deallocate_pool(manual_destructor)
            if freed is not None:
                self.free_resources(freed.consuming_resources)


ResourceQueryPrimitive = Union[slice, str, Injected]
ResourceQuery = Union[ResourceQueryPrimitive, Tuple[ResourceQueryPrimitive], Dict[str, int]]


class IReleasable(Generic[T], ABC):
    @abstractmethod
    def release(self):
        pass

    @property
    @abstractmethod
    def value(self) -> T:
        pass


@dataclass
class ResourceSchedulerClient:
    actor: ActorHandle
    design: ResourceDesign

    @staticmethod
    def create(design: ResourceDesign) -> "ResourceSchedulerClient":
        actor = ray.remote(ClusterResourceScheduler).remote()
        ray.get(actor.start.remote())
        client = ResourceSchedulerClient(actor, design)
        for k, v in design.truncated().resources.items():
            client.add_resource(k, v)
        client.add_resource("arch", InjectedResource(Injected.pure(client), "reserved", 100000))
        return client

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

    def status(self) -> ObjectRef:
        return ray.get(self.actor.status.remote())

    def get_resources(self, **request: int) -> ObjectRef:
        """
        return the resource or throw error immidiately if no sufficient resources are available.
        :param request:
        :return:
        """
        return self.actor.get_resource.remote(request)

    def free_resources(self, resources: Resources) -> ObjectRef:
        """
        :param resources:
        :return:
        """
        return self.actor.free_resources.remote(resources)

    def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None) -> ObjectRef:
        return self.actor.free_pool.remote(key, amt, manual_destructor)

    def schedule_resources(self, request: Dict[str, int], timeout: Optional[Union[timedelta, str]]) -> ObjectRef:
        timeout = pd.Timedelta(timeout) if not isinstance(timeout, timedelta) else timeout
        return self.actor.schedule_resource.remote(request, timeout)

    def schedule_resources_v2(self, query: ResourceQuery, timeout: Optional[Union[timedelta, str]]) -> List[
        List[IReleasable]]:
        """
        :param query:
        :return:
        """
        aligned = self.align_query(query)
        request = self.aligned_to_request(aligned)
        resources = ray.get(self.actor.schedule_resource.remote(request, timeout))
        return self.distribute_resources_for_injected(aligned, resources)

    @staticmethod
    def align_query(query: ResourceQuery) -> List[List[Injected]]:
        match query:
            case dict():
                return [[Injected.by_name(k) for i in range(v)] for k, v in query.items()]
            case [*_]:
                res = []
                for item in query:
                    res += ResourceSchedulerClient.align_query(item)
                return res
            case Injected():
                return [[query]]
            case str():
                return [[Injected.by_name(query)]]
            case slice() if isinstance(query.start, str):
                return [[Injected.by_name(query.start) for i in range(query.stop)]]
            case slice() if isinstance(query.start, Injected):
                return [[query.start for i in range(query.stop)]]
            case _:
                raise ValueError(f"unknown query type:{query}")

    @staticmethod
    def aligned_to_request(query: List[List[Injected]]) -> Dict[str, int]:
        counts = defaultdict(int)
        for items in query:
            item = items[0]
            nitem = len(items)
            for dep in item.dependencies():
                counts[dep] += nitem
        return counts

    def distribute_resources_for_injected(self, aligned: List[List[Injected]], resources: Resources) -> \
            List[List[IReleasable]]:
        res = []
        resources = ResourceSchedulerClient.copy_resources(resources)

        for items in aligned:
            tmp = []
            for item in items:
                tmp_res = defaultdict(list)
                for dep in item.dependencies():
                    tmp_res[dep].append(resources[dep].pop())
                handle = self.injected_to_releasable(item, tmp_res)
                tmp.append(handle)
            if len(tmp) == 1:
                tmp = tmp[0]
            res.append(tmp)
        return res

    def injected_to_releasable(self, injected: Injected, resources: Resources) -> IReleasable:
        value = run_injected(injected, resources)
        return ReleasableImpl(
            injected,
            resources,
            self,
            value,
        )

    @staticmethod
    def copy_resources(resources: Resources):
        res = defaultdict(list)
        for k, vs in resources.items():
            for v in vs:
                res[k].append(v)

        return res

    @contextmanager
    def __getitem__(self, query: ResourceQuery):
        res = self.schedule_resources_v2(query, None)
        try:
            to_yield = []
            for items in res:
                match items:
                    case [*_]:
                        to_yield.append([v.value for v in items])
                    case IReleasable() as r:
                        to_yield.append(r.value)
            if len(to_yield) == 1:
                to_yield = to_yield[0]
            yield to_yield
        finally:
            tasks = []
            for releasables in res:
                match releasables:
                    case [*_]:
                        tasks += [r.release() for r in releasables]
                    case IReleasable() as r:
                        tasks.append(r.release())
            print(f"release results:{ray.get(tasks)}")

    def _injected_resources(self, injected):
        return {k: 1 for k in injected.dependencies()}

    def add_resource(self, name: str, resource: InjectedResource):
        self.add_factory_lambda(
            name,
            self._injected_resources(resource.factory),
            resource.to_awaitable,
            n_issuable=resource.num_issuable,
            scope=resource.scope,
            destructor=resource.destructor
        )


@dataclass
class ReleasableImpl(IReleasable[T]):
    src: Injected[T]
    holding_resources: Resources
    client: ResourceSchedulerClient
    created_value: T

    # destructor: Callable[[T], None]

    @property
    def value(self):
        return self.created_value

    def release(self) -> ObjectRef:
        # self.destructor(self.created_value)
        return self.client.free_resources(self.holding_resources)
