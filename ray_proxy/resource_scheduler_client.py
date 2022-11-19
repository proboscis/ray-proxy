import inspect
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Callable, Awaitable, Union, Optional, List, TypeVar, Any

import pandas as pd
import ray
from ray import ObjectRef
from ray.actor import ActorHandle

from pinject_design import Injected
from ray_proxy.cluster_resource import IResourceFactory, ResourceScope, Resources, OnDemandScope, ReservedScope, \
    LambdaResourceFactory, IResourceHandle
from ray_proxy.injected_resource import InjectedResource
from ray_proxy.releasable import IReleasable, ReleasableImpl
from ray_proxy.resource_design import ResourceDesign
from ray_proxy.type_aliases import ResourceQuery
from ray_proxy.util import run_injected

T = TypeVar("T")


@dataclass
class ResourceSchedulerClient:
    actor: ActorHandle
    actor_name: str
    design: ResourceDesign

    @staticmethod
    def create(design: ResourceDesign, actor_options: Dict = None) -> "ResourceSchedulerClient":
        assert isinstance(design, ResourceDesign)
        from ray_proxy.resource_scheduler import ClusterResourceScheduler
        actor_options = actor_options or dict()
        if "name" not in actor_options:
            actor_options["name"] = str(uuid.uuid4())[:6]

        actor = ray.remote(ClusterResourceScheduler).options(
            **actor_options
        ).remote(
            request_prioritizer=design.prioritizer
        )
        ray.get(actor.start.remote())
        client = ResourceSchedulerClient(actor, actor_options["name"], design)
        for k, v in design.truncated().resources.items():
            client.add_resource(k, v)
        client.add_resource("arch", InjectedResource(Injected.pure(client), "reserved", 100000))
        return client

    @staticmethod
    def from_name(name: str):
        actor = ray.get_actor(name)
        return ResourceSchedulerClient(actor, name, ResourceDesign())

    def add_factory(self, name, factory: IResourceFactory, scope: ResourceScope):
        return ray.get(self.actor.register_resource_factory.remote(name, factory, scope))

    def add_factory_lambda(self, name, reqs: Dict[str, int],
                           factory: Callable[[Resources, ThreadPoolExecutor], Awaitable],
                           n_issuable: int,
                           scope: Union[ResourceScope, str],
                           destructor: Optional[Callable[[T], None]] = None,
                           health_checker: Optional[Callable[[T], bool]] = None
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
        self.add_factory(name, LambdaResourceFactory(reqs, factory, n_issuable, destructor, health_checker), scope)

    def status(self) -> pd.DataFrame:
        return ray.get(self.actor.status.remote())

    def all_status(self) -> ObjectRef:
        return ray.get(self.actor.all_status.remote())

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
            ray.get(tasks)
            # print(f"release results:{ray.get(tasks)}")

    def _injected_resources(self, injected):
        # TODO parse dependencies to use multiple resources if needed, by like xyz_2 where 2 is the amount of resources
        return {k: 1 for k in injected.dependencies()}

    def add_resource(self, name: str, resource: InjectedResource):
        self.add_factory_lambda(
            name,
            self._injected_resources(resource.factory),
            resource.to_awaitable,
            n_issuable=resource.num_issuable,
            scope=resource.scope,
            destructor=resource.destructor,
            health_checker=resource.health_checker
        )

    def set_max_issuables(self, **amounts: int):
        return ray.get(self.actor.set_max_issuables.remote(**amounts))

    def discard_on_condition(self, key, to_discard: Callable[[T], bool]):
        return ray.get(self.actor.discard_on_condition.remote(key, to_discard))

    def add_events_subscriber(self, callable: Callable[[Any], Awaitable]):
        assert inspect.iscoroutinefunction(callable),"callback must be an async function."
        return ray.get(self.actor.subscribe_events.remote(callable))

    def __hash__(self):
        return 0
