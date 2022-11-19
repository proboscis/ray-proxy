import asyncio
from asyncio import Task, Event, Future
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Coroutine, Optional, Union, Callable, TypeVar, Any, Awaitable

import pandas as pd
import ray
from cytoolz import valmap, groupby
from rx.subject import Subject

from ray_proxy.cluster_resource import ResourceState, ResourceRequest, Resources, IResourceHandle


async def assign_resources(request: ResourceRequest, resource_task: Task):
    res = await resource_task
    request.result.set_result(res)


T = TypeVar("T")


@dataclass
class AsyncPubsub:
    subscribers: List[Callable[[Any], Awaitable]] = field(default_factory=list)

    def add_subscriber(self, f: Callable[[Any], Awaitable]):
        async def impl(item):
            try:
                await f(item)
            except Exception as e:
                from loguru import logger
                logger.error(f"error {e} caused by {item}")

        self.subscribers.append(impl)

    async def publish(self, item):
        await asyncio.gather(*[sub(item) for sub in self.subscribers])


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
    request_prioritizer: Callable[[Dict[str, int]], int] = field(default=lambda req: 0)
    events_pubsub: AsyncPubsub = field(default_factory=AsyncPubsub)

    # I think I need to plot the statistics to influxdb

    def status(self):
        num_in_pools = self.pool_counts()
        num_issuables = self.issuables()
        use_counts = valmap(lambda v: v.use_count, self.resource_states)
        return pd.DataFrame([num_in_pools, use_counts, num_issuables, self.demands()],
                            index=["in_pool", "in_use", "issuable", "demands"]).T

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

    def demands(self):
        counts = defaultdict(int)
        for req in self.request_queue:
            for k, amt in req.request.items():
                counts[k] += amt
        return counts

    def find_missing_resources(self, request: Dict[str, int]):
        resource_counts = self.pool_counts()
        num_availables = self.issuables()

        # print(f"{resource_counts},{num_availables}")

        def gather_req(req: dict):
            for k, amt in req.items():
                if num_availables[k]:
                    num_availables[k] -= 1
                    gather(k, amt)
                else:
                    # print(f"insufficient availability. by {k},{resource_counts},{num_availables}")
                    resource_counts[k] -= 1

        def gather(key, amount):
            in_pool = resource_counts[key]
            missing = max(0, amount - in_pool)
            # print(f"found {missing} {key} missing from pool")
            fac = self.resource_states[key].source
            for i in range(missing):
                reqs = fac.required_resources
                gather_req(reqs)
            resource_counts[key] -= min(in_pool, amount)

        gather_req(request)
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
                    await self.free_single_resource(k, r)
                for k, e, trc in failures:
                    print(f"-------resource allocation of {k} failed due to :{e}------------------")
                    print(trc)
                    print(f"----------------------------------------------------------------")
                self.reschedule_event.set()
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

    async def schedule_resource(self,
                                request: Dict[str, int],
                                timeout: Optional[Union[str, timedelta, float]] = None,
                                priority=None) -> Resources:
        """
        schedule for resources in this scheduler.
        a user must free the resources after use!
        :param timeout:
        :param request:
        :return:
        """
        if priority is None:
            priority = self.request_prioritizer(request)
        assert priority <= 100, "priority must be below 100 where 100 has the highest priority"
        for k in request.keys():
            assert k in self.resource_states, f"resource {k} is not managed in the scheduler"
        # todo use priority queue for performance
        request = ResourceRequest(request, Future(), priority)
        self.request_queue.append(request)  # .insert(queue_index, request)
        self.request_queue.sort(key=lambda rr: rr.priority, reverse=True)
        self.reschedule_event.set()
        return await asyncio.wait_for(request.result, self._parse_timeout(timeout))

    async def set_prioritizer(self, prioritizer: Callable[[Dict[str, int]], int]):
        self.request_prioritizer = prioritizer

    async def _free_resources(self, used_resources: Resources):
        for key, resources in used_resources.items():
            for r in resources:
                await self.free_single_resource(key, r)

    async def free_resources(self, used_resources: Resources):
        start = datetime.now()
        # why would this take more than 10 seconds?
        await self._free_resources(used_resources)
        end = datetime.now()
        et = end - start
        print(f"freeing resources took {et.total_seconds()} seconds")
        self.reschedule_event.set()

    async def free_single_resource(self, key, handle: IResourceHandle):
        start = datetime.now()
        self.run_background(self.events_pubsub.publish(
            dict(type="free_single_resource_start", key=key, start=start,time=datetime.now())
        ))
        if key in self.resource_states:
            unstocked = self.resource_states[key].stock(handle)
            if unstocked is not None:
                await self._free_resources(handle.consuming_resources)
                handle.free()

        else:
            await self.free_resources(handle.consuming_resources)
        end = datetime.now()
        et = end - start
        print(f"freeing resource {key} took {et.total_seconds()} seconds.")
        self.run_background(self.events_pubsub.publish(dict(
            type="free_single_resource_end",
            time=datetime.now(),
            key=key,
            start=start,
            end=end,
            et=et
        )))
        # TODO some resource is taking more than 10 seconds to release. wtf??

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
        print(f"cluster resource scheduler started")
        while True:
            # print(f"waiting for an event to check runnable tasks")
            await self.reschedule_event.wait()
            self.run_background(self.events_pubsub.publish(dict(
                type="scheduling_start",
                time=datetime.now(),
                status=self.status()
            )))
            print(f"--- queue check. {len(self.request_queue)} == queued_requests ---")
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
                    self.run_background(self.events_pubsub.publish(dict(
                        type="resource_allocation",
                        time=datetime.now(),
                        request=item.request
                    )))
                    self.last_task_submission_time = datetime.now()
                    submitted.append(item)
            for item in submitted:
                self.request_queue.remove(item)
            print(f"-----------------")

            self.reschedule_event.clear()

    def __repr__(self):
        return f"ClusterResourceScheduler({len(self.request_queue)})"

    async def free_pool(self, key, amt, manual_destructor: Optional[Callable[[IResourceHandle], None]] = None):
        state = self.resource_states[key]
        for i in range(amt):
            freed = state.deallocate_pool(manual_destructor)
            if freed is not None:
                await self.free_resources(freed.consuming_resources)

    async def set_max_issuables(self, **amounts: int):
        for k, amt in amounts.items():
            assert amt >= 0, "amount must be positive. or 0 for forbidding the use"
            state = self.resource_states[k]
            state.set_max_issuable(amt)
            while state.issuable < 0 and state.pool:
                freed = state.deallocate_pool()
                if freed is not None:
                    await self.free_resources(freed.consuming_resources)
        return self.status()

    async def discard_single_resource(self, key, handle: IResourceHandle):
        """force handle to be discarded without pooling"""
        if key in self.resource_states:
            print(f"discarding {key}")
            self.resource_states[key].discard(handle)
            await self.free_resources(handle.consuming_resources)
        else:
            await self.free_resources(handle.consuming_resources)

    async def discard_on_condition(self, key, to_discard: Callable[[T], bool]):
        """
        wait for a resource and discard it if the condition is met.
        :param to_discard:
        :return:
        """
        res = await self.schedule_resource({key: 1}, priority=100)
        if to_discard(res[key][0].value):
            await self.discard_single_resource(key, res[key][0])
        else:
            await self.free_resources(res)

    async def subscribe_events(self, callable: Callable[[Any], Awaitable[Any]]):
        self.events_pubsub.add_subscriber(callable)
