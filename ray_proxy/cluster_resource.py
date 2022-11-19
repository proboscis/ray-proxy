import uuid
from abc import ABC, abstractmethod
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import List, Generic, TypeVar, Dict, Callable, Awaitable, Any, Coroutine, Optional

import ray

from pinject_design import Injected

T = TypeVar("T")


class IResourceHandle(Generic[T], ABC):
    @abstractmethod
    def free(self):
        # this should be able to be called from anywhere within the cluster.
        # and calling this should immidiately release the resource on scheduler
        # and dependencies should also be released.
        # this means that this handle holds a reference to the scheduler actor
        # the question is whether it is ok to call the actor itself...
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
    _value: T
    _consuming_resources: Resources
    _id: uuid.UUID = field(default_factory=uuid.uuid4)
    _free: Optional[Callable[[T], None]] = field(default=None)

    @property
    def value(self) -> T:
        return self._value

    @property
    def id(self) -> uuid.UUID:
        return self._id

    def free(self):
        if self._free is not None:
            self._free(self._value)

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
    def max_issuable(self) -> int:
        pass

    def is_resource_healthy(self, handle: IResourceHandle) -> bool:
        """
        :param handle:
        :return: False if resource is not healthy. this will make the resource not pooled for reuse.
        """
        return True


@dataclass
class LambdaResourceFactory(IResourceFactory[T]):
    _required_resources: Dict[str, int]
    _factory: Callable[[Resources, ThreadPoolExecutor], Awaitable[T]]
    _max_issuable: int
    destructor: Optional[Callable[[T], None]] = field(default=None)
    health_checker: Optional[Callable[[T], bool]] = field(default=None)

    async def create(self, resources: Resources, executor: ThreadPoolExecutor) -> IResourceHandle[T]:
        value = await self._factory(resources, executor)
        return LambdaResourceHandle(
            value,
            resources,
            _free=self._destructor,
        )

    def max_issuable(self) -> int:
        return self._max_issuable

    @property
    def required_resources(self):
        return self._required_resources

    def _destructor(self, value: T):
        if self.destructor is not None:
            self.destructor(value)

    def is_resource_healthy(self, handle: IResourceHandle) -> bool:
        if self.health_checker is not None:
            return self.health_checker(handle.value)
        return True


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
class ResourceRequest:
    request: Dict[str, int]
    result: Future[Resources]
    priority: float


async def pure(x):
    return x


@dataclass
class ResourceState:
    source: IResourceFactory
    scope: ResourceScope
    pool: List[IResourceHandle] = field(default_factory=list)
    use_count: int = field(default_factory=int)
    issuable: int = field(default_factory=int)

    def __post_init__(self):
        self.issuable = self.source.max_issuable()

    def set_max_issuable(self, n):
        # this means we can decrease but not increase.
        current_issuable = self.issuable + self.use_count + len(self.pool)
        # to_release = max(current_issuable - n, 0)
        to_release = current_issuable - n
        self.issuable -= to_release

    def num_issuable(self):
        return self.issuable

    def num_pooled(self):
        return len(self.pool)

    def obtain(self, request: Callable[[Dict[str, int]], Coroutine], executor) -> Coroutine:
        self.use_count += 1
        # why is this only incremented twice?
        # ah it is quite true because the gpus are only obtained twice.
        # I mean the gpu is not returned to the pool. while the txt2img_env is using it.
        # in that case, why does the count get decremented?
        if self.pool:
            return pure(self.pool.pop())
        else:
            # await is inevitable, but we dont want to block.
            self.issuable -= 1
            task = request(self.source.required_resources)
            return self._obtain(task, executor)

    def stock(self, item: IResourceHandle):
        if self.source.is_resource_healthy(item):
            if self.issuable >= 0 and self.scope.to_keep(item):
                self.use_count -= 1
                self.pool.insert(0, item)  # trying to make resource usage round robin
                return
        return self.discard(item)

    def discard(self, item: IResourceHandle):
        #print(f"discarded item: {item}")
        self.use_count -= 1
        self.issuable += 1
        item.free()
        return item

    def deallocate_pool(self, destructor=None):
        if self.pool:
            res = self.pool.pop()
            self.issuable += 1
            if destructor is not None:
                destructor(res)
            res.free()
            return res

    async def _obtain(self, resource_task, executor):
        try:
            res = await resource_task
            return await self.source.create(res, executor)
        except Exception as e:
            self.use_count -= 1
            self.issuable += 1
            print(e)
            raise e
