from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

import pytz
from ray import ObjectRef

from pinject_design import Injected
from ray_proxy.cluster_resource import Resources

T = TypeVar("T")


class IReleasable(Generic[T], ABC):
    @abstractmethod
    def release(self):
        pass

    @property
    @abstractmethod
    def value(self) -> T:
        pass


@dataclass
class ReleasableImpl(IReleasable[T]):
    src: Injected[T]
    holding_resources: Resources
    client: "ResourceSchedulerClient"
    created_value: T

    def __post_init__(self):
        self.created_time = datetime.now(pytz.utc)

    # destructor: Callable[[T], None]

    @property
    def value(self):
        return self.created_value

    def release(self) -> ObjectRef:
        # self.destructor(self.created_value)
        now = datetime.now(pytz.utc)
        dt = now - self.created_time
        print(f"releasing resource after {dt.total_seconds()} seconds:{self.src}")
        return self.client.free_resources(self.holding_resources)
