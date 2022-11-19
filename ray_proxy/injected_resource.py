from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from typing import Generic, Union, Optional, Callable, Awaitable, TypeVar

from pinject_design import Injected
from pinject_design.di.applicative import Applicative
from pinject_design.di.proxiable import DelegatedVar
from pinject_design.di.static_proxy import AstProxyContextImpl, eval_app
from ray_proxy.cluster_resource import ResourceScope, Resources
from ray_proxy.util import run_injected

T = TypeVar("T")
@dataclass
class InjectedResource(Generic[T]):
    factory: Injected
    scope: Union[str, ResourceScope]  # add n_issuable functionality to scope
    num_issuable: int
    destructor: Optional[Callable[[T], None]] = field(default=None)
    health_checker:Optional[Callable[[T], bool]] = field(default=None)

    @property
    def proxy(self):
        return DelegatedVar(self, INJECTED_RESOURCE_EVAL_CONTEXT)

    def override(self,
                 scope: Union[str, ResourceScope] = None,
                 num_issuable: int = None):
        scope = scope or self.scope
        num_issuable = self.num_issuable if num_issuable is None else num_issuable
        return replace(self,scope=scope,num_issuable=num_issuable)

    def map(self, f):
        return INJECTED_RESOURCE_APPLICATIVE.map(self, f)

    def __repr__(self):
        return f"InjectedResource(deps={self.factory.dependencies()},num_issuable={self.num_issuable},scope={self.scope},destructor={self.destructor})"

    def to_awaitable(self, resources: Resources, executor: ThreadPoolExecutor) -> Awaitable:
        task = executor.submit(run_injected, self.factory, resources)
        async_future = Future()

        def on_done(f: Future):
            try:
                async_future.set_result(f.result())
            except Exception as e:
                async_future.set_exception(e)

        task.add_done_callback(on_done)
        return async_future


class InjectedResourceApplicative(Applicative[InjectedResource]):

    def map(self, target: InjectedResource, f) -> InjectedResource:
        return replace(target,factory=target.factory.map(f),health_checker=None)

    def zip(self, *targets: InjectedResource):
        return InjectedResource(
            Injected.mzip(*(t.factory for t in targets)),
            targets[0].scope,
            targets[0].num_issuable,
            health_checker=None
        )

    def pure(self, item) -> InjectedResource:
        return InjectedResource(Injected.pure(item), "reserved", num_issuable=1)

    def is_instance(self, item) -> bool:
        return isinstance(item, InjectedResource)


INJECTED_RESOURCE_APPLICATIVE = InjectedResourceApplicative()
INJECTED_RESOURCE_EVAL_CONTEXT = AstProxyContextImpl(
    lambda expr: eval_app(expr, INJECTED_RESOURCE_APPLICATIVE),
)
