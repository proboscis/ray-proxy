from dataclasses import dataclass, field, replace
from typing import Dict, Union, TypeVar

from bidict import bidict

from pinject_design import Injected
from pinject_design.di.app_injected import EvaledInjected
from pinject_design.di.injected import MappedInjected, InjectedFunction, ZippedInjected, MZippedInjected, InjectedPure, \
    InjectedByName
from ray_proxy.cluster_resource import ResourceScope
from ray_proxy.injected_resource import InjectedResource

T = TypeVar("T")


@dataclass
class ResourceDesign:
    resources: Dict[str, InjectedResource] = field(default_factory=dict)

    def __post_init__(self):
        self.key_to_id_bidict = bidict({k: id(v.factory) for k, v in self.resources.items()})
        self.id_to_key = self.key_to_id_bidict.inv

    def truncate_injected_tree(self, injected: Injected) -> Injected:
        def _replace(target: Injected, is_head=False):
            _id = id(target)
            if not is_head and _id in self.id_to_key:
                return Injected.by_name(self.id_to_key[_id])
            match target:
                case MappedInjected(src, f):
                    return MappedInjected(_replace(src), f)
                case InjectedFunction(func, kwarg_mapping):
                    replaced = dict()
                    for k, v in kwarg_mapping.items():
                        match v:
                            case Injected():
                                replaced[k] = _replace(v)
                            case _:
                                replaced[k] = v

                    return InjectedFunction(func, replaced)
                case ZippedInjected(a, b):
                    return ZippedInjected(_replace(a), _replace(b))
                case MZippedInjected(srcs):
                    return MZippedInjected(*[_replace(src) for src in srcs])
                case EvaledInjected(value, ast):
                    return EvaledInjected(_replace(value), ast)
                case InjectedPure(value):
                    return target
                case InjectedByName(name):
                    return target
                case _:
                    raise RuntimeError(
                        f"unsupported inejected factory for a resource design to be truncated.:{target}")

        return _replace(injected, is_head=True)
        # we need to dig except the head. for that we need to check every possibilities of a head

    def _truncated_injected_resource(self, res: InjectedResource):
        return replace(res, factory=self.truncate_injected_tree(res.factory))

    def truncated(self) -> "ResourceDesign":
        return ResourceDesign(
            {k: self._truncated_injected_resource(v) for k, v in self.resources.items()}
        )

    def bind_provider(self, **resources: InjectedResource):
        return self + ResourceDesign(resources)
        pass

    def __add__(self, other: "ResourceDesign") -> "ResourceDesign":
        return ResourceDesign(
            {**self.resources, **other.resources}
        )

    def to_scheduler(self):
        from ray_proxy.resource_scheduler import ResourceSchedulerClient
        sch = ResourceSchedulerClient.create(self)
        return sch

    def override_issuable(self, **kwargs: int) -> "ResourceDesign":
        overrides = dict()
        for k, amt in kwargs.items():
            overrides[k] = replace(self.resources[k], num_issuable=amt)
        return self + ResourceDesign(overrides)

    def override_scope(self, **kwargs: Union[ResourceScope, str]):
        overrides = dict()
        for k, scope in kwargs.items():
            overrides[k] = replace(self.resources[k], scope=scope)
        return self + ResourceDesign(overrides)
