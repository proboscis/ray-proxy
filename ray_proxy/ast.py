from dataclasses import dataclass, field
from typing import Tuple, Dict, Any

from frozendict import frozendict


class Expr:
    def __getattr__(self, item: str):
        return Attr(self, item)

    def __call__(self, *args, **kwargs):
        return Call(self, args, kwargs)


# def __call__(self, *args, **kwargs) -> "Call":
#    return Call(self, args, kwargs)


@dataclass(frozen=True)
class Call(Expr):
    func: Expr
    args: Tuple[Expr] = field(default_factory=tuple)
    kwargs: Dict[str, Expr] = field(default_factory=dict)

    def __hash__(self):
        return hash(hash(self.func) + hash(self.args) + hash(frozendict(self.kwargs)))


@dataclass(frozen=True)
class Attr(Expr):
    data: Expr
    attr_name: str  # static access so no ast involved


@dataclass(frozen=True)
class Object(Expr):
    """
    Use this to construct an AST and then compile it for any use.
    """
    data: Any  # holds user data

    def __hash__(self):
        return hash(id(self.data))

    def __repr__(self):
        return f"Object({str(self.data)[:20]})".replace("\n", "").replace(" ", "")


@dataclass(frozen=True)
class TupleExpr(Expr):
    items: Tuple[Expr]


@dataclass(frozen=True)
class DictExpr(Expr):
    data: Dict[str, Expr]


@dataclass(frozen=True)
class Symbol(Expr):
    identifier: str


@dataclass(frozen=True)
class Assign(Expr):
    tgt: Symbol
    src: Expr
