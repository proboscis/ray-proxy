from typing import Union, Tuple, Dict

from pinject_design import Injected

ResourceQueryPrimitive = Union[slice, str, Injected]
ResourceQuery = Union[ResourceQueryPrimitive, Tuple[ResourceQueryPrimitive], Dict[str, int]]
