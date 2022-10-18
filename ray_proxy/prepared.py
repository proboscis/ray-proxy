from dataclasses import dataclass

import ray
from ray import ObjectRef


@dataclass
class PreparedRef:
    """
    a container that indicates the value is already 'put' thus safe to retrieve
    """
    ref:ObjectRef
    def fetch(self):
        return ray.get(self.ref)
