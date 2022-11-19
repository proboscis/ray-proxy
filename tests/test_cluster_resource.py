import uuid
from dataclasses import dataclass
from typing import List

import ray
from ray.actor import ActorHandle

from ray_proxy.cluster_task_scheduler import RemoteTaskScheduler


def task(res):
    print(res)

def test_set_max_issuables():
    assert False
