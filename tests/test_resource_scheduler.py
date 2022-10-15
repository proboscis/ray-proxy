import time
import uuid
from datetime import datetime

import pandas as pd
import ray

from pinject_design import Injected
from ray_proxy.resource_design import ResourceDesign
from ray_proxy.injected_resource import InjectedResource
from ray_proxy.resource_scheduler import ResourceSchedulerClient

design = ResourceDesign().bind_provider(
    x=InjectedResource(Injected.pure("x"), "reserved", 100),
    y=InjectedResource(Injected.pure("y"), "reserved", 100),
    z=InjectedResource(Injected.by_name("x").zip(Injected.by_name("y")), "reserved", 100)
)


class Dummy:
    def __getitem__(self, item):
        return ResourceSchedulerClient.align_query(item)


def test__align_query():
    d = Dummy()
    print(d["x":2, "y", Injected.by_name("x"):3])


def provide_errorneous_resource():
    import numpy as np
    if np.random.rand() > 0.5:
        raise RuntimeError("erroneous resource")
    return "successfull resource"


def test_errorneous_resource():
    res1 = InjectedResource(Injected.bind(provide_errorneous_resource), "reserved", 10)
    design = ResourceDesign().bind_provider(
        res1=res1
    )
    sch = design.to_scheduler()
    print(sch.status())
    try:
        with sch.managed_resources(res1=10) as res:
            print(res)
    except Exception as e:
        print(e)
    print(sch.status())




def test_get_resource():
    bsch = design.to_scheduler()

    # although this code makes very heavy use of schduler...
    with bsch["x":2, "y"] as ((x1, x2), y):
        print(x1, x2, y)
        assert (x1,x2,y) == ("x","x","y")
    with bsch["x":2] as (x1,x2):
        print(x1,x2)
        assert (x1,x2) == ("x","x")
    with bsch["x"] as x:
        print(x)
        assert x == "x"

def test_in_use_count():
    sch = design.to_scheduler()
    @ray.remote
    def task():
        with sch["z"] as z:
            print(sch.status())
            print(z)
        print(sch.status())
    ray.get([task.remote() for i in range(10)])
    print(sch.status())

def test_scheduling_speed():
    sch = design.override_issuable(z=2).to_scheduler()
    start = datetime.now()
    @ray.remote
    def task():
        from loguru import logger
        tid = str(uuid.uuid4())[:5]
        logger.info(f"task {tid} waiting for resources...")
        with sch["z"] as z:
            logger.info(f"task {tid} started")
            time.sleep(3)
            logger.info(f"task {tid} freed {z}")
        logger.info(f"task {tid} finished")
    ray.get([task.remote() for i in range(10)])
    end = datetime.now()
    dt = end - start
    print(dt)
    assert dt <= pd.Timedelta("20 seconds"),dt
    print(sch.status())
