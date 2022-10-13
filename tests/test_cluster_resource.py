import uuid

import ray

from ray_proxy.cluster_resource import RemoteTaskScheduler


def task(res):
    print(res)


def test_register_resource_factory():
    # I feel like this scheduler is working perfectly.
    sch = RemoteTaskScheduler.create()
    sch.add_factory_lambda("gpu", dict(), lambda res: f"gpu:{str(uuid.uuid4())[:3]}", 6, "reserved")
    sch.add_factory_lambda("env", dict(gpu=1), lambda res: f"env:{str(uuid.uuid4())[:3]}", 3, "reserved")
    sch.add_factory_lambda("harmonizer", dict(env=1), lambda res: f"harmonizer:{str(uuid.uuid4())[:3]}", 3, "ondemand")

    results = []
    for i in range(10):
        results.append(sch.submit(dict(gpu=1), task))
        results.append(sch.submit(dict(gpu=1), task))
        results.append(sch.submit(dict(harmonizer=1), task))
    print(ray.get(results))
    print(sch.status())

