import asyncio
import time

import ray

from ray_proxy.cluster_resource import ClusterTaskScheduler, LambdaResourceFactory, ReservedScope, ClusterTask, \
    LambdaClusterTask, OnDemandScope, RemoteTaskScheduler


class A:
    pass


def task(res):
    print(f"task working")
    time.sleep(1)
    return 1


def test_register_resource_factory():
    # I feel like this scheduler is working perfectly.
    actor = ray.remote(A).remote()
    a_fac = LambdaResourceFactory(dict(), lambda res: ray.remote(A).remote(), remaining=3)
    gpu_fac = LambdaResourceFactory(dict(), lambda res: 1, remaining=6)
    env_fac = LambdaResourceFactory(dict(gpu=1), lambda res: "env", remaining=3)
    harmonizer_fac = LambdaResourceFactory(dict(env=1),lambda res: "harmonizer", remaining=100)
    sch = RemoteTaskScheduler.create(dict(
        a = (a_fac,ReservedScope()),
        gpu = (gpu_fac,ReservedScope()),
        env = (env_fac,ReservedScope()),
        harmonizer = (harmonizer_fac,OnDemandScope()),
    ))
    results = []
    for i in range(10):
        results.append(sch.submit(dict(a=1,gpu=1),task))
        results.append(sch.submit(dict(gpu=1),task))
        results.append(sch.submit(dict(harmonizer=1),task))
    print(ray.get(results))
    print(sch.status())

    # I dont know the semantics of on_demand_resources and reserved_resources
    # reserved_resources are created upon creation
    # on_demand_resources are created upon allocation, released after allocation of parent resource.
    # what we do on releasing the resource is that
    # we keep some resources alive
    # and release some resources
    # pass
    # okey, so how can we tell that the resource should be kept alive or not?
    # one way to do that is to use a scope.
    # give schduler a scope and use its scope for deciding whether to keep the resource alive or not.
    # so the work will be like this.
    # 1. create a schduler
    # 2. create a scope for a task
    # 3. register resource factories with a scope
    # 4. submit jobs
    # 5. retrieve results
    # 6. close the scope.


if __name__ == '__main__':
    test_register_resource_factory()
