import asyncio
import time

import ray

from ray_proxy.cluster_resource import ClusterTaskScheduler, LambdaResourceFactory, ReservedScope, ClusterTask, \
    LambdaClusterTask, OnDemandScope


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
    scheduler = ray.remote(ClusterTaskScheduler).remote()

    def register(name, factory, scope=ReservedScope()):
        ray.get(scheduler.register_resource_factory.remote(
            name, factory, scope
        ))

    register("a", a_fac)
    register("gpu", gpu_fac)
    register("env", env_fac)
    register("harmonizer",harmonizer_fac,OnDemandScope())
    ray.get(scheduler.start.remote())

    lct = LambdaClusterTask(dict(a=1, gpu=1), task)
    env_task = LambdaClusterTask(dict(env=1), task)
    harm_task = LambdaClusterTask(dict(harmonizer=1),task)
    results = []
    for i in range(10):
        results.append(scheduler.schedule_task.remote(lct))
        results.append(scheduler.schedule_task.remote(env_task))
        results.append(scheduler.schedule_task.remote(harm_task))
    print(ray.get(results))
    print(ray.get(scheduler.status.remote()))

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
