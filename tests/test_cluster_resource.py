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


def test_schedule_resources():
    assert False


@dataclass
class Resource:
    pass

@dataclass
class ResourceHandle:
    scheduler:ActorHandle
    resource:Resource
    async def free(self):
        return await self.scheduler.free.remote(self.resource)

@dataclass
class Scheduler:
    resources: List[Resource]
    self_ref:ActorHandle=None

    async def free(self, res: Resource):
        self.resources.remove(res)
    async def set_self(self,ref):
        self.self_ref = ref

    async def create_res(self):
        res = Resource()
        self.resources.append(res)
        return ResourceHandle(self.self_ref,res)
    async def self_free(self,handle:ResourceHandle):
        return await handle.free()



def test_recursive_call():
    sch = ray.remote(Scheduler).remote([])
    ray.get(sch.set_self.remote(sch))
    handle = ray.get(sch.create_res.remote())
    ray.remote(sch.self_free.remote(handle))
