import asyncio
import socket
import traceback
import uuid
from asyncio import Lock
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Union, Any

import ray
from ray import ObjectRef
from bidict import bidict

class MyStopIteration(RuntimeError):
    pass

@dataclass
class AsyncPyInterpreter:
    env_id: uuid.UUID = field(default_factory=uuid.uuid4)
    instances: dict = field(default_factory=dict)  # id->instance mapping
    ref_counts: dict = field(
        default_factory=lambda: defaultdict(int))  # todo ref_counts is not reliable at this moment.
    named_instances: bidict = field(default_factory=bidict)  # mapping from name to id and vice versa.
    lock: Lock = field(default_factory=Lock)

    def __post_init__(self):
        self.host = socket.gethostname()
        print(f"PyInterpreter created at {self.host}")
        # why would you even need an async actor?
        # I dont think we would?

    async def reset(self):
        from loguru import logger
        async with self.lock:
            logger.info(f"resetting interpreter. only named variables are kept.")
            prev_instances = self.instances
            tmp = self.named_instances

            self.instances = dict()
            self.ref_counts = defaultdict(int)
            self.named_instances = bidict()
            for name, _id in tmp.items():
                await self._put_named_without_lock(prev_instances[_id], name)
            logger.info(f"reset done:\n{self.status()}")
            logger.info(f"self.named_instances:{self.named_instances}")

    async def get_kind(self):
        return "PyInterpreter"

    async def get_host(self):
        print(f"get_host called. just before returning it.")

        return self.host

    async def get_env_id(self):
        return self.env_id

    async def run(self, func: Callable, args=None, kwargs=None):
        return await self.run_with_proxy(func, args, kwargs)

    async def _unwrap(self, proxy: "RemoteProxy"):
        from ray_proxy.remote_proxy import Var
        print(f"unwrapping:{type(proxy)}")
        if isinstance(proxy, Var):
            # unwrap if the proxy lives in this environment
            assert isinstance(proxy.env.id, ObjectRef), f"proxy.env.id is not a reference:{type(proxy.env.id)}"
            print(f"awaiting env id of Var")
            eid = await proxy.env.id
            if self.env_id == eid:
                print(f"awaiting Var's id")
                oid = await proxy.id
                return self.instances[oid]
            else:
                print(f"fetching from a proxy which lives in different env:{eid}")
                return await proxy.fetch_ref()
        if isinstance(proxy, tuple):
            return tuple(await asyncio.gather(*[self._unwrap(item) for item in proxy]))
        if isinstance(proxy, list):
            return list(await asyncio.gather(*[self._unwrap(item) for item in proxy]))
        if isinstance(proxy, dict):
            keys, values = map(self._unwrap, proxy.keys()), map(self._unwrap, proxy.values())
            keys, values = await asyncio.gather(asyncio.gather(*keys), asyncio.gather(*values))
            return {k: v for k, v in zip(keys, values)}

        return proxy

    async def _unwrap_inputs(self, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = dict()
        args, kwargs = await self._unwrap((args, kwargs))
        return args, kwargs

    async def run_with_proxy(self, func: Callable, args=None, kwargs=None):
        async with self.lock:
            args, kwargs = await self._unwrap_inputs(args, kwargs)
            func = await self._unwrap(func)
            res = func(*args, **kwargs)  # hmm since this function call blocks, there is no way we can await..
            return await self._register_without_lock(res)

    async def register(self, instance):
        async with self.lock:  # we need a lock here.todo take care of reentrant.
            return await self._register_without_lock(instance)

    async def _register_without_lock(self, instance):
        instance = await self._unwrap(instance)
        import uuid
        uuid = uuid.uuid4()
        while uuid in self.instances:
            uuid = uuid.uuid4()
        self.instances[uuid] = instance
        await self.incr_ref_id(uuid)
        return uuid

    async def unregister_id(self, id):
        from loguru import logger
        print(f"unregister_id:{id}")
        await self.decr_ref_id(id)

    async def incr_ref_id(self, id):
        # print(f"incr_ref_id:{id}")
        self.ref_counts[id] += 1
        return "incr_ref_success"

    async def decr_ref_id(self, id):
        # assert self.ref_counts[id] > 0, "unregistering something doesn't exist"
        # print(f"decr_ref_id:{id}")
        if id not in self.ref_counts:
            return "decr_ref_failure: id not found."
        self.ref_counts[id] -= 1
        if self.ref_counts[id] == 0 and id not in self.named_instances.inverse:
            # do not delete named instances.
            from loguru import logger
            # print(f"deleted instance:{self.instances[id]}")
            del self.instances[id]
            del self.ref_counts[id]
        # how you do incr/decr ref is the key problem now.
        # this is because at serialization time incr/decr is called.
        # and it will implicitly call the calling actor.
        # to prevent this. we can:
        # 1. disable incr/decr at serializer
        # 2. manually incr/decr upon sending RemoteProxy.
        # make incr/decr inside actor
        # I think the proble in ray is that there is no Future for outsiders.
        # so, there is no way to make a multi client server using just Queue and Actor?
        # ah, so,, one actor is for all the incoming messages and the queue must not be shared to
        # any other actors. ok then, how do you avoid dead lock?
        # reentrant is a common problem.
        # usually this doesnt happen, but,,,it is happening now.
        # one possible solution is to make RemoteProxy call another actor for ref counting.
        # but, that also makes a problem. so, one way to mitigate this problem is to introduce another proxy.
        # why did I choose to incr ref count instead of making copies?

        return "decr_ref_success"

    async def call_id(self, id, args, kwargs):
        #print(f"call_id:{id},{args},{kwargs} trying to obtain lock")
        print(f"calling id,before lock")
        async with self.lock:
            print(f"calling id,after lock")
            #print(f"call_id:{id},{args},{kwargs} obtained lock")
            args, kwargs = await self._unwrap_inputs(args, kwargs)
            res = self.instances[id](*args, **kwargs)
            return await self._register_without_lock(res)

    async def call_method_id(self, id, method, args, kwargs):
        async with self.lock:
            args, kwargs = await self._unwrap_inputs(args, kwargs)
            res = getattr(self.instances[id], method)(*args, **kwargs)
            return await self._register_without_lock(res)

    async def call_operator_id(self, id, operator, args):
        """returns id or NotImplemented"""
        async with self.lock:
            args, kwargs = await self._unwrap_inputs(args=args, kwargs=None)
            res = getattr(self.instances[id], operator)(*args)
            if res is NotImplemented:
                return res
            else:
                return await self._register_without_lock(res)

    async def call_id_with_not_implemented(self, id, args, kwargs):
        async with self.lock:
            args, kwargs = await self._unwrap_inputs(args, kwargs)
            res = self.instances[id](*args, **kwargs)
            if res is NotImplemented:
                return res
            else:
                return await self._register_without_lock(res)

    async def fetch_id(self, id):
        async with self.lock:
            return self.instances[id]

    async def attr_of_id(self, id, attr: str):
        async with self.lock:
            return await self._register_without_lock(getattr(self.instances[id], attr))

    async def setattr_of_id(self, id, attr: str, value: Any):
        async with self.lock:
            attr = await self._unwrap(attr)
            value = await self._unwrap(value)
            tgt = self.instances[id]
            setattr(tgt, attr, value)

    async def getitem_of_id(self, id, item):
        async with self.lock:
            item = await self._unwrap(item)
            return await self._register_without_lock(self.instances[id][item])

    async def setitem_of_id(self, id, item, value):
        async with self.lock:
            item, value = await self._unwrap((item, value))
            self.instances[id][item] = value
            return await self._register_without_lock(None)

    async def dir_of_id(self, id):
        async with self.lock:
            res = dir(self.instances[id])
            return res

    async def copy_of_id(self, id):
        async with self.lock:
            item = self.instances[id]
            return await self._register_without_lock(item)

    async def type(self, id):
        async with self.lock:
            return type(self.instances[id])

    async def func_on_id(self, id, function: Callable) -> uuid.UUID:
        print(f"calling func on id, before lock")
        async with self.lock:
            print(f"obtained lock in calling func_on_id:{id}")
            data = function(self.instances[id])
            return await self._register_without_lock(data)

    async def next_of_id(self, id) -> uuid.UUID:
        # you cannot
        async with self.lock:
            # hmm, raising StopIteration from async function is a RuntimeError
            # so we must catch it and... raise something else instead.
            try:
                thing = next(self.instances[id])
                return await self._register_without_lock(thing)
            except StopIteration:
                raise MyStopIteration()


    async def iter_of_id(self, id) -> uuid.UUID:
        async with self.lock:
            return await self._register_without_lock(iter(self.instances[id]))

    def __repr__(self):
        return f"""Env@{self.host}(id={str(self.env_id)[:5]}...,{len(self.instances)} instances)"""

    async def status(self):
        async with self.lock:
            res = []
            for k in self.instances.keys():
                res.append(dict(
                    key=k,
                    type=type(self.instances[k]),
                    refs=self.ref_counts[k]
                ))
        return res

    async def put_named(self, item, name: str):
        print(f"put named before lock")
        async with self.lock:
            return await self._put_named_without_lock(item, name)

    async def _put_named_without_lock(self, item, name):
        print(f"put named:{name},{type(item)}")
        name, item = await self._unwrap((name, item))
        assert isinstance(name, str)
        if name in self.named_instances:
            print(f"replacing named instance:{name}")
            await self.del_named(name)
        _id = await self._register_without_lock(item)
        self.named_instances[name] = _id
        print(f"placed named instance:{name}")
        await self.incr_ref_id(_id)  # increment reference from this env.
        return _id

    async def del_named(self, item):
        from loguru import logger
        async with self.lock:
            logger.warning(f"deleting named instance:{item}")
            item = await self._unwrap(item)
            _id = self.named_instances[item]
            del self.named_instances[item]
            # hmm, without lock,
            # this can lead to removing the reference count
            # even thouth some one might be trying to reference it?
        await self.decr_ref_id(_id)

    async def get_named(self, name: str):
        print(f"get named before lock")
        async with self.lock:
            print(f"get_named called with name:{name}")
            name = await self._unwrap(name)
            print(f"unwrapped name:{name}")
            _id = self.named_instances[name]
            print(f"id of named instance:{_id}")
            assert _id in self.instances, f"registered named instance does not exist in instance table ({name})"
            return _id

    async def get_named_instances(self):
        async with self.lock:
            return {k: _id for k, _id in self.named_instances.items()}

    async def get_instance_names(self):
        async with self.lock:
            return list(self.named_instances.keys())

    def __contains__(self, item: Union[str, uuid.UUID]):
        if isinstance(item, str):
            return item in self.named_instances
        if isinstance(item, uuid.UUID):
            return item in self.instances

    async def contains(self, item: Union[str, uuid.UUID]) -> bool:
        print(f"contains check before lock")
        async with self.lock:
            print(f"contains check after lock")
            if isinstance(item, str):
                return item in self.named_instances
            if isinstance(item, uuid.UUID):
                return item in self.instances
