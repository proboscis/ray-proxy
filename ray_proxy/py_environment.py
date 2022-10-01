import socket
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Union, Any

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.util.queue import Queue
from bidict import bidict


@dataclass
class PyInterpreter:
    env_id: uuid.UUID = field(default_factory=uuid.uuid4)
    instances: dict = field(default_factory=dict)  # id->instance mapping
    ref_counts: dict = field(
        default_factory=lambda: defaultdict(int))  # todo ref_counts is not reliable at this moment.
    named_instances: bidict = field(default_factory=bidict)  # mapping from name to id and vice versa.

    def __post_init__(self):
        self.host = socket.gethostname()
        print(f"PyInterpreter created at {self.host}")

    def reset(self):
        from loguru import logger
        logger.info(f"resetting interpreter. only named variables are kept.")
        prev_instances = self.instances
        tmp = self.named_instances

        self.instances = dict()
        self.ref_counts = defaultdict(int)
        self.named_instances = bidict()
        for name, _id in tmp.items():
            self.put_named(prev_instances[_id], name)
        logger.info(f"reset done:\n{self.status()}")
        logger.info(f"self.named_instances:{self.named_instances}")

    def get_kind(self):
        return "PyInterpreter"

    def get_host(self):
        return self.host

    def get_env_id(self):
        return self.env_id

    def run(self, func: Callable, args=None, kwargs=None):
        return self.run_with_proxy(func, args, kwargs)

    def _unwrap(self, proxy: "RemoteProxy"):
        from ray_proxy.remote_proxy import Var
        if isinstance(proxy, Var):
            # unwrap if the proxy lives in this environment
            from loguru import logger
            assert isinstance(proxy.env.id, ObjectRef), f"proxy.env.id is not a reference:{type(proxy.env.id)}"
            eid = ray.get(proxy.env.id)
            if self.env_id == eid:
                return self.instances[ray.get(proxy.id)]
            else:
                logger.warning(f"fetching from a proxy which lives in different env:{eid}")
                return proxy.fetch()
        if isinstance(proxy, tuple):
            return tuple(self._unwrap(item) for item in proxy)
        if isinstance(proxy, list):
            return [self._unwrap(item) for item in proxy]
        if isinstance(proxy, dict):
            return {self._unwrap(key): self._unwrap(value) for key, value in proxy.items()}

        return proxy

    def _unwrap_inputs(self, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = dict()
        args = [self._unwrap(arg) for arg in args]
        kwargs = {key: self._unwrap(value) for key, value in kwargs.items()}
        return args, kwargs

    def run_with_proxy(self, func: Callable, args=None, kwargs=None):
        args, kwargs = self._unwrap_inputs(args, kwargs)
        func = self._unwrap(func)
        return self.register(func(*args, **kwargs))

    def register(self, instance):
        instance = self._unwrap(instance)
        import uuid
        uuid = uuid.uuid4()
        while uuid in self.instances:
            uuid = uuid.uuid4()
        self.instances[uuid] = instance
        self.incr_ref_id(uuid)
        return uuid

    def unregister_id(self, id):
        from loguru import logger
        print(f"unregister_id:{id}")
        self.decr_ref_id(id)

    def incr_ref_id(self, id):
        # print(f"incr_ref_id:{id}")
        self.ref_counts[id] += 1
        return "incr_ref_success"

    def decr_ref_id(self, id):
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

    def call_id(self, id, args, kwargs):
        args, kwargs = self._unwrap_inputs(args, kwargs)
        res = self.instances[id](*args, **kwargs)
        return self.register(res)

    def call_method_id(self, id, method, args, kwargs):
        args, kwargs = self._unwrap_inputs(args, kwargs)
        res = getattr(self.instances[id], method)(*args, **kwargs)
        return self.register(res)

    def call_operator_id(self, id, operator, args):
        """returns id or NotImplemented"""
        args, kwargs = self._unwrap_inputs(args=args, kwargs=None)
        res = getattr(self.instances[id], operator)(*args)
        if res is NotImplemented:
            return res
        else:
            return self.register(res)

    def call_id_with_not_implemented(self, id, args, kwargs):
        args, kwargs = self._unwrap_inputs(args, kwargs)
        res = self.instances[id](*args, **kwargs)
        if res is NotImplemented:
            return res
        else:
            return self.register(res)

    def fetch_id(self, id):
        return self.instances[id]

    def attr_of_id(self, id, attr: str):
        return self.register(getattr(self.instances[id], attr))

    def setattr_of_id(self, id, attr: str, value: Any):
        attr = self._unwrap(attr)
        value = self._unwrap(value)
        tgt = self.instances[id]
        setattr(tgt, attr, value)

    def getitem_of_id(self, id, item):
        item = self._unwrap(item)
        return self.register(self.instances[id][item])

    def dir_of_id(self, id):
        res = dir(self.instances[id])
        return res

    def copy_of_id(self, id):
        item = self.instances[id]
        return self.register(item)

    def run_generator(self, id, stop_signal: ActorHandle, receiver: Queue):
        from loguru import logger
        last_check = datetime.now()
        check_period = timedelta(seconds=1)
        # should I make this async? idk...

        try:
            for i, item in enumerate(self.instances[id]):
                now = datetime.now()
                dt = now - last_check
                if dt > check_period:
                    if ray.get(stop_signal.get.remote()):
                        break
                receiver.put((i, item))
                last_check = now
            receiver.put((None, None))
        except Exception as e:
            logger.error(f"generator error in remote env: {e}")
            logger.error(f"{traceback.format_exc()}")
            receiver.put(("error", e))

    def type(self, id):
        return type(self.instances[id])

    def func_on_id(self, id, function: Callable) -> uuid.UUID:
        return self.register(function(self.instances[id]))

    def next_of_id(self, id) -> uuid.UUID:
        return self.register(next(self.instances[id]))

    def iter_of_id(self, id) -> uuid.UUID:
        return self.register(iter(self.instances[id]))

    def __repr__(self):
        return f"""Env@{self.host}(id={str(self.env_id)[:5]}...,{len(self.instances)} instances)"""

    def status(self):
        res = []
        for k in self.instances.keys():
            res.append(dict(
                key=k,
                type=type(self.instances[k]),
                refs=self.ref_counts[k]
            ))
        return res

    def put_named(self, item, name: str):
        from loguru import logger
        name = self._unwrap(name)
        item = self._unwrap(item)
        assert isinstance(name, str)
        if name in self.named_instances:
            from loguru import logger
            logger.warning(f"replacing named instance:{name}")
            self.del_named(name)
        _id = self.register(item)
        self.named_instances[name] = _id
        logger.warning(f"placed named instance:{name}")
        self.incr_ref_id(_id)  # increment reference from this env.
        return _id

    def del_named(self, item):
        from loguru import logger
        logger.warning(f"deleting named instance:{item}")
        item = self._unwrap(item)
        _id = self.named_instances[item]
        del self.named_instances[item]
        self.decr_ref_id(_id)

    def get_named(self, name: str):
        name = self._unwrap(name)
        _id = self.named_instances[name]
        assert _id in self.instances, f"registered named instance does not exist in instance table ({name})"
        return _id

    def get_named_instances(self):
        return {k: _id for k, _id in self.named_instances.items()}

    def get_instance_names(self):
        return list(self.named_instances.keys())

    def __contains__(self, item: Union[str, uuid.UUID]):
        if isinstance(item, str):
            return item in self.named_instances
        if isinstance(item, uuid.UUID):
            return item in self.instances
