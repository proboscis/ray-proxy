import atexit
from dataclasses import dataclass
from typing import Iterable

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError
from ray.util.queue import Queue

from ray_proxy.interface import IRemoteInterpreter


@dataclass
class Var:
    """
    a proxy class that represents a variable which lives in the remote python interpreter.
    this, can be copied and used from multiple remote places if I implemnt the corrent reference counting mechanism!
    until then only one process can use this.
    todo implement an reference counting actor. which can just live in remote environment.
    then call its incr/decr upon creating/deleting this instance
    when you send thie proxy to another, call incr() un receiving side.
    """
    env: IRemoteInterpreter  # RemoteEnvironment
    id: ObjectRef  # an id which can identify the variable at remote environment.

    def __post_init__(self):
        assert isinstance(self.id, ObjectRef)
        self.___proxy_dirs___ = None
        self.released = False
        atexit.register(self.__atexit__)

    def __atexit__(self):
        # this is required because the __del__ is called
        # after the imported modules are destroyed on interpreter shutdown.
        self.env.decr_ref(self.id)
        self.released = True

    def fetch(self):
        return ray.get(self.fetch_ref())

    def fetch_ref(self):
        return self.env.fetch_id(self.id)

    def __str__(self):
        _str: Var = self.env.func_on_id(self.id, str)  # this returns proxy
        host = self.env.get_host()
        host, _repr, _id = ray.get([host, _str.fetch_ref(), self.env.id])
        # Var@zeus[abcde]:hello world
        return f"Var@{host}[{str(_id)[:5]}]:{_repr}"

    def __repr__(self):
        return str(self)

    def _remote_attr(self, item):
        try:
            return self.env.attr_id(self.id, item)
        except RayTaskError as e:
            if "no attribute" in e.cause:
                raise AttributeError(item)
            else:
                raise e

    def __getattr__(self, item):
        # ah, the torch asks for __torch_function__ which must return 'NotImplemented'.
        # and yes, __torch_function__ is implemented in remote, so we need to convert it to a local function
        # but implementing it means that the __torch_function__ is called remotely. and it is not what we want.
        # we want the left side to be uploaded, so..
        # at this time we just raise AttributeError?
        # but if you implement this, you can call torch.mean(RemoteProxy).
        # logger.info(f"getattr is called for {item}")
        if item == "__getstate__":  # this is crucial for sending this object to remote!
            raise AttributeError(item)
        elif item == "__setstate__":
            raise AttributeError(item)
        # logger.debug(f"__getattr__({item})")
        if item == "__bases__":
            raise AttributeError(item)
        if item == "__remote_class__":
            return self._remote_attr("__class__")
        if self.___proxy_dirs___ is None:
            self.___proxy_dirs___ = set(self.__dir__())
        if item.startswith("_") and item not in self.___proxy_dirs___:
            raise AttributeError(item)
        if item not in self.___proxy_dirs___:
            raise AttributeError(item)
        if item.startswith("_repr_"):  # for ipython visualizations
            return self._remote_attr(item).fetch()
        res = self._remote_attr(item)

        if item == "__torch_function__":
            # we found the attr and it is __torch_function__. so make it a complete function
            # this downloads the tensor to local... so
            def __torch_function__(func, types, args=(), kwargs=None):
                return res.env.call_id_with_not_implemented(res.id, (func, types), dict(args=args, kwargs=kwargs))

            return __torch_function__

        return res

    def __setattr__(self, key, value):
        from loguru import logger
        if key in "id env ___proxy_dirs___ released".split():
            return super().__setattr__(key, value)
        logger.info(f"setting attribute:{key}={value}")
        res = self.env.setattr_id(self.id, key, value)
        self.___proxy_dirs___ = None  # invalidate dir cache so that we can access the new attr
        return ray.get(res)

    def __call__(self, *args, **kwargs):
        return self.env.call_id(self.id, args, kwargs)

    def __getitem__(self, item):
        return self.env.getitem_id(self.id, item)

    def __iter__(self):
        return self.env.iter_of_id(self.id)

    def __del__(self):
        print(f"dereferencing")
        # logger.info(f"dereferencing:{self.id}")
        if not self.released:
            self.env.decr_ref(self.id)
            print(f"decr_ref:{self.id}")
            atexit.unregister(self.__atexit__)
        print(f"dereferencing done")

    def __dir__(self) -> Iterable[str]:
        _dir = self.env.dir_of_id(self.id)
        # print(f"dir is called for {self}")
        return _dir
        # return ray.get(self.remote_env.dir_of_id.remote(self.id))

    def ___call_method___(self, method, args, kwargs):
        return self.env.method_call_id(self.id, method, args, kwargs)
        # return self._proxy(self.remote_env.call_method_id.remote(self.id, method, args, kwargs))

    def ___call_operator___(self, operator, *args):
        args = [self.env.put_if_needed(a) for a in args]
        return self.env.operator_call_id(self.id, operator, args)

    def ___call_left_operator___(self, operator, arg):
        res = self.___call_operator___(operator, arg)
        if res == NotImplemented:
            # regardless of the type of arg, we need to call its right operator
            op_name = operator.replace("__", "")
            res = self.env.put(arg).___call_operator___(f"__r{op_name}__", self)
        return res

    def ___call_right_operator___(self, operator, arg):
        res = self.___call_operator___(operator, arg)
        if res == NotImplemented and not isinstance(arg, Var):
            # we need to manually call the left side operator of the arg.
            op_name = operator.replace("__r", "__")
            res = self.env.put(arg).___call_operator___(op_name, self)
        else:
            pass
        return res

    def __add__(self, other):
        return self.___call_left_operator___("__add__", other)

    def __radd__(self, other):
        return self.___call_right_operator___("__radd__", other)

    def __mul__(self, other):
        return self.___call_left_operator___("__mul__", other)

    def __rmul__(self, other):
        return self.___call_right_operator___("__rmul__", other)

    def __mod__(self, other):
        return self.___call_left_operator___("__mod__", other)

    def __rmod__(self, other):
        return self.___call_right_operator___("__rmod__", other)

    def __truediv__(self, other):
        return self.___call_left_operator___("__truediv__", other)

    def __rtruediv__(self, other):
        return self.___call_right_operator___("__rtruediv__", other)

    def __sub__(self, other):
        return self.___call_left_operator___("__sub__", other)

    def __rsub__(self, other):
        return self.___call_right_operator___("__rsub__", other)

    def __eq__(self, other):
        # the behavior is, if both of them return NotImplemented, the interpreter checks for id
        # so we can safely call directly.
        return self.___call_operator___("__eq__", other)

    def __bool__(self):
        return self.___call_operator___("__bool__").fetch()

    def __type__(self):
        return self.env.type_of_id(self.id).fetch()

    def __len__(self):
        return self.___call_operator___("__len__").fetch()


def rp_deserializer(state):
    rp = Var(state[0], state[1])
    return rp


def rp_serializer(rp: Var):
    rp.env.incr_ref(rp.id)
    return rp.env, rp.id


# None of __getstate__ __setstate__ __reduce__ worked for serialization. this register_serializer is my last hope!
def register_rp_serializer():
    # THIS WORKED!?
    from loguru import logger
    print(f"trying to register a serializer for Var object for the ray serializer")

    ray.util.register_serializer(
        Var, serializer=rp_serializer, deserializer=rp_deserializer
    )
    logger.info("successfully registered Var serializer!")


register_rp_serializer()


@dataclass
class RemoteBlockingIterator:
    src: Var

    def __iter__(self):
        return self

    def __next__(self):
        try:
            proxy: Var = self.src.env.next_of_id(self.src.id)
            ray.get(proxy.id)  # this is for raising StopIteration
            return proxy
        except RayTaskError as e:
            if isinstance(e.cause, StopIteration):
                raise e.cause
            else:
                raise e


@dataclass
class RemoteIterator:
    in_queue: Queue
    stop_signal: ActorHandle

    def __next__(self):
        try:
            header, item = self.in_queue.get()
            if header == "error":
                from loguru import logger
                logger.info(f"error on remote env!")
                raise item
            elif header is None:
                raise StopIteration()
            return item
        except KeyboardInterrupt as e:
            self.stop_signal.set.remote(True)
            raise e

    def __iter__(self):
        return self
