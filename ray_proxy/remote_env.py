from dataclasses import dataclass, field
from typing import TypeVar, Union, List, Dict
from uuid import UUID

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.util.client.common import ClientActorHandle
from returns.result import safe, Result

from ray_proxy.interface import IRemoteInterpreter
from ray_proxy.py_environment import PyInterpreter
from ray_proxy.remote_proxy import Var, RemoteBlockingIterator


@dataclass
class ActorRefRemoteInterpreter(IRemoteInterpreter):
    remote_env: ActorHandle
    remote_env_refs: ActorHandle = field(default=None)
    remote_env_id_ref: ObjectRef = field(default=None)
    remote_env_id:UUID = field(default=None)

    def __post_init__(self):
        if self.remote_env_id_ref is None:
            self.remote_env_id_ref = self.remote_env.get_env_id.remote()
        if self.remote_env_refs is None:
            self.remote_env_refs = self.remote_env
        assert isinstance(self.remote_env_id_ref, ObjectRef), f"remote env id must be an object ref"
        assert isinstance(self.remote_env, (ActorHandle, ClientActorHandle)), \
            f"main env actor must be an instance of ActorHandle,{type(self.remote_env)}"
        assert isinstance(self.remote_env_refs, (ActorHandle, ClientActorHandle)), \
            f"main env refs must be an instance of ActorHandle,{type(self.remote_env_refs)}"

    def delete_unnamed_vars(self):
        ray.get(self.remote_env.reset.remote())

    def run(self, func, args=None, kwargs=None) -> Var:
        return Var(
            self,
            self.remote_env.run_with_proxy.remote(func, args, kwargs)
        )

    def get_host(self) -> ObjectRef:
        return self.remote_env.get_host.remote()

    def put(self, item) -> Var:
        # logger.info(f"put item:{item}")
        return Var(
            self,
            self.remote_env.register.remote(item)
        )

    def put_named(self, name: str, item) -> Var:
        return Var(
            self,
            self.remote_env.put_named.remote(item, name)
        )

    def get_named_instances(self) -> Dict[str, Var]:
        return {k: Var(self, ray.put(_id)) for k, _id in
                ray.get(self.remote_env.get_named_instances.remote()).items()}

    def get_named(self, name: str) -> Var:
        return Var(
            self,
            self.remote_env.get_named.remote(name)
        )

    def del_named(self, name: str) -> ObjectRef:
        return self.remote_env.del_named.remote(name)

    def call_id(self, id, args=None, kwargs=None) -> Var:
        res_ref = self.remote_env.call_id.remote(id, args, kwargs)
        # ray.get(res_ref)
        return self.proxy(res_ref)

    def call_id_with_not_implemented(self, id, args=None, kwargs=None) -> Var:
        ref = self.remote_env.call_id_with_not_implemented.remote(id, args, kwargs)
        if ray.get(ref) == NotImplemented:
            return NotImplemented
        else:
            return self.proxy(ref)

    def method_call_id(self, id, method, args=None, kwargs=None) -> Var:
        return self.proxy(self.remote_env.call_method_id.remote(id, method, args, kwargs))

    def operator_call_id(self, id, operator, args):
        """returns NotImplementedObject if the operator returns NotImplemented"""
        ref = self.remote_env.call_operator_id.remote(id, operator, args)
        if ray.get(ref) == NotImplemented:
            return NotImplemented
        else:
            return self.proxy(ref)

    def attr_id(self, id: ObjectRef, item: str) -> Var:
        return self.proxy(self.remote_env.attr_of_id.remote(id, item))

    def setattr_id(self, id: ObjectRef, attr, value) -> ObjectRef:
        return self.remote_env.setattr_of_id.remote(id, attr, value)

    def unregister(self, id):
        return self.remote_env.unregister_id.remote(id)

    def fetch_id(self, id) -> ObjectRef:
        return self.remote_env.fetch_id.remote(id)

    def getitem_id(self, id, item) -> Var:
        return self.proxy(self.remote_env.getitem_of_id.remote(id, item))

    def dir_of_id(self, id) -> List[str]:
        return ray.get(self.remote_env.dir_of_id.remote(id))

    def incr_ref(self, id) -> ObjectRef:
        # we need to know if the remote_env is actually remote or not.
        return self.remote_env_refs.incr_ref_id.remote(id)

    def decr_ref(self, id) -> ObjectRef:
        # decr_ref must not be called inside PyEnvServerProxy
        # since it will cause deadlock.
        # we need to directly call the decr_ref_id of this proxy.
        # but how? maybe using global variables?
        #
        return self.remote_env_refs.decr_ref_id.remote(id)

    def type_of_id(self, id) -> Var:
        return self.func_on_id(id, type)

    def next_of_id(self, id) -> Var:
        return self.proxy(self.remote_env.next_of_id.remote(id))

    def copy_of_id(self, id) -> ObjectRef:
        return self.remote_env.copy_of_id.remote(id)

    def iter_of_id(self, id) -> RemoteBlockingIterator:
        it = self.proxy(self.remote_env.iter_of_id.remote(id))
        return RemoteBlockingIterator(it)

    def proxy(self, id: ObjectRef) -> Var:
        return Var(self, id)

    def func_on_id(self, id, f):
        return self.proxy(self.remote_env.func_on_id.remote(id, f))

    def status(self):
        return ray.get(self.remote_env.status.remote())

    def __getitem__(self, item: str):
        return self.get_named(item)

    def __setitem__(self, name, value):
        return self.put_named(name, value)

    @property
    def id(self) -> ObjectRef:
        return self.remote_env_id_ref

    def instance_names(self) -> List[str]:
        return ray.get(self.remote_env.get_instance_names.remote())

    def __contains__(self, item: Union[str, UUID, ObjectRef]) -> bool:
        return ray.get(self.remote_env.__contains__.remote(item))


@dataclass
class RemoteInterpreterFactory:
    ray: "ray"

    def create(self, **options) -> IRemoteInterpreter:
        actor = self.ray.remote(PyInterpreter).options(**options).remote()
        return ActorRefRemoteInterpreter(actor)

    def get(self, name) -> IRemoteInterpreter:
        actor = ray.get_actor(name)
        kind = ray.get(actor.get_kind.remote())
        return ActorRefRemoteInterpreter(actor)

    def __getitem__(self, env_name) -> IRemoteInterpreter:
        return self.get(env_name)

    def destroy(self, name) -> Result:
        return safe(ray.get_actor)(name).map(ray.kill)

    def get_or_create(self, name, **options) -> IRemoteInterpreter:
        return self.create(
            name=name,
            get_if_exists=True,
            # lifetime="detached",
            **options,
        )


T = TypeVar("T")
