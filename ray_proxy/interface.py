from abc import ABCMeta, abstractmethod
from typing import List, Union
from uuid import UUID

from ray import ObjectRef


class IRemoteInterpreter(metaclass=ABCMeta):

    def delete_unnamed_vars(self):
        """invalidates all var stored except named ones"""
        pass

    @abstractmethod
    def run(self, func, args=None, kwargs=None):
        """run a provided function with args and kwargs at this interpreter"""
        pass

    @abstractmethod
    def get_host(self):
        """the name of the host which this interpreter is running on"""
        pass

    @abstractmethod
    def put(self, item) -> "Var":
        """puts a new item to this interpreter,
        returns new Var which holds the id
        of given item stored in this interpreter"""
        pass

    @abstractmethod
    def put_named(self, name, item):
        """puts a new item to this interpreter associating the provided name to be retrieved later,
        returns new Var which holds the id
        of given item stored in this interpreter"""
        pass

    @abstractmethod
    def get_named_instances(self):
        """returns a list of all named instances in this interpreter"""
        pass

    @abstractmethod
    def get_named(self, name):
        """returns a single named instance in this interpreter"""
        pass

    @abstractmethod
    def del_named(self, name):
        """deletes a named instance in this interpreter"""
        pass

    @abstractmethod
    def call_id(self, id, args, kwargs):
        """calls __call__ function of the variable with provided id with the given arguments and kwargs at this interpreter"""
        pass

    @abstractmethod
    def call_id_with_not_implemented(self, id, args=None, kwargs=None):
        """call a funciton which is associated with the id, returning NotImplemented if pappropriate"""
        pass

    @abstractmethod
    def method_call_id(self, id, method, args=None, kwargs=None):
        """calls a method of id-associated object"""
        pass

    @abstractmethod
    def operator_call_id(self, id, operator, arg):
        """calls an operator of id-associated object"""
        pass

    @abstractmethod
    def attr_id(self, id, item):
        """calls an getattr of id-associated object"""
        pass

    @abstractmethod
    def setattr_id(self, id, item, value):
        """calls setattr on id-associated object"""
        pass

    @abstractmethod
    def unregister(self, id):
        """unregister this id from this interpreter"""
        pass

    @abstractmethod
    def fetch_id(self, id):
        """fetch the actual value of id-associated object"""
        pass

    @abstractmethod
    def getitem_id(self, id, item):
        """calls __getitem__ on the id-associated object"""
        pass

    @abstractmethod
    def dir_of_id(self, id):
        """calls dir() on the id-associated object"""
        pass

    @abstractmethod
    def incr_ref(self, id):
        """increase the ref-count on the id-associated object"""
        pass

    @abstractmethod
    def decr_ref(self, id):
        """decrease the ref-count on the id-associated object"""
        pass

    @abstractmethod
    def type_of_id(self, id):
        """calls type() on the id-associated object"""
        pass

    @abstractmethod
    def next_of_id(self, id):
        """calls next() on the id-associated object"""
        pass

    @abstractmethod
    def copy_of_id(self, id):
        """calls copy() on the id-associated object"""
        pass

    @abstractmethod
    def iter_of_id(self, id):
        """calls iter() on the id-associated object"""
        pass

    @abstractmethod
    def func_on_id(self, id, f):
        """calls f on the id-associated object"""
        pass

    @abstractmethod
    def status(self):
        """returns the current status of the interpreter"""
        pass

    @abstractmethod
    def __getitem__(self, item):
        """returns a Var in this interpreter by name. same as get_named"""
        pass

    @abstractmethod
    def __setitem__(self, item, value):
        """sets a Var in this interpreter by name. same as put_named"""
        pass

    @property
    @abstractmethod
    def id(self) -> ObjectRef:
        """an ObjectRefj holding the id of this interpreter"""
        pass

    def put_if_needed(self, item):
        """calls put if item is not an instance of Var"""
        from ray_proxy.remote_proxy import Var
        if isinstance(item, Var):
            return item
        else:
            return self.put(item)

    @abstractmethod
    def instance_names(self) -> List[str]:
        """returns a list of names of instances of this interpreter"""
        pass

    @abstractmethod
    def __contains__(self, item: Union[str, UUID]):
        """returns True if item is in this interpreter"""
        pass
