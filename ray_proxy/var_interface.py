from abc import ABCMeta, abstractmethod


class IVar(metaclass=ABCMeta):
    @abstractmethod
    def __atexit__(self):
        pass

    @abstractmethod
    def __post_init__(self):
        pass

    @abstractmethod
    def __del__(self):
        pass

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def fetch_ref(self):
        pass

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def _remote_attr(self, item):
        pass

    @abstractmethod
    def __getattr__(self, item):
        pass

    @abstractmethod
    def __setattr__(self, key, value):
        pass

    @abstractmethod
    def __call__(self, args, kwargs):
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def __setitem__(self, item, value):
        pass

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __dir__(self):
        pass

    @abstractmethod
    def ___call_method___(self, method, args, kwargs):
        pass

    @abstractmethod
    def ___call_operator___(self, operator, args):
        pass

    @abstractmethod
    def ___call_left_operator___(self, operator, arg):
        pass

    @abstractmethod
    def ___call_right_operator___(self, operator, arg):
        pass

    @abstractmethod
    def __add__(self, other):
        pass

    @abstractmethod
    def __radd__(self, other):
        pass

    @abstractmethod
    def __mul__(self, other):
        pass

    @abstractmethod
    def __rmul__(self, other):
        pass

    @abstractmethod
    def __mod__(self, other):
        pass

    @abstractmethod
    def __rmod__(self, other):
        pass

    @abstractmethod
    def __truediv__(self, other):
        pass

    @abstractmethod
    def __rtruediv__(self, other):
        pass

    @abstractmethod
    def __sub__(self, other):
        pass

    @abstractmethod
    def __rsub__(self, other):
        pass

    @abstractmethod
    def __eq__(self, other):
        pass

    @abstractmethod
    def __bool__(self):
        pass

    @abstractmethod
    def __type__(self):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __lt__(self, other):
        pass

    @abstractmethod
    def __le__(self, other):
        pass

    @abstractmethod
    def __ge__(self, other):
        pass

    @abstractmethod
    def __gt__(self, other):
        pass

    @abstractmethod
    def __ne__(self, other):
        pass

    @abstractmethod
    def __neg__(self, other):
        pass

    @abstractmethod
    def __await__(self):
        pass