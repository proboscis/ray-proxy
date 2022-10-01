import re
from dataclasses import dataclass

import ray
from ray_proxy import IRemoteInterpreter, Var, RemoteInterpreterFactory


def get_interpreter(**ray_actor_options) -> IRemoteInterpreter:
    if not ray.is_initialized():
        ray.init()
    rif = RemoteInterpreterFactory(ray)
    return rif.create(**ray_actor_options)


def test_repr():
    e = get_interpreter()
    remote_x = e.put("abcde")
    pat = f"^Var@.*abcde$"
    matches = re.findall(pat, repr(remote_x))
    assert matches, str(matches)


def test_bool():
    e = get_interpreter()
    flag = e.put(True)
    assert flag
    assert flag == True


def test_remote_generator():
    env = get_interpreter()
    assert [i.fetch() for i in env.put(range(10))] == list(range(10))


def test_remote_env_run():
    env = get_interpreter()
    x = env.put("hello")
    y = env.put("world")
    f = env.put(lambda x: x)  # idk why this doesnt work while the above works.
    assert isinstance(x, Var)
    assert isinstance(y, Var)
    assert isinstance(f(x), Var)
    assert f(x) == "hello"
    assert env.run(lambda x, y: x + " " + y, args=(x, y)).fetch() == "hello world"
    assert x + " " + y == "hello world"


def test_serializing_proxy():
    e = get_interpreter()
    x = e.put("hello")
    y = e.put("world")
    f = e.put(lambda x: x)  # idk why this doesnt work while the above works.
    x1: Var = e.put("x1")

    @ray.remote
    def remote_func(x):
        assert isinstance(x, Var)
        return x

    res = remote_func.remote(x1)
    result = ray.get(res)
    assert isinstance(result, Var)
    assert result == "x1"


def test_passing_remote_proxy():
    e1 = get_interpreter()
    e2 = get_interpreter()
    print(e1)
    print(e2)
    x_e1 = e1.put("x")
    print(x_e1)
    x_e2 = e2.put(x_e1)  # ref2
    print(x_e2)
    assert x_e1 == x_e2
    added: Var = x_e1 + x_e2
    assert added.env.id == x_e1.env.id
    assert added == "xx"


def test_reset_env():
    e = get_interpreter()
    e["x"] = "hello world"
    e.delete_unnamed_vars()
    print(e["x"])


@dataclass
class A:
    test_att: str

    def greet(self):
        return "hello !"


def test_remote_setattr():
    env = get_interpreter()
    x = env.put(A("hello"))
    x.test_att = "test_attr"
    eq = x.test_att == "test_attr"
    print(x)
    print(x.test_att)
    print(eq)
    assert eq


def test_remote_update_method():
    env = get_interpreter()
    x = env.put(A("hello"))
    x.__class__.greet = lambda self: "greetings"
    print(x.greet())
    assert x.greet() == "greetings"
