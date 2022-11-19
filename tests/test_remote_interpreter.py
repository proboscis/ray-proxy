import asyncio
import re
import threading
import time
from asyncio import Lock
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


def test_lt():
    e = get_interpreter()
    x = e.put(10)
    assert x < 20
    assert not (x < 10)
    assert x <= 20
    assert x <= 10
    assert 10 <= x
    assert (x <= 10)
    assert not (x < 10)
    assert 8 < x
def test_gt():
    e = get_interpreter()
    x = e.put(10)
    assert x > 0
    assert x >= 10
    assert x > 9
    assert not (x > 10)



def test_remote_generator():
    env = get_interpreter()
    assert [i.fetch() for i in env.put(range(10))] == list(range(10))


def test_put_item():
    env = get_interpreter()
    x = env.put("hello")
    print(x)


def test_remote_env_run():
    env = get_interpreter()
    x = env.put("hello")
    y = env.put("world")
    f = env.put(lambda x: x)  # idk why this doesnt work while the above works.
    assert isinstance(x, Var)
    assert isinstance(y, Var)
    assert isinstance(f(x), Var)
    print(x)
    print(y)
    print(f)
    print(f(x))
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


async def wait_task(task):
    return await task


class B:
    def __init__(self):
        self.lock = Lock()

    async def a(self, x):
        async with self.lock:
            print("a")
            return x

    async def b(self, x):
        async with self.lock:
            print("b")
            return x

    async def c(self, x):
        async with self.lock:
            print("c")
            return x


def test_async_actor():
    actor = ray.remote(B).remote()
    x = "hello"
    ref_a = actor.a.remote(x)
    ref_b = actor.b.remote(x)
    ref_c = actor.c.remote(x)
    print(ray.get(ref_c))


def test_put_named():
    env = get_interpreter()
    # rstr = env.put(str)
    # x = env.put("hello")
    # y = rstr(x)
    # env["x"] = "hello"
    env.put_named("x", 0)
    # x = env["x"]
    # print(x)
    # for some reason this containes_check is executed before the execution of
    # previous commands. why??
    # time.sleep(1) # this sleep makes the env work as expected
    assert "x" in env
    # the reason why we get 'contains' executed first is that
    # the name does not depend on the ray's ObjectRef chain.
    # this means that accessing variables through string identifiers is totally
    # meaningless.
    # one possible solution would be to always sync when calling 'put'
    # or have a manager class to remember what is being put with name!
    # and use the identifiers to retrieve.
    # but this means that we need to sync the state of name_id manager.
    # since we don't often use named_put, I think it is safe to make the put_named as blocking

def test_async_fetch():
    env= get_interpreter()
    x = env.put("hello")
    async def test_await():
        return await x

    awaited = asyncio.run(test_await())
    assert awaited == "hello"
