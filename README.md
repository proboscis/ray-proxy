# Ray-Proxy

> A library to create proxy variables for remote python objects in [Ray](https://docs.ray.io/en/latest/index.html)
> ecosystem.

In python, multiprocessing and remote function calls are cumbersome stuff to work with for leveraging multiple machine
resources.
Although this pain is greatly mitigated by Ray, The need of creating an Actor class for each stateful task in ray
ecosystem makes it hard to write multi-machine codes seamlessly and interactively.
This library aims to make it possible to write multi-machine code as seamless as writing a simple function in
interactive jupyter notebook.

Here are some examples of what you can do with ray-proxy.

```python
import ray
from ray_proxy import RemoteInterpreterFactory, IRemoteInterpreter, Var

ray.init()  # connect to your ray cluster if you have one, otherwise ray will create a local one.
rif = RemoteInterpreterFactory(ray)
env: IRemoteInterpreter = rif.create(num_cpus=1)  # this will create a remote ray actor that holds any python objects
# we can send any serializable python object to remote side.
x = env.put("x")  # send any python variable to remote and get a handle for it.
n = env.put(10)
f = env.put(lambda x: x * 2)  # you can send a function, or a class.
print(x)  # Var@cpu-server[abcde]:"x" 
print(n)  # Var@cpu-server[abcde]:10 
assert isinstance(x, Var)
assert x == "x"
# now we can do any calculation on remote variables
nx = x * n  # == "xxxxxxxxxx"
assert isinstance(nx, Var)
assert nx == "xxxxxxxxxx" == x * 10  # this is realized by calling remote __bool__ operator
assert f(x) == "xx"  # you can call the remote function directly.
assert x.fetch() == "x"  # calling fetch() on Var will retrieve the remote variable
assert f(x).fetch() == f(x) == x * 2 == "xx"
```
## __getitem__/__setitem__
```python
d = env.put(dict(a=0))
assert d['a'] == 0
d['b'] = 15
print(d['a']) # Var@cpu-server[abcde]:0
print(d['b']) # Var@cpu-server[abcde]:15
```
## Remote Generator
```python
# actually we can iterate over remote variable like so:
for item in nx: # Var@cpu-server[abcde]:"xxxxxxxxxx"  
    print(item) # Var@cpu-server[abcde]:"x"  

a,b,c = ray.put(range(3)) # this works since remote generator works.
```

## GPU example
```python
import ray
from ray_proxy import RemoteInterpreterFactory, IRemoteInterpreter, Var

ray.init()  # connect to your ray culuster if you have one, otherwise ray will create a local one.
# in this case we need a server with at least 1 gpu resource.
rif = RemoteInterpreterFactory(ray)
env: IRemoteInterpreter = rif.create(num_gpus=1)  # this asks the ray to create an actor with 1 GPU reosurce
# now lets create torch tensor on gpu
import torch
r_torch = env.put(torch) # we need access to remote torch module. r_ is meant to indicate that the variable lives in remote side.
r_x = r_torch.arange(10).cuda() # we can directly access the remote variable's attributes through remote getattr call.
r_x += 1 # we can access any operator of remote variable, so almost all operations on a tensor works the same as the local tensor.
r_x.fetch() # this fails if the local python has no gpu available.
r_x.cpu().fetch() # always works.
```

When interactively building a python program which requires a long start up preparation, such as loading large GPU model,
this library can help you to work with it without reinitializing the python interpreter when you make changes.

## Multi interpreter example
```python
import ray
import torch
from ray_proxy import RemoteInterpreterFactory, IRemoteInterpreter, Var
ray.init()
rif = RemoteInterpreterFactory(ray)
env1: IRemoteInterpreter = rif.create(num_gpus=1) 
env2: IRemoteInterpreter = rif.create(num_gpus=1)
# now we have two interpreter on different machine/processs
# we can create a pipeline
r1_torch = env1.put(torch)
r2_torch = env2.put(torch)
x1 = r1_torch.arange(10).cuda()
x2 = r2_torch.arange(10).cuda()
y = x1 + x2 # we can do calculation on two different remote actor
assert y.env.id == x1.env.id # y now lives in x1's environment because x1 has __add__ operator implemented. 
# note that if x1 doesn' have __add__ operator, then __radd__ of x2 will be called and y will live in x2's environment.
# we prioritize uploading local variable rather than downloading remote object.
```
This allows creation of multiple gpu enabled environment creation on multiple ray nodes, and efficiently connecting them together.
This is especially the case when you have multiple tasks that requires models placed on GPU and takes a lot of time to load parameters.

# Installation
> on Macbook with Apple Silicon, please install grpcio from conda after installing ray-proxy.
```
pip install ray-proxy
```
If you see symbol not found... error from protbuf, downgrade protobuf to 3.20.1. see [here](https://github.com/protocolbuffers/protobuf/issues/10571)

# Notes
## Asynchronous Call and Synchronous Call
Most of the calls on Var object is done asynchronously through ray's actor system.
Some operators such as __bool__, __repr__, __str__, __type__ and __len__ are synchronous and returns local variable by calling fetch() interanally.
```python
x = env.put([[[[0]]]])
value = x[0][0][0][0] # queries successive asynchronous remote variable access. 
print(value) # synchronously calls __str__ for printing remote __str__ call result.
```
# Overhead
Each invocation of operator on Var object requires 1~ ray.remote calls under the hood.

# Automatic unwrapping on remote function
When you call a remote function with some parameters, the parameters get automatically unwrapped from Var on remote side.
```python
def test_remote_func(a:int,b:str,c:float):
    assert isinstance(a,int)
    assert isinstance(b,str)
    assert isinstance(c,float)
a,b,c = env.put((0,"b",1.0))
r_func = env.put(test_remote_func)
r_func(a,b,c) # works fine. a,b,c gets automatically unwrapped at the time they get passed to this function remotely.
c2 = env2.put(2.5) # var at different env works fine too.
r_func(a,b,c2) # this is done by automatically calling c2.fetch() on r_func's env.
r_func(0,'x',c2) # local variable and remote variable can be used at the same time.
# it is also valid to put and call at the same line.
env.put(test_remote_func)(a,b,c)
assert env.put(type)(a) == int 

```
