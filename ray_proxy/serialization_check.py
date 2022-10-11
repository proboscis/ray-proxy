from pprint import pformat

import ray


@ray.remote
def ray_get_put_test(x):
    return ray.get(ray.put(x))


def rec_valmap(f, tgt: dict):
    res = dict()
    for k, v in tgt.items():
        if isinstance(v, dict):
            res[k] = rec_valmap(tgt[k], v)
        else:
            res[k] = f(v)
    return res


def rec_val_filter(f, tgt: dict):
    res = dict()
    for k, v in tgt.items():
        if isinstance(v, dict):
            res[k] = rec_val_filter(f, v)
        elif f(v):
            res[k] = v
    return res


def check_ray_serialization(x):
    from returns.result import safe
    from returns.result import Failure
    @safe
    def test_func(item):
        return ray.get(ray_get_put_test.remote(item))

    res = test_func(x)
    if isinstance(res, Failure):
        rec_check = rec_valmap(lambda v: (test_func(v), v), x)
        failures = rec_val_filter(lambda v: isinstance(v[0], Failure), rec_check)
        from logging import getLogger
        logger = getLogger(__name__)
        logger.error(f"faild to pass object around in ray ecosystem the causing items are:{pformat(failures)}")
        return failures
    else:
        return res
