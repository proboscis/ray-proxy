from pinject_design import Injected
from ray_proxy.cluster_resource import Resources


def run_injected(injected: Injected, res: Resources):
    kwargs = {k: v[0].value for k, v in res.items()}
    return injected.get_provider()(**kwargs)
