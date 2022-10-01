from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional, Iterable

import pandas as pd
import tqdm

from ray_proxy.ray_queue import RayQueue


@dataclass
class RayProgBarManager:
    # start a client thread that creates prog bar.
    bar_factory: Callable = field(default=tqdm.tqdm)
    bar_creation_queue: RayQueue = field(default_factory=RayQueue)

    def __post_init__(self):
        self.bar_creation_queue.serve_on_thread(self._serve_bar_creation)
        self.factory = RemoteProgBarFactory(self.bar_creation_queue)

    def _serve_bar_creation(self, msg):
        bar_events, kwargs = msg
        bar = self.bar_factory(**kwargs)

        def serve_for_bar(server_msg):
            method, kwargs = server_msg
            getattr(bar, method)(**kwargs)

        bar_events.serve_on_thread(serve_for_bar)

        return bar, kwargs

    @property
    def bar(self):
        return self.factory


@dataclass
class RemoteProgBarFactory:
    # this will be passed to remote side.
    bar_creation_queue: RayQueue

    def __call__(self, iterable=None, **kwargs) -> "ServerProgBar":
        # import tqdm
        bar_events = RayQueue()
        self.bar_creation_queue.put((bar_events, kwargs))
        return ServerProgBar(iterable, bar_events)

    # ahh we need someone accept the creation of progbar on client side, since
    # the creation requests are done on the server side.


@dataclass
class ServerProgBar:
    src_generator: Optional[Iterable]
    bar_events: RayQueue

    def __post_init__(self):
        self.last_check = datetime.now()
        self.interval = pd.Timedelta("0.1 seconds")

    def __iter__(self):
        assert self.src_generator is not None, "iterator is requested even though no generator is available."
        count = 0
        self.last_check = datetime.now()
        for item in self.src_generator:
            now = datetime.now()
            dt = now - self.last_check
            yield item
            count += 1
            if dt > self.interval:
                self.update(count)
                count = 0
                self.last_check = now
        self.update(count)
        self.end()

    def __enter__(self):
        pass

    def __exit__(self, *args):
        self.end()

    def update(self, n: int):
        self.bar_events.put(("update", dict(n=n)))

    def end(self):
        self.bar_events.put(("close", dict()))
        self.bar_events.end()
