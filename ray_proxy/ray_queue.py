from dataclasses import dataclass, field
from threading import Thread

from ray.util.queue import Queue


@dataclass
class RayQueue:
    queue: Queue = field(default_factory=Queue)

    def _serve(self, f):
        while True:
            header, item = self.queue.get()
            if header == "error":
                raise RuntimeError(f"error:{item}")
            elif header == "end":
                break
            f(item)

    def serve_on_thread(self, f):
        t = Thread(target=self._serve, args=(f,))
        t.start()
        return t

    def put(self, item):
        self.queue.put((None, item))

    def put_error(self, item):
        self.queue.put(("error", item))

    def end(self):
        self.queue.put(("end", None))
