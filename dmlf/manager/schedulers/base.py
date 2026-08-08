import abc
import time
import threading
from typing import Callable, List, Dict, Any

from dmlf.manager.queue import JobQueue
from dmlf.manager.node_registry import NodeRegistry, NodeState
from dmlf.manager.allocator import NodeAllocator

class BaseScheduler(abc.ABC):
    """
    Abstract base class for scheduler plugins.

    A scheduler is responsible for repeatedly pulling jobs from the queue,
    selecting appropriate nodes via the allocator, and invoking the
    `on_job_allocated` callback with the job and the chosen nodes.
    """

    def __init__(
        self,
        registry: NodeRegistry,
        job_queue: JobQueue,
        allocator: NodeAllocator,
        on_job_allocated: Callable[[Dict[str, Any], List[Dict[str, Any]]], None],
        interval_sec: int = 2,
    ):
        self.registry = registry
        self.queue = job_queue
        self.allocator = allocator
        self.on_job_allocated = on_job_allocated
        self.interval_sec = interval_sec
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @abc.abstractmethod
    def _select_nodes(self, job: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return a list of node dicts to run the given job, or empty list if not enough resources."""
        ...

    def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self.interval_sec)
            if self._stop.is_set():
                break
            job = self.queue.pop_top_job()
            if not job:
                continue
            nodes = self._select_nodes(job)
            if not nodes:
                # not enough nodes – requeue at front
                self.queue.requeue_job(job)
            else:
                for node in nodes:
                    self.registry.update_node_state(node["node_id"], NodeState.RESERVED)
                self.on_job_allocated(job, nodes)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)