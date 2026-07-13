import threading
import time
from typing import Callable, Any, Dict, List

from dmlf.manager.queue import JobQueue
from dmlf.manager.allocator import NodeAllocator
from dmlf.manager.node_registry import NodeRegistry, NodeState

class Scheduler:
    def __init__(self, registry: NodeRegistry, job_queue: JobQueue, allocator: NodeAllocator, on_job_allocated: Callable):
        self.registry = registry
        self.queue = job_queue
        self.allocator = allocator
        self.on_job_allocated = on_job_allocated
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _scheduler_loop(self):
        while self.running:
            time.sleep(2)  # Polling interval
            job = self.queue.pop_top_job()
            if not job:
                continue

            available_nodes = self.registry.get_available_nodes()
            allocated = self.allocator.allocate(available_nodes, job["required_nodes"])

            if not allocated:
                # Not enough nodes available right now. Put it back at the front of the queue.
                self.queue.requeue_job(job)
            else:
                # Reserve nodes so they aren't grabbed by another loop iteration
                for node in allocated:
                    self.registry.update_node_state(node["node_id"], NodeState.RESERVED)
                
                # Dispatch the job via callback
                self.on_job_allocated(job, allocated)
