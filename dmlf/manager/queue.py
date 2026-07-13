import threading
from typing import List, Dict, Any, Optional

from dmlf.manager.node_registry import NodeRegistry, JobState

class JobQueue:
    def __init__(self, registry: NodeRegistry):
        self.registry = registry
        self.queue: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        
        # Load any pending jobs from DB on startup
        self._recover_pending_jobs()

    def _recover_pending_jobs(self):
        pending = self.registry.get_jobs(status=JobState.PENDING)
        # Sort by priority (higher is better) then created_at (older is better)
        pending.sort(key=lambda j: (j.get("priority", 0), -j.get("created_at", 0)), reverse=True)
        with self.lock:
            self.queue = pending

    def submit_job(self, job_data: Dict[str, Any]):
        """Persists to DB and adds to memory queue."""
        job_data["status"] = JobState.PENDING
        self.registry.insert_job(job_data)
        
        with self.lock:
            self.queue.append(job_data)
            # Re-sort queue to maintain priority order
            self.queue.sort(key=lambda j: (j.get("priority", 0), -j.get("created_at", 0)), reverse=True)

    def peek_top_job(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.queue:
                return None
            return self.queue[0]

    def pop_top_job(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.queue:
                return None
            return self.queue.pop(0)

    def requeue_job(self, job_data: Dict[str, Any]):
        """Puts a job back if it couldn't be scheduled."""
        with self.lock:
            self.queue.insert(0, job_data)

    def retry_job(self, job_data: Dict[str, Any]):
        """Increments retries and puts the job back in the queue."""
        retries = job_data.get("retries", 0) + 1
        job_data["retries"] = retries
        job_data["status"] = JobState.PENDING
        
        # Persist the change to DB
        self.registry.update_job_retries(job_data["job_id"], JobState.PENDING, retries)
        
        with self.lock:
            # We append it so it goes to the back of the queue (or we can use insert(0)). Let's append and re-sort.
            self.queue.append(job_data)
            self.queue.sort(key=lambda j: (j.get("priority", 0), -j.get("created_at", 0)), reverse=True)
