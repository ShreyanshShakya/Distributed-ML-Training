from typing import List, Dict, Any
from dmlf.manager.schedulers.base import BaseScheduler
from dmlf.manager.allocator import NodeAllocator
from dmlf.manager.node_registry import NodeRegistry, NodeState
from dmlf.manager.queue import JobQueue

class PriorityBinPackScheduler(BaseScheduler):
    """
    Default scheduler that mimics the original behaviour:
    - Pops highest‑priority job
    - Uses the NodeAllocator (GPU‑weighted scoring) to pick the best N idle nodes
    """

    def _select_nodes(self, job: Dict[str, Any]) -> List[Dict[str, Any]]:
        available = self.registry.get_available_nodes()
        return self.allocator.allocate(available, job["required_nodes"])