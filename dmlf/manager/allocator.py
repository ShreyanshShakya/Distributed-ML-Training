import re
from typing import List, Dict, Any

class NodeAllocator:
    def __init__(self, gpu_weight=1.0, cpu_weight=0.5, ram_weight=0.1):
        self.gpu_weight = gpu_weight
        self.cpu_weight = cpu_weight
        self.ram_weight = ram_weight

    def _parse_memory(self, mem_str: str) -> float:
        """Parse '16GB' or '16384MB' to MB float."""
        if not mem_str: return 0.0
        match = re.search(r"(\d+(\.\d+)?)\s*(GB|MB)", mem_str.upper())
        if not match: return 0.0
        val = float(match.group(1))
        unit = match.group(3)
        return val * 1024 if unit == "GB" else val

    def score_node(self, node: Dict[str, Any]) -> float:
        # GPU Score (Simulated: if has GPU model string, give it high score)
        gpu_model = node.get("gpu_model", "")
        has_gpu = 1 if gpu_model and gpu_model != "None" else 0
        gpu_score = has_gpu * 1000 * self.gpu_weight

        # CPU Score
        cpu_score = node.get("cpu_count", 1) * self.cpu_weight

        # RAM Score
        ram_mb = self._parse_memory(node.get("ram_total", "0MB"))
        ram_score = (ram_mb / 1024) * self.ram_weight

        # Total Score
        score = gpu_score + cpu_score + ram_score
        return score

    def allocate(self, available_nodes: List[Dict[str, Any]], required_nodes: int) -> List[Dict[str, Any]]:
        """Returns the best nodes based on scoring formula. Empty list if not enough nodes."""
        if len(available_nodes) < required_nodes:
            return []

        # Sort nodes by score descending
        sorted_nodes = sorted(available_nodes, key=self.score_node, reverse=True)
        return sorted_nodes[:required_nodes]
