import importlib
import time
import threading
from typing import Callable, Dict, Any, List

from dmlf.manager.node_registry import NodeRegistry
from dmlf.manager.queue import JobQueue
from dmlf.settings import get_settings
from dmlf.monitoring.prometheus import (
    JOBS_QUEUED, NODE_CPU, NODE_GPU_UTIL,
)


class Autoscaler:
    """
    Periodically evaluates a very small rule‑set and calls a user‑supplied
    `scale_callback(direction)` where direction == "out" or "in".
    """

    def __init__(
        self,
        registry: NodeRegistry,
        job_queue: JobQueue,
        callback: Callable[[str], None] | None = None,
    ):
        self.registry = registry
        self.job_queue = job_queue
        self.callback = callback or self._default_callback
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_scale_in = 0.0

        s = get_settings().autoscaler
        self.interval = s.interval_sec
        self.queue_high = s.queue_depth_high
        self.cpu_high = s.avg_cpu_high
        self.gpu_high = s.avg_gpu_high
        self.queue_low = s.queue_depth_low
        self.cpu_low = s.avg_cpu_low
        self.gpu_low = s.avg_gpu_low
        self.cooldown = s.scale_in_cooldown_sec

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self.interval)
            if self._stop.is_set():
                break
            try:
                self._evaluate()
            except Exception as exc:               # pragma: no cover
                print(f"[Autoscaler] evaluation error: {exc}")

    # ------------------------------------------------------------------
    # Policy evaluation
    # ------------------------------------------------------------------
    def _evaluate(self) -> None:
        # ---- 1️⃣  Gather current snapshot --------------------------------
        pending = JOBS_QUEUED._value.get()               # type: ignore[attr-defined]
        idle_nodes = self.registry.get_available_nodes()  # only IDLE nodes
        all_nodes = self.registry.get_all_nodes()

        def gauge_avg(gauge, nodes: List[Dict[str, Any]]) -> float:
            if not nodes:
                return 0.0
            total = 0.0
            cnt = 0
            for n in nodes:
                val = gauge.labels(node_id=n["node_id"])._value.get()   # type: ignore[attr-defined]
                if val is not None:
                    total += val
                    cnt += 1
            return total / cnt if cnt else 0.0

        avg_cpu_idle = gauge_avg(NODE_CPU, idle_nodes)
        avg_gpu_idle = gauge_avg(NODE_GPU_UTIL, idle_nodes)
        avg_cpu_all  = gauge_avg(NODE_CPU, all_nodes)
        avg_gpu_all  = gauge_avg(NODE_GPU_UTIL, all_nodes)

        # ---- 2️⃣  Scale‑out decision ------------------------------------
        scale_out = (
            pending > self.queue_high
            or avg_cpu_idle > self.cpu_high
            or avg_gpu_idle > self.gpu_high
        )

        # ---- 3️⃣  Scale‑in decision -------------------------------------
        now = time.time()
        scale_in = (
            pending <= self.queue_low
            and avg_cpu_all < self.cpu_low
            and avg_gpu_all < self.gpu_low
            and (now - self._last_scale_in) > self.cooldown
        )

        # ---- 4️⃣  Act ----------------------------------------------------
        if scale_out:
            print("[Autoscaler] 📈 Scale‑OUT requested")
            self.callback("out")
        elif scale_in:
            print("[Autoscaler] 📉 Scale‑IN requested")
            self.callback("in")
            self._last_scale_in = now

    # ------------------------------------------------------------------
    # Default callback – replace via config (import path)
    # ------------------------------------------------------------------
    @staticmethod
    def _default_callback(direction: str) -> None:
        """
        The default implementation only logs the decision.
        In a real deployment you would start/stop a container,
        call a cloud API, edit a K8s Deployment replica count, …
        """
        print(f"[Autoscaler] default callback → {direction}")

    @classmethod
    def load_callback(cls, dotted_path: str) -> Callable[[str], None]:
        """Import a `callable(direction: str)` from a ``module:function`` string."""
        mod_name, func_name = dotted_path.rsplit(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, func_name)