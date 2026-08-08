"""Prometheus metric definitions + tiny helpers used by manager & agents."""
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ----------------------------------------------------------------------
# Cluster‑wide (manager) metrics
# ----------------------------------------------------------------------
JOBS_QUEUED = Gauge("dmlf_jobs_queued", "Number of jobs waiting in the queue")
JOBS_RUNNING = Gauge("dmlf_jobs_running", "Number of jobs currently executing")
JOB_LATENCY = Histogram(
    "dmlf_job_latency_seconds",
    "End‑to‑end latency from submit to completion",
    buckets=(5, 15, 30, 60, 120, 300, 600, 1800, 3600),
)
JOB_RETRIES = Counter("dmlf_job_retries_total", "Total number of job retries")
NODES_IDLE = Gauge("dmlf_nodes_idle", "Idle nodes ready for work")
NODES_BUSY = Gauge("dmlf_nodes_busy", "Nodes currently running a job")
NODES_OFFLINE = Gauge("dmlf_nodes_offline", "Nodes that have missed heartbeats")

# ----------------------------------------------------------------------
# Per‑node metrics (exposed by each agent)
# ----------------------------------------------------------------------
NODE_CPU = Gauge("dmlf_node_cpu_percent", "CPU utilisation %", ["node_id"])
NODE_RAM = Gauge("dmlf_node_ram_percent", "RAM utilisation %", ["node_id"])
NODE_GPU_UTIL = Gauge("dmlf_node_gpu_util_percent", "GPU utilisation %", ["node_id"])
NODE_GPU_MEM = Gauge("dmlf_node_gpu_memory_mb", "GPU memory used (MiB)", ["node_id"])

# ----------------------------------------------------------------------
# gRPC latency (both sides)
# ----------------------------------------------------------------------
GRPC_LATENCY = Histogram(
    "dmlf_grpc_latency_seconds",
    "gRPC call latency",
    ["method", "side"],      # side = "client" | "server"
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)

# ----------------------------------------------------------------------
# Helper functions (called from manager / agent)
# ----------------------------------------------------------------------
def inc_jobs_queued(delta: int = 1):
    JOBS_QUEUED.inc(delta)

def dec_jobs_queued(delta: int = 1):
    JOBS_QUEUED.dec(delta)

def inc_jobs_running(delta: int = 1):
    JOBS_RUNNING.inc(delta)

def dec_jobs_running(delta: int = 1):
    JOBS_RUNNING.dec(delta)

def observe_job_latency(seconds: float):
    JOB_LATENCY.observe(seconds)

def inc_job_retries():
    JOB_RETRIES.inc()

def set_nodes_idle(n: int):
    NODES_IDLE.set(n)

def set_nodes_busy(n: int):
    NODES_BUSY.set(n)

def set_nodes_offline(n: int):
    NODES_OFFLINE.set(n)

def set_node_cpu(node_id: str, value: float):
    NODE_CPU.labels(node_id=node_id).set(value)

def set_node_ram(node_id: str, value: float):
    NODE_RAM.labels(node_id=node_id).set(value)

def set_node_gpu(node_id: str, util: float, mem_mb: float):
    NODE_GPU_UTIL.labels(node_id=node_id).set(util)
    NODE_GPU_MEM.labels(node_id=node_id).set(mem_mb)

def observe_grpc_latency(method: str, side: str, seconds: float):
    GRPC_LATENCY.labels(method=method, side=side).observe(seconds)

# ----------------------------------------------------------------------
# aiohttp response helper
# ----------------------------------------------------------------------
async def metrics_handler(request):
    """aiohttp handler that returns the Prometheus text format."""
    from aiohttp import web
    body = generate_latest()
    return web.Response(body=body, content_type=CONTENT_TYPE_LATEST.split(';')[0])