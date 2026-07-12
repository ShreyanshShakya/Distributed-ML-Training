import psutil
import subprocess

def get_cpu_usage() -> float:
    return psutil.cpu_percent(interval=1)

def get_memory_usage() -> dict:
    mem = psutil.virtual_memory()
    return {
        "total_gb": mem.total / (1024 ** 3),
        "used_gb": mem.used / (1024 ** 3),
        "percent": mem.percent
    }

def get_network_io() -> dict:
    io = psutil.net_io_counters()
    return {
        "bytes_sent": io.bytes_sent,
        "bytes_recv": io.bytes_recv
    }

def get_gpu_usage() -> list:
    """
    Returns GPU usage info using nvidia-smi if available.
    """
    try:
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=index,utilization.gpu,memory.used,memory.total', '--format=csv,noheader,nounits'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode != 0:
            return []
            
        gpus = []
        for line in result.stdout.strip().split('\n'):
            if line:
                idx, util, mem_used, mem_total = map(str.strip, line.split(','))
                gpus.append({
                    "index": int(idx),
                    "utilization_percent": float(util),
                    "memory_used_mb": float(mem_used),
                    "memory_total_mb": float(mem_total)
                })
        return gpus
    except (FileNotFoundError, Exception):
        return []

def log_system_metrics(writer, step: int, rank: int = 0):
    """
    Logs system metrics to tensorboard if a writer is provided.
    """
    if not writer:
        return
        
    writer.add_scalar(f"System_Rank{rank}/CPU_Percent", get_cpu_usage(), step)
    
    mem = get_memory_usage()
    writer.add_scalar(f"System_Rank{rank}/Memory_Percent", mem["percent"], step)
    
    # GPU
    gpus = get_gpu_usage()
    for gpu in gpus:
        idx = gpu["index"]
        writer.add_scalar(f"System_Rank{rank}/GPU{idx}_Utilization", gpu["utilization_percent"], step)
        writer.add_scalar(f"System_Rank{rank}/GPU{idx}_Memory_Used_MB", gpu["memory_used_mb"], step)
