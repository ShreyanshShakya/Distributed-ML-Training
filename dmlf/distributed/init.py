import os
from datetime import timedelta
import torch
import torch.distributed as dist

def setup_distributed(backend="nccl", timeout_minutes=30):
    """
    Initializes the PyTorch distributed process group.
    Relies on environment variables set by torchrun:
    - RANK
    - LOCAL_RANK
    - WORLD_SIZE
    - MASTER_ADDR
    - MASTER_PORT
    
    Args:
        backend (str): The communication backend ('nccl' for GPU, 'gloo' for CPU/Windows).
        timeout_minutes (int): Timeout in minutes for collective operations.
    """
    # Verify environment variables exist
    required_env_vars = ["RANK", "LOCAL_RANK", "WORLD_SIZE", "MASTER_ADDR", "MASTER_PORT"]
    for var in required_env_vars:
        if var not in os.environ:
            raise RuntimeError(f"Missing environment variable {var}. Did you use torchrun?")
    
    rank = int(os.environ["RANK"])
    local_rank = int(os.environ["LOCAL_RANK"])
    world_size = int(os.environ["WORLD_SIZE"])

    # Fallback to gloo if CUDA is not available and backend is nccl
    if backend == "nccl" and not torch.cuda.is_available():
        print(f"[Rank {rank}] CUDA not available, falling back to Gloo backend.")
        backend = "gloo"
        
    print(f"[Rank {rank}] Initializing process group with backend '{backend}'...")
    dist.init_process_group(
        backend=backend,
        timeout=timedelta(minutes=timeout_minutes)
    )
    
    # Set the device for the current process
    if torch.cuda.is_available():
        torch.cuda.set_device(local_rank)
        
    print(f"[Rank {rank}] Process group initialized. World size: {world_size}")
    
    return {
        "rank": rank,
        "local_rank": local_rank,
        "world_size": world_size
    }

def cleanup_distributed():
    """
    Destroys the distributed process group.
    """
    if dist.is_initialized():
        dist.destroy_process_group()
