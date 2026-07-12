import torch
import torch.nn as nn
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist

def prepare_ddp_model(model: nn.Module) -> nn.Module:
    """
    Wraps a model in DistributedDataParallel for gradient synchronization.
    The model should already be instantiated.
    
    Args:
        model (nn.Module): The local model instance.
        
    Returns:
        nn.Module: The DDP wrapped model.
    """
    if not dist.is_initialized():
        raise RuntimeError("Distributed process group is not initialized.")
        
    local_rank = int(torch.environ.get("LOCAL_RANK", 0)) if hasattr(torch, "environ") else int(torch.os.environ.get("LOCAL_RANK", 0))
    # Corrected os.environ import dependency if not imported:
    import os
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    
    device = torch.device(f"cuda:{local_rank}" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    # Wrap the model in DDP
    # If we're using CPU (e.g., Gloo backend without CUDA), DDP doesn't take device_ids
    if torch.cuda.is_available():
        ddp_model = DDP(model, device_ids=[local_rank])
    else:
        ddp_model = DDP(model)
        
    return ddp_model
