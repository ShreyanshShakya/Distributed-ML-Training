import os
import sys

# Force USE_LIBUV to 0 before importing torch
os.environ["USE_LIBUV"] = "0"
os.environ["TORCH_CPP_LOG_LEVEL"] = "INFO"

from torch.distributed.run import main

if __name__ == '__main__':
    # We simulate passing arguments to torchrun
    sys.argv = [
        "torchrun",
        "--standalone",
        "--nnodes=1",
        "--nproc_per_node=2",
        "train.py",
        "--epochs=1",
        "--backend=gloo"
    ]
    main()
