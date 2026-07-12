import os
import sys
import torch
import torch.multiprocessing as mp

def run_worker(rank, world_size):
    # Set environment variables for this worker
    os.environ['RANK'] = str(rank)
    os.environ['LOCAL_RANK'] = str(rank)
    os.environ['WORLD_SIZE'] = str(world_size)
    os.environ['MASTER_ADDR'] = 'localhost'
    os.environ['MASTER_PORT'] = '29500'
    os.environ['USE_LIBUV'] = '0'
    
    print(f"Starting worker with rank {rank}")
    
    # Import and run train.py's main
    import train
    
    # We simulate command line arguments
    sys.argv = [
        "train.py",
        "--epochs=1",
        "--backend=gloo",
        "--batch-size=32"
    ]
    
    try:
        train.main()
    except Exception as e:
        print(f"Error in rank {rank}: {e}")
        raise e

if __name__ == '__main__':
    world_size = 2
    print(f"Spawning {world_size} processes for local testing...")
    mp.spawn(run_worker, args=(world_size,), nprocs=world_size, join=True)
