import os
import argparse
import random
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms

from dmlf.distributed.init import setup_distributed, cleanup_distributed
from dmlf.data.distributed_loader import create_distributed_dataloader
from dmlf.training.ddp_model import prepare_ddp_model
from dmlf.training.trainer import DDPTrainer

# Define a simple CNN for MNIST to satisfy the MVP model size constraint
class SimpleCNN(nn.Module):
    def __init__(self):
        super(SimpleCNN, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = torch.relu(x)
        x = self.conv2(x)
        x = torch.relu(x)
        x = torch.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = torch.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        return torch.log_softmax(x, dim=1)

def set_seed(seed=42):
    """Ensure reproducible experiments."""
    torch.manual_seed(seed)
    np.random.seed(seed)
    random.seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

def main():
    parser = argparse.ArgumentParser(description='DMLF MVP Training Script')
    parser.add_argument('--epochs', type=int, default=10, help='number of epochs to train')
    parser.add_argument('--batch-size', type=int, default=64, help='input batch size for training (per worker)')
    parser.add_argument('--lr', type=float, default=0.01, help='learning rate')
    parser.add_argument('--backend', type=str, default='nccl', choices=['nccl', 'gloo'], help='distributed backend')
    parser.add_argument('--resume', type=str, default='', help='path to checkpoint to resume from')
    args = parser.parse_args()

    # Phase 8.1: Environment Synchronization
    set_seed(42)

    # Setup distributed environment
    dist_info = setup_distributed(backend=args.backend)
    rank = dist_info['rank']
    local_rank = dist_info['local_rank']

    # Phase 7.2: Dataset Distribution (Identical local copies)
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    
    # Download on rank 0 first to avoid race conditions, then load on all
    if rank == 0:
        datasets.MNIST('data', train=True, download=True, transform=transform)
    torch.distributed.barrier() # wait for rank 0 to download
    
    dataset = datasets.MNIST('data', train=True, download=False, transform=transform)

    # Create distributed dataloader
    train_loader, sampler = create_distributed_dataloader(
        dataset=dataset,
        batch_size=args.batch_size,
        is_training=True,
        num_workers=2
    )

    # Phase 7.4: Model Synchronization
    model = SimpleCNN()
    ddp_model = prepare_ddp_model(model)

    optimizer = optim.SGD(ddp_model.parameters(), lr=args.lr, momentum=0.9)
    criterion = nn.NLLLoss()

    # Initialize Trainer
    trainer = DDPTrainer(
        model=ddp_model,
        optimizer=optimizer,
        train_loader=train_loader,
        criterion=criterion,
        epochs=args.epochs,
        checkpoint_dir='dmlf/checkpoint',
        rank=rank,
        local_rank=local_rank,
        checkpoint_interval_minutes=5
    )

    if args.resume:
        trainer.load_checkpoint(args.resume)

    # Start training
    if rank == 0:
        print("Starting training...")
    trainer.train()

    cleanup_distributed()
    if rank == 0:
        print("Training completed.")

if __name__ == '__main__':
    main()
