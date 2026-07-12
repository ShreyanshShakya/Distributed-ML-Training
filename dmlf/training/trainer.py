import os
import time
import torch
import torch.distributed as dist
from torch.utils.tensorboard import SummaryWriter
from typing import Optional, Callable

from dmlf.monitoring.system import log_system_metrics

class DDPTrainer:
    def __init__(
        self,
        model: torch.nn.Module,
        optimizer: torch.optim.Optimizer,
        train_loader,
        criterion: Callable,
        epochs: int,
        checkpoint_dir: str,
        rank: int,
        local_rank: int,
        checkpoint_interval_minutes: int = 5
    ):
        self.model = model
        self.optimizer = optimizer
        self.train_loader = train_loader
        self.criterion = criterion
        self.epochs = epochs
        self.checkpoint_dir = checkpoint_dir
        self.rank = rank
        self.local_rank = local_rank
        self.checkpoint_interval_seconds = checkpoint_interval_minutes * 60
        self.device = torch.device(f"cuda:{local_rank}" if torch.cuda.is_available() else "cpu")

        # Set up logging and checkpointing on Rank 0 only
        self.is_main_process = (self.rank == 0)
        self.writer = None
        if self.is_main_process:
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            self.writer = SummaryWriter(log_dir=os.path.join("logs", "tensorboard"))

        self.last_checkpoint_time = time.time()
        self.current_epoch = 0

    def load_checkpoint(self, path: str):
        """Loads a checkpoint if it exists."""
        if not os.path.exists(path):
            if self.is_main_process:
                print(f"Checkpoint {path} not found. Starting from scratch.")
            return

        map_location = {"cuda:0": f"cuda:{self.local_rank}"} if torch.cuda.is_available() else "cpu"
        checkpoint = torch.load(path, map_location=map_location)
        self.model.module.load_state_dict(checkpoint['model_state_dict']) # Unwrap DDP
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.current_epoch = checkpoint['epoch'] + 1
        if self.is_main_process:
            print(f"Resumed from epoch {self.current_epoch}")

    def save_checkpoint(self, epoch: int):
        """Saves a checkpoint on Rank 0."""
        if not self.is_main_process:
            return

        checkpoint = {
            'epoch': epoch,
            'model_state_dict': self.model.module.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
        }
        path = os.path.join(self.checkpoint_dir, f"checkpoint_epoch_{epoch}.pt")
        torch.save(checkpoint, path)
        print(f"Checkpoint saved to {path}")
        self.last_checkpoint_time = time.time()

    def train(self):
        for epoch in range(self.current_epoch, self.epochs):
            # Ensure different shuffling per epoch in distributed training
            if hasattr(self.train_loader.sampler, "set_epoch"):
                self.train_loader.sampler.set_epoch(epoch)

            self.model.train()
            running_loss = 0.0
            epoch_start_time = time.time()

            global_step = epoch * len(self.train_loader)
            for batch_idx, (inputs, targets) in enumerate(self.train_loader):
                batch_start_time = time.time()
                
                inputs, targets = inputs.to(self.device), targets.to(self.device)

                self.optimizer.zero_grad()
                outputs = self.model(inputs)
                loss = self.criterion(outputs, targets)
                loss.backward()
                self.optimizer.step()

                running_loss += loss.item()
                global_step += 1
                
                # Calculate throughput
                batch_time = time.time() - batch_start_time
                batch_size = inputs.size(0)
                world_size = dist.get_world_size() if dist.is_initialized() else 1
                samples_per_sec = (batch_size * world_size) / max(batch_time, 1e-6)

                if self.is_main_process and batch_idx % 10 == 0:
                    if self.writer:
                        self.writer.add_scalar("Throughput/Samples_per_sec", samples_per_sec, global_step)
                        # Log system metrics every 50 batches
                        if batch_idx % 50 == 0:
                            log_system_metrics(self.writer, global_step, rank=self.rank)

            epoch_duration = time.time() - epoch_start_time
            avg_loss = running_loss / len(self.train_loader)

            if self.is_main_process:
                print(f"Epoch [{epoch}/{self.epochs}] - Loss: {avg_loss:.4f} - Time: {epoch_duration:.2f}s - Throughput: {samples_per_sec:.2f} samples/s")
                if self.writer:
                    self.writer.add_scalar("Loss/train", avg_loss, epoch)
                    self.writer.add_scalar("Time/epoch", epoch_duration, epoch)

            # Checkpoint by time or end of epoch
            if time.time() - self.last_checkpoint_time >= self.checkpoint_interval_seconds:
                self.save_checkpoint(epoch)

        # Save final checkpoint at the end of training
        self.save_checkpoint(self.epochs - 1)

        if self.writer:
            self.writer.close()
