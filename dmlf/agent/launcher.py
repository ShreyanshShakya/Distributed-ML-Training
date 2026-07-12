import subprocess
import os

class JobLauncher:
    def __init__(self):
        self.current_process = None

    def launch_torchrun(self, job_id, nnodes, node_rank, master_addr, master_port, nproc_per_node, script_path, extra_args):
        if self.current_process and self.current_process.poll() is None:
            print("A job is already running on this node.")
            return False

        # Build torchrun command
        cmd = [
            "torchrun",
            f"--nnodes={nnodes}",
            f"--node_rank={node_rank}",
            f"--master_addr={master_addr}",
            f"--master_port={master_port}",
            f"--nproc_per_node={nproc_per_node}",
            script_path
        ]
        if extra_args:
            cmd.extend(extra_args.split())

        print(f"Executing: {' '.join(cmd)}")
        
        # Start in background
        env = os.environ.copy()
        # Add debugging for DDP
        env["NCCL_DEBUG"] = "INFO"
        self.current_process = subprocess.Popen(
            cmd,
            env=env
            # We remove stdout=PIPE so the process inherits the console
            # and prints directly to the terminal without blocking OS buffers!
        )
        return True

    def stop_current_job(self):
        if self.current_process and self.current_process.poll() is None:
            self.current_process.terminate()
            self.current_process = None
            return True
        return False
