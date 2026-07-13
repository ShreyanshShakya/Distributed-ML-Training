import subprocess
import os
import threading
import time
from dmlf.communication import cml_pb2

class JobLauncher:
    def __init__(self):
        self.current_process = None
        self.log_thread = None

    def launch_torchrun(self, job_id, nnodes, node_rank, master_addr, master_port, nproc_per_node, script_path, extra_args, node_id, stub):
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
        
        env = os.environ.copy()
        self.current_process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        def log_generator():
            for line in iter(self.current_process.stdout.readline, ''):
                if line:
                    yield cml_pb2.LogMessage(
                        timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                        node_id=node_id,
                        job_id=job_id,
                        level="INFO",
                        message=line.strip()
                    )
            
            # Send status update when process finishes
            exit_code = self.current_process.wait()
            status = "completed" if exit_code == 0 else "failed"
            try:
                stub.ReportJobStatus(cml_pb2.JobStatusRequest(
                    node_id=node_id,
                    job_id=job_id,
                    status=status
                ))
            except Exception as e:
                print(f"Failed to report job status: {e}")

        def stream_logs():
            try:
                stub.StreamLogs(log_generator())
            except Exception as e:
                print(f"Log streaming disconnected: {e}")
                
        self.log_thread = threading.Thread(target=stream_logs, daemon=True)
        self.log_thread.start()
        
        return True

    def stop_current_job(self):
        if self.current_process and self.current_process.poll() is None:
            self.current_process.terminate()
            if self.log_thread:
                self.log_thread.join(timeout=2.0)
            self.current_process = None
            return True
        return False
