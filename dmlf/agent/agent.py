import time
import grpc
import threading
import sys
import argparse

from dmlf.communication import cml_pb2
from dmlf.communication import cml_pb2_grpc
from dmlf.agent import hardware
from dmlf.agent.launcher import JobLauncher
from dmlf.monitoring import system
from dmlf.settings import load_settings, get_settings
from dmlf.communication.auth import create_client_interceptors

class NodeAgent:
    def __init__(self, manager_addr: str = None, config_path: str = "config.yaml"):
        self.settings = load_settings(config_path)
        mgr_settings = self.settings.manager
        if manager_addr is None:
            manager_addr = f"localhost:{mgr_settings.grpc_port}"
        self.manager_addr = manager_addr
        self.node_id = None
        self.node_secret = None
        self.status = "IDLE"
        self.launcher = JobLauncher()
        # Interceptors - created with settings
        self.token_interceptor, self.node_secret_interceptor = create_client_interceptors(mgr_settings.auth_token)
        base_channel = grpc.insecure_channel(self.manager_addr)
        self.channel = grpc.intercept_channel(base_channel, self.token_interceptor, self.node_secret_interceptor)
        self.stub = cml_pb2_grpc.ClusterManagerStub(self.channel)
        # heartbeat interval from settings
        self.heartbeat_interval = mgr_settings.heartbeat_interval_sec
        agent_settings = self.settings.agent
        self.reconnect_base = agent_settings.reconnect_base_sec
        self.reconnect_max = agent_settings.reconnect_max_sec
        
    def register(self):
        print(f"Registering with Cluster Manager at {self.manager_addr}...")
        req = cml_pb2.RegistrationRequest(
            hostname=hardware.get_hostname(),
            ip_address=hardware.get_ip_address(),
            cpu_count=hardware.get_cpu_count(),
            gpu_model=hardware.get_gpu_model(),
            ram_total=hardware.get_ram_total()
        )
        try:
            resp = self.stub.RegisterNode(req)
            if resp.success:
                self.node_id = resp.node_id
                self.node_secret = resp.node_secret
                # Provide credentials to node-secret interceptor for subsequent calls
                self.node_secret_interceptor.set_credentials(self.node_id, self.node_secret)
                print(f"Registration successful! Node ID: {self.node_id}")
                return True
            else:
                print(f"Registration failed: {resp.message}")
                return False
        except grpc.RpcError as e:
            print(f"Failed to connect to Manager: {e.details()}")
            return False

    def _heartbeat_loop(self):
        while True:
            time.sleep(self.heartbeat_interval)
            if not self.node_id:
                continue
                
            try:
                # Try to get GPU metrics if available, otherwise 0
                gpus = system.get_gpu_usage()
                gpu_util = gpus[0]["utilization_percent"] if gpus else 0.0
                gpu_mem = gpus[0]["memory_used_mb"] if gpus else 0.0
                
                req = cml_pb2.HeartbeatRequest(
                    node_id=self.node_id,
                    cpu_percent=system.get_cpu_usage(),
                    ram_percent=system.get_memory_usage()["percent"],
                    gpu_utilization=gpu_util,
                    gpu_memory_mb=gpu_mem,
                    current_status=self.status,
                    send_timestamp=time.time()
                )
                self.stub.SendHeartbeat(req)
            except grpc.RpcError:
                print("Heartbeat failed. Manager might be down.")

    def listen_for_commands(self):
        if not self.node_id:
            print("Cannot listen for commands: Node not registered.")
            return

        print("Listening for commands from Manager...")
        req = cml_pb2.CommandListenRequest(node_id=self.node_id)
        
        try:
            # This returns a stream iterator
            for command in self.stub.ListenForCommands(req):
                print(f"\n[Command Received]: {command.type} (ID: {command.command_id})")
                
                if command.type == cml_pb2.Command.LAUNCH_JOB:
                    self.status = "TRAINING"
                    print("Launching Job...")
                    payload = command.job_payload
                    self.launcher.launch_torchrun(
                        job_id=payload.job_id,
                        nnodes=payload.nnodes,
                        node_rank=payload.node_rank,
                        master_addr=payload.master_addr,
                        master_port=payload.master_port,
                        nproc_per_node=payload.nproc_per_node,
                        script_path=payload.script_path,
                        extra_args=payload.args,
                        node_id=self.node_id,
                        stub=self.stub
                    )
                    
                elif command.type == cml_pb2.Command.STOP_JOB:
                    self.status = "IDLE"
                    print("Stopping Job...")
                    self.launcher.stop_current_job()
                    
        except grpc.RpcError as e:
            print(f"Command stream disconnected: {e.details()}")
            self.status = "OFFLINE"

    def start(self):
        if self.register():
            # Start heartbeat thread
            threading.Thread(target=self._heartbeat_loop, daemon=True).start()
            # Main thread blocks listening for commands
            backoff = self.reconnect_base
            while True:
                self.listen_for_commands()
                # If stream disconnects, wait and try to reconnect with exponential backoff
                print(f"Reconnecting in {backoff} seconds...")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.reconnect_max)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="DMLF Node Agent")
    parser.add_argument("--manager", type=str, default=None, help="Address of the Cluster Manager (host:port)")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()

    agent = NodeAgent(manager_addr=args.manager, config_path=args.config)
    try:
        agent.start()
    except KeyboardInterrupt:
        print("Agent shutting down.")
        sys.exit(0)
