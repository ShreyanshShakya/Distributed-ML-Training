import time
import uuid
import grpc
import queue
import threading
from concurrent import futures
from typing import Dict, Any

from dmlf.communication import cml_pb2
from dmlf.communication import cml_pb2_grpc
from dmlf.manager.node_registry import NodeRegistry

class ClusterManagerServicer(cml_pb2_grpc.ClusterManagerServicer):
    def __init__(self, registry: NodeRegistry):
        self.registry = registry
        # A dictionary mapping node_id -> Queue to hold pending commands
        self.command_queues: Dict[str, queue.Queue] = {}
        self.lock = threading.Lock()

    def RegisterNode(self, request, context):
        node_id = f"node-{uuid.uuid4().hex[:8]}"
        success = self.registry.register_node(
            node_id=node_id,
            hostname=request.hostname,
            ip_address=request.ip_address,
            cpu_count=request.cpu_count,
            gpu_model=request.gpu_model,
            ram_total=request.ram_total
        )
        
        with self.lock:
            self.command_queues[node_id] = queue.Queue()

        print(f"[{time.strftime('%H:%M:%S')}] Node Registered: {node_id} ({request.hostname} @ {request.ip_address})")
        return cml_pb2.RegistrationResponse(
            success=success,
            node_id=node_id,
            message="Successfully registered with Cluster Manager."
        )

    def SendHeartbeat(self, request, context):
        metrics = {
            "cpu_percent": request.cpu_percent,
            "ram_percent": request.ram_percent,
            "gpu_utilization": request.gpu_utilization,
            "gpu_memory_mb": request.gpu_memory_mb
        }
        self.registry.update_heartbeat(request.node_id, request.current_status, metrics)
        return cml_pb2.HeartbeatResponse(acknowledged=True)

    def ListenForCommands(self, request, context):
        node_id = request.node_id
        
        with self.lock:
            if node_id not in self.command_queues:
                self.command_queues[node_id] = queue.Queue()
        
        q = self.command_queues[node_id]
        
        # Keep the stream open and yield commands as they arrive
        try:
            while context.is_active():
                try:
                    # Block for 2 seconds to check for commands, then loop to check context
                    command = q.get(timeout=2.0)
                    yield command
                except queue.Empty:
                    continue
        except grpc.RpcError:
            print(f"Node {node_id} disconnected from command stream.")
        
        return

    def ReportJobStatus(self, request, context):
        print(f"[{time.strftime('%H:%M:%S')}] Job Status from {request.node_id}: {request.job_id} is {request.status}")
        if request.error_message:
            print(f"Error: {request.error_message}")
        return cml_pb2.JobStatusResponse(acknowledged=True)
        
    def SubmitJob(self, request, context):
        print(f"[{time.strftime('%H:%M:%S')}] Received Job Submission:")
        print(f"  Script: {request.script_path}")
        print(f"  Nodes required: {request.nnodes}")
        
        # Super simple scheduling logic for now: just grab the first N available nodes
        available_nodes = self.registry.get_available_nodes()
        if len(available_nodes) < request.nnodes:
            return cml_pb2.JobSubmitResponse(
                success=False,
                job_id="",
                message=f"Not enough available nodes. Requested {request.nnodes}, but only {len(available_nodes)} are idle."
            )
            
        selected_nodes = available_nodes[:request.nnodes]
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        
        # Node 0 in our selected list will be the Master
        master_node = selected_nodes[0]
        master_addr = master_node["ip_address"]
        master_port = 29500 # hardcoded for MVP
        
        print(f"  Selected Master Node: {master_addr}")
        
        # Send LAUNCH_JOB commands
        for rank, node in enumerate(selected_nodes):
            cmd = cml_pb2.Command(
                command_id=f"cmd-{uuid.uuid4().hex[:8]}",
                type=cml_pb2.Command.LAUNCH_JOB,
                job_payload=cml_pb2.JobPayload(
                    job_id=job_id,
                    nnodes=request.nnodes,
                    node_rank=rank,
                    master_addr=master_addr,
                    master_port=master_port,
                    nproc_per_node=request.nproc_per_node,
                    script_path=request.script_path,
                    args=request.args
                )
            )
            self.send_command(node["node_id"], cmd)
            
        return cml_pb2.JobSubmitResponse(
            success=True,
            job_id=job_id,
            message=f"Job successfully scheduled on {request.nnodes} nodes."
        )

    def send_command(self, node_id: str, command: cml_pb2.Command):
        with self.lock:
            if node_id in self.command_queues:
                self.command_queues[node_id].put(command)
                print(f"Queued command for {node_id}")
            else:
                print(f"Cannot send command: Node {node_id} not registered.")

def serve(port=50051):
    registry = NodeRegistry()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ClusterManagerServicer(registry)
    cml_pb2_grpc.add_ClusterManagerServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Cluster Manager started on port {port}")
    
    # Optional: Background thread to monitor offline nodes
    def monitor_nodes():
        while True:
            registry.mark_offline_nodes()
            time.sleep(10)
            
    threading.Thread(target=monitor_nodes, daemon=True).start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down Cluster Manager...")

if __name__ == '__main__':
    serve()
