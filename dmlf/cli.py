import sys
import yaml
import grpc
import argparse

from dmlf.communication import cml_pb2
from dmlf.communication import cml_pb2_grpc

def submit_job(config_path: str, manager_addr: str = "localhost:50051"):
    print(f"Submitting job from {config_path} to {manager_addr}...")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    cluster_cfg = config.get('cluster', {})
    training_cfg = config.get('training', {})
    
    req = cml_pb2.JobSubmitRequest(
        script_path=training_cfg.get('script_path', 'train.py'),
        nnodes=cluster_cfg.get('nodes', 1),
        nproc_per_node=training_cfg.get('nproc_per_node', 1),
        args=training_cfg.get('args', '')
    )
    
    try:
        channel = grpc.insecure_channel(manager_addr)
        stub = cml_pb2_grpc.ClusterManagerStub(channel)
        
        resp = stub.SubmitJob(req)
        
        if resp.success:
            print(f"Success! Job ID: {resp.job_id}")
            print(f"Message: {resp.message}")
        else:
            print(f"Failed to submit job: {resp.message}")
            
    except grpc.RpcError as e:
        print(f"Connection error: {e.details()}")

def main():
    parser = argparse.ArgumentParser(description="DMLF CLI")
    subparsers = parser.add_subparsers(dest="command")
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a job config")
    submit_parser.add_argument("config", type=str, help="Path to YAML config")
    submit_parser.add_argument("--manager", type=str, default="localhost:50051", help="Manager address")
    
    args = parser.parse_args()
    
    if args.command == "submit":
        submit_job(args.config, args.manager)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
