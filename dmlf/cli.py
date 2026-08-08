import sys
import yaml
import grpc
import argparse

from dmlf.communication import cml_pb2
from dmlf.communication import cml_pb2_grpc
from dmlf.settings import load_settings
from dmlf.communication.auth import TokenClientInterceptor

def submit_job(job_config_path: str, manager_addr: str = None, config_path: str = "config.yaml"):
    settings = load_settings(config_path)
    if manager_addr is None:
        manager_addr = f"localhost:{settings.manager.grpc_port}"
    print(f"Submitting job from {job_config_path} to {manager_addr}...")
    
    with open(job_config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    cluster_cfg = config.get('cluster', {})
    training_cfg = config.get('training', {})
    
    # Include new optional fields if present
    req = cml_pb2.JobSubmitRequest(
        script_path=training_cfg.get('script_path', 'train.py'),
        nnodes=cluster_cfg.get('nodes', 1),
        nproc_per_node=training_cfg.get('nproc_per_node', 1),
        args=training_cfg.get('args', ''),
        max_retries=training_cfg.get('max_retries', settings.job_defaults.max_retries),
        checkpoint_interval_minutes=training_cfg.get('checkpoint_interval_minutes', settings.job_defaults.checkpoint_interval_minutes),
        backend=training_cfg.get('backend', settings.job_defaults.backend)
    )
    
    try:
        token_interceptor = TokenClientInterceptor()
        base_channel = grpc.insecure_channel(manager_addr)
        channel = grpc.intercept_channel(base_channel, token_interceptor)
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
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to global config.yaml")
    subparsers = parser.add_subparsers(dest="command")
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a job config")
    submit_parser.add_argument("job_config", type=str, help="Path to job YAML config")
    submit_parser.add_argument("--manager", type=str, default=None, help="Manager address (host:port)")
    
    args = parser.parse_args()
    
    if args.command == "submit":
        submit_job(args.job_config, args.manager, args.config)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
