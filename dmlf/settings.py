from pathlib import Path
from typing import Any, Dict
from pydantic import BaseModel, Field
import yaml


class ManagerSettings(BaseModel):
    grpc_port: int = 50051
    http_health_port: int = 8080
    heartbeat_interval_sec: int = 5
    offline_timeout_sec: int = 15
    scheduler_interval_sec: int = 2
    auth_token: str = "CHANGE_ME_32_BYTE_HEX"


class AgentSettings(BaseModel):
    reconnect_base_sec: int = 2
    reconnect_max_sec: int = 30


class AllocatorSettings(BaseModel):
    gpu_weight: float = 1.0
    cpu_weight: float = 0.5
    ram_weight: float = 0.1


class JobDefaults(BaseModel):
    max_retries: int = 3
    checkpoint_interval_minutes: int = 5
    backend: str = "gloo"


class MetricsSettings(BaseModel):
    manager_port: int = 9090
    agent_port: int = 9091


class AutoscalerSettings(BaseModel):
    enabled: bool = True
    interval_sec: int = 30
    queue_depth_high: int = 5
    avg_cpu_high: float = 80.0
    avg_gpu_high: float = 70.0
    queue_depth_low: int = 0
    avg_cpu_low: float = 20.0
    avg_gpu_low: float = 10.0
    scale_in_cooldown_sec: int = 300
    scale_callback: str = "dmlf.autoscaler.default_callback:scale_callback"


class SchedulerSettings(BaseModel):
    plugin: str = "dmlf.manager.schedulers.builtin:PriorityBinPackScheduler"


class DistributedSettings(BaseModel):
    master_port: int = 29500
    rendezvous_timeout_sec: int = 30


class StorageSettings(BaseModel):
    database_path: str = "cluster.db"
    log_directory: str = "logs"


class MonitorSettings(BaseModel):
    node_check_interval_sec: int = 10


class Settings(BaseModel):
    manager: ManagerSettings = Field(default_factory=ManagerSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    allocator: AllocatorSettings = Field(default_factory=AllocatorSettings)
    job_defaults: JobDefaults = Field(default_factory=JobDefaults)
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)
    autoscaler: AutoscalerSettings = Field(default_factory=AutoscalerSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    distributed: DistributedSettings = Field(default_factory=DistributedSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    monitor: MonitorSettings = Field(default_factory=MonitorSettings)


_settings_instance: Settings | None = None


def load_settings(config_path: str | Path = "config.yaml") -> Settings:
    global _settings_instance
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r") as f:
        data = yaml.safe_load(f) or {}
    _settings_instance = Settings(**data)
    return _settings_instance


def get_settings() -> Settings:
    if _settings_instance is None:
        return load_settings()
    return _settings_instance