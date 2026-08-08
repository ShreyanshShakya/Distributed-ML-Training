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


class Settings(BaseModel):
    manager: ManagerSettings = Field(default_factory=ManagerSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    allocator: AllocatorSettings = Field(default_factory=AllocatorSettings)
    job_defaults: JobDefaults = Field(default_factory=JobDefaults)


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