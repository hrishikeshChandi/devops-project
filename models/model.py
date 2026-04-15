from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime, timezone


class WorkerCreate(BaseModel):
    cpu_cores: int
    ram: int
    simulate: bool = True

    model_config = {
        "json_schema_extra": {
            "cpu_cores": 4,
            "ram": 8,
            "simulate": True,
        }
    }


class Worker(BaseModel):
    cpu_cores: int
    ram: int
    available_ram: int
    available_cpu: int
    assigned_tasks: List[str] = Field(default_factory=list)
    last_heartbeat: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "active"


class WorkerResponse(BaseModel):
    id: str
    cpu_cores: int
    ram: int
    available_cpu: int
    available_ram: int
    status: str
    last_heartbeat: datetime


class TaskCreate(BaseModel):
    data: Dict
    required_cpu: int
    required_ram: int
    priority: int = 0
    model_config = {
        "json_schema_extra": {
            "data": {"type": "example"},
            "required_cpu": 2,
            "required_ram": 4,
            "priority": 0,
        }
    }


class StatusUpdate(BaseModel):
    status: str


class Task(BaseModel):
    status: str = "pending"
    data: Dict
    priority: int = 0
    retry_count: int = 0
    assigned_worker: Optional[str] = None
    started_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    required_cpu: int = 0
    required_ram: int = 0
    allocated_cpu: int = 0
    allocated_ram: int = 0

    # RL Features
    rl_state: Optional[str] = None
    rl_action_taken: Optional[int] = None
    rl_outcome: Optional[Dict] = None


class TaskResponse(BaseModel):
    id: str
    status: str
    data: Dict
    priority: int
    assigned_worker: Optional[str]
    retry_count: int
    started_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    required_cpu: int
    required_ram: int
    allocated_cpu: int
    allocated_ram: int

    # RL Features
    rl_state: Optional[str] = None
    rl_action_taken: Optional[int] = None
    rl_outcome: Optional[Dict] = None
