from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class RunStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class TaskStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    skipped = "skipped"


class DagTaskSpec(BaseModel):
    id: str = Field(min_length=1)
    command: str = Field(min_length=1, description="Commande shell exécutée par la tâche")
    depends_on: list[str] = Field(default_factory=list)


class DagSpec(BaseModel):
    name: str = Field(min_length=1)
    version: int = Field(ge=1)
    max_concurrency: int = Field(default=4, ge=1, le=256)
    tasks: list[DagTaskSpec] = Field(min_length=1)


class RunCreateResponse(BaseModel):
    run_id: str


class TaskRunView(BaseModel):
    task_id: str
    status: TaskStatus
    queued_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    exit_code: int | None
    stdout_path: str | None
    stderr_path: str | None
    error: str | None


class RunView(BaseModel):
    run_id: str
    dag_name: str
    dag_version: int
    status: RunStatus
    submitted_at: datetime
    finished_at: datetime | None
    tasks: list[TaskRunView]


class RunResults(BaseModel):
    run_id: str
    status: RunStatus
    makespan_s: float | None
    task_durations_s: dict[str, float]
    resource_summary: dict[str, Any]
