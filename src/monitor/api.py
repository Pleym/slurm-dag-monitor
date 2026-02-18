from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from prometheus_client import make_asgi_app

from .dag_parser import parse_dag_from_yaml, validate_dag
from .models import RunCreateResponse, RunResults, RunStatus, RunView, TaskRunView, TaskStatus
from .orchestrator import Orchestrator
from .runner import LocalCommandRunner
from .storage import SqliteStorage


DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "dagmon.sqlite3"
RUNS_DIR = DATA_DIR / "runs"

app = FastAPI(title="hpc-dag-monitor", version="0.1.0")
app.mount("/metrics", make_asgi_app())

storage = SqliteStorage(DB_PATH)
storage.init_db()

orchestrator = Orchestrator(storage=storage, runner=LocalCommandRunner(runs_dir=RUNS_DIR))


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/runs", response_model=RunCreateResponse)
async def create_run(req: Request) -> RunCreateResponse:
    # Accept YAML by default for ergonomics
    raw = (await req.body()).decode("utf-8")
    try:
        dag = parse_dag_from_yaml(raw)
        validate_dag(dag)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    run_id = uuid.uuid4().hex
    storage.create_run(run_id=run_id, dag_name=dag.name, dag_version=dag.version)
    storage.create_task_runs(run_id=run_id, task_ids=[t.id for t in dag.tasks])

    asyncio.create_task(orchestrator.run_dag(run_id=run_id, dag=dag))
    return RunCreateResponse(run_id=run_id)


@app.get("/runs/{run_id}", response_model=RunView)
def get_run(run_id: str) -> RunView:
    run = storage.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")

    tasks = []
    for row in storage.list_task_runs(run_id):
        def parse_dt(v: str | None) -> datetime | None:
            return datetime.fromisoformat(v) if v else None

        tasks.append(
            TaskRunView(
                task_id=str(row["task_id"]),
                status=TaskStatus(str(row["status"])),
                queued_at=datetime.fromisoformat(str(row["queued_at"])),
                started_at=parse_dt(row["started_at"]),
                finished_at=parse_dt(row["finished_at"]),
                exit_code=row["exit_code"],
                stdout_path=row["stdout_path"],
                stderr_path=row["stderr_path"],
                error=row["error"],
            )
        )

    finished_at = run["finished_at"]
    return RunView(
        run_id=str(run["run_id"]),
        dag_name=str(run["dag_name"]),
        dag_version=int(run["dag_version"]),
        status=RunStatus(str(run["status"])),
        submitted_at=datetime.fromisoformat(str(run["submitted_at"])),
        finished_at=datetime.fromisoformat(finished_at) if finished_at else None,
        tasks=tasks,
    )


@app.get("/runs/{run_id}/results", response_model=RunResults)
def get_results(run_id: str) -> RunResults:
    run = storage.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")

    status = RunStatus(str(run["status"]))
    tasks = storage.list_task_runs(run_id)

    durations: dict[str, float] = {}
    for row in tasks:
        if row["started_at"] and row["finished_at"]:
            start = datetime.fromisoformat(str(row["started_at"]))
            end = datetime.fromisoformat(str(row["finished_at"]))
            durations[str(row["task_id"])] = (end - start).total_seconds()

    makespan_s = None
    if run["submitted_at"] and run["finished_at"]:
        makespan_s = (datetime.fromisoformat(str(run["finished_at"])) - datetime.fromisoformat(str(run["submitted_at"]))).total_seconds()

    # MVP: resource_summary par tâche non agrégé (on renvoie juste les chemins logs)
    resource_summary = {
        "artifacts_dir": str(RUNS_DIR / run_id),
    }

    return RunResults(
        run_id=run_id,
        status=status,
        makespan_s=makespan_s,
        task_durations_s=durations,
        resource_summary=resource_summary,
    )
