from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .metrics import RUNS_RUNNING, TASKS_RUNNING, RUNS_TOTAL, TASKS_TOTAL, TASK_DURATION
from .models import DagSpec, RunStatus, TaskStatus
from .runner import LocalCommandRunner
from .storage import SqliteStorage, utcnow


@dataclass(frozen=True)
class Orchestrator:
    storage: SqliteStorage
    runner: LocalCommandRunner

    async def run_dag(self, *, run_id: str, dag: DagSpec) -> None:
        self.storage.set_run_status(run_id=run_id, status=RunStatus.running)
        RUNS_RUNNING.inc()

        task_specs = {t.id: t for t in dag.tasks}
        deps = {t.id: set(t.depends_on) for t in dag.tasks}

        running: set[str] = set()
        succeeded: set[str] = set()
        failed: set[str] = set()

        async def launch(task_id: str) -> None:
            nonlocal running, succeeded, failed
            running.add(task_id)
            self.storage.set_task_status(
                run_id=run_id,
                task_id=task_id,
                status=TaskStatus.running,
                started_at=utcnow(),
            )
            TASKS_RUNNING.inc()

            try:
                result = await self.runner.run(run_id=run_id, task_id=task_id, command=task_specs[task_id].command)
                finished = utcnow()

                if result.exit_code == 0:
                    succeeded.add(task_id)
                    self.storage.set_task_status(
                        run_id=run_id,
                        task_id=task_id,
                        status=TaskStatus.succeeded,
                        finished_at=finished,
                        exit_code=result.exit_code,
                        stdout_path=result.stdout_path,
                        stderr_path=result.stderr_path,
                    )
                    TASKS_TOTAL.labels(status="succeeded", dag=dag.name, task=task_id).inc()
                    TASK_DURATION.labels(dag=dag.name, task=task_id).observe(result.wall_time_s)
                else:
                    failed.add(task_id)
                    self.storage.set_task_status(
                        run_id=run_id,
                        task_id=task_id,
                        status=TaskStatus.failed,
                        finished_at=finished,
                        exit_code=result.exit_code,
                        stdout_path=result.stdout_path,
                        stderr_path=result.stderr_path,
                        error=f"exit_code={result.exit_code}",
                    )
                    TASKS_TOTAL.labels(status="failed", dag=dag.name, task=task_id).inc()

            except Exception as e:
                failed.add(task_id)
                self.storage.set_task_status(
                    run_id=run_id,
                    task_id=task_id,
                    status=TaskStatus.failed,
                    finished_at=utcnow(),
                    error=str(e),
                )
                TASKS_TOTAL.labels(status="failed", dag=dag.name, task=task_id).inc()
            finally:
                running.discard(task_id)
                TASKS_RUNNING.dec()

        pending_tasks = set(task_specs.keys())
        in_flight: dict[str, asyncio.Task[None]] = {}

        try:
            while pending_tasks or in_flight:
                if failed:
                    # MVP: fail-fast → le run échoue dès qu’une tâche échoue
                    break

                runnable = [
                    t for t in pending_tasks
                    if deps[t].issubset(succeeded)
                ]

                while runnable and len(in_flight) < dag.max_concurrency:
                    task_id = runnable.pop(0)
                    pending_tasks.remove(task_id)
                    in_flight[task_id] = asyncio.create_task(launch(task_id))

                if in_flight:
                    done, _ = await asyncio.wait(in_flight.values(), return_when=asyncio.FIRST_COMPLETED)
                    for finished_task in done:
                        # remove finished tasks
                        for task_id, t in list(in_flight.items()):
                            if t is finished_task:
                                del in_flight[task_id]
                                break
                else:
                    await asyncio.sleep(0.05)

            if failed:
                self.storage.set_run_status(run_id=run_id, status=RunStatus.failed, finished_at=utcnow())
                RUNS_TOTAL.labels(status="failed", dag=dag.name).inc()
            else:
                self.storage.set_run_status(run_id=run_id, status=RunStatus.succeeded, finished_at=utcnow())
                RUNS_TOTAL.labels(status="succeeded", dag=dag.name).inc()

        finally:
            RUNS_RUNNING.dec()
