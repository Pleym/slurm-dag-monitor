from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .models import RunStatus, TaskStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SqliteStorage:
    db_path: Path

    def init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    dag_name TEXT NOT NULL,
                    dag_version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    finished_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_runs (
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    queued_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    exit_code INTEGER,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    error TEXT,
                    PRIMARY KEY (run_id, task_id)
                )
                """
            )

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        finally:
            conn.close()

    def create_run(self, *, run_id: str, dag_name: str, dag_version: int) -> None:
        now = utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO runs(run_id, dag_name, dag_version, status, submitted_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, dag_name, dag_version, RunStatus.queued.value, now),
            )

    def set_run_status(self, *, run_id: str, status: RunStatus, finished_at: datetime | None = None) -> None:
        with self._connect() as conn:
            if finished_at is None:
                conn.execute("UPDATE runs SET status = ? WHERE run_id = ?", (status.value, run_id))
            else:
                conn.execute(
                    "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                    (status.value, finished_at.isoformat(), run_id),
                )

    def create_task_runs(self, *, run_id: str, task_ids: list[str]) -> None:
        now = utcnow().isoformat()
        with self._connect() as conn:
            for task_id in task_ids:
                conn.execute(
                    """
                    INSERT INTO task_runs(run_id, task_id, status, queued_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (run_id, task_id, TaskStatus.queued.value, now),
                )

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
            return cur.fetchone()

    def list_task_runs(self, run_id: str) -> list[sqlite3.Row]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM task_runs WHERE run_id = ? ORDER BY task_id", (run_id,))
            return list(cur.fetchall())

    def set_task_status(
        self,
        *,
        run_id: str,
        task_id: str,
        status: TaskStatus,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        exit_code: int | None = None,
        stdout_path: str | None = None,
        stderr_path: str | None = None,
        error: str | None = None,
    ) -> None:
        fields = ["status = ?"]
        values: list[object] = [status.value]

        if started_at is not None:
            fields.append("started_at = ?")
            values.append(started_at.isoformat())
        if finished_at is not None:
            fields.append("finished_at = ?")
            values.append(finished_at.isoformat())
        if exit_code is not None:
            fields.append("exit_code = ?")
            values.append(int(exit_code))
        if stdout_path is not None:
            fields.append("stdout_path = ?")
            values.append(stdout_path)
        if stderr_path is not None:
            fields.append("stderr_path = ?")
            values.append(stderr_path)
        if error is not None:
            fields.append("error = ?")
            values.append(error)

        values.extend([run_id, task_id])
        with self._connect() as conn:
            conn.execute(
                f"UPDATE task_runs SET {', '.join(fields)} WHERE run_id = ? AND task_id = ?",
                tuple(values),
            )
