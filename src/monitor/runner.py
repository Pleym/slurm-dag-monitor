from __future__ import annotations

import asyncio
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    wall_time_s: float
    stdout_path: str
    stderr_path: str
    resource_summary: dict[str, object]


class ProcessSampler:
    def __init__(self, proc: psutil.Process, *, period_s: float = 0.5) -> None:
        self._proc = proc
        self._period_s = period_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._cpu: list[float] = []
        self._rss: list[int] = []

    def start(self) -> None:
        try:
            self._proc.cpu_percent(interval=None)
        except Exception:
            pass
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def summary(self) -> dict[str, object]:
        cpu_avg = sum(self._cpu) / len(self._cpu) if self._cpu else None
        rss_max = max(self._rss) if self._rss else None
        return {"cpu_percent_avg": cpu_avg, "rss_bytes_max": rss_max, "samples": len(self._cpu)}

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._cpu.append(float(self._proc.cpu_percent(interval=None)))
                self._rss.append(int(self._proc.memory_info().rss))
            except Exception:
                pass
            time.sleep(self._period_s)


class LocalCommandRunner:
    def __init__(self, *, runs_dir: Path) -> None:
        self._runs_dir = runs_dir

    async def run(self, *, run_id: str, task_id: str, command: str) -> CommandResult:
        task_dir = self._runs_dir / run_id
        task_dir.mkdir(parents=True, exist_ok=True)

        stdout_path = task_dir / f"{task_id}.stdout.log"
        stderr_path = task_dir / f"{task_id}.stderr.log"

        t0 = time.perf_counter()
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )

        sampler = None
        try:
            sampler = ProcessSampler(psutil.Process(proc.pid))
            sampler.start()
        except Exception:
            sampler = None

        out, err = await proc.communicate()
        wall = time.perf_counter() - t0

        stdout_path.write_bytes(out or b"")
        stderr_path.write_bytes(err or b"")

        if sampler is not None:
            sampler.stop()
            summary = sampler.summary()
        else:
            summary = {"samples": 0}

        return CommandResult(
            exit_code=int(proc.returncode or 0),
            wall_time_s=float(wall),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            resource_summary=summary,
        )
