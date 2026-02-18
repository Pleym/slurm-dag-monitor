from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

RUNS_RUNNING = Gauge("dagmon_runs_running", "Nombre de runs en cours")
TASKS_RUNNING = Gauge("dagmon_tasks_running", "Nombre de tâches en cours")

RUNS_TOTAL = Counter(
    "dagmon_runs_total",
    "Nombre de runs terminés",
    labelnames=("status", "dag"),
)

TASKS_TOTAL = Counter(
    "dagmon_tasks_total",
    "Nombre de tâches terminées",
    labelnames=("status", "dag", "task"),
)

TASK_DURATION = Histogram(
    "dagmon_task_duration_seconds",
    "Durée wall-clock d’une tâche",
    labelnames=("dag", "task"),
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
