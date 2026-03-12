# slurm-dag-monitor

> **English** | [Français](#français)

A **DAG (Directed Acyclic Graph) orchestration MVP** with an HPC-platform approach: submit a workflow as YAML, execute tasks with Slurm-style dependencies (`afterok`), persist run history, and monitor everything with **Prometheus + Grafana**.

## Features (MVP)

- **FastAPI REST API** – create runs and track their execution
- **YAML DAG definition** – tasks, dependencies, and `max_concurrency`
- **Local runner** – executes shell commands (no Slurm cluster needed for development)
- **SQLite persistence** – run/task status, timestamps, and log file paths
- **Observability** – `GET /metrics/` Prometheus endpoint + auto-provisioned Grafana dashboard
- **Scaling campaign** – measures `speedup` and `efficiency` across different `max_concurrency` values

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FastAPI (api.py)                         │
│  POST /runs  GET /runs/{id}  GET /runs/{id}/results  GET /metrics│
└───────┬──────────────────────────────────────┬───────────────────┘
        │ parse & validate                      │ Prometheus metrics
        ▼                                       ▼
┌───────────────┐  schedule   ┌──────────────────────────────────┐
│  dag_parser   │────────────▶│         Orchestrator             │
│  + models     │             │  (async, respects dependencies   │
└───────────────┘             │   and max_concurrency)           │
                              └──────────┬───────────────────────┘
                                         │ run tasks
                                         ▼
                              ┌──────────────────────────────────┐
                              │        LocalCommandRunner        │
                              │  asyncio subprocess + psutil     │
                              │  resource sampling (CPU/RSS)     │
                              └──────────┬───────────────────────┘
                                         │ persist
                                         ▼
                              ┌──────────────────────────────────┐
                              │        SQLite (storage.py)       │
                              │  tables: runs, task_runs         │
                              └──────────────────────────────────┘

Monitoring stack (Docker):
  FastAPI /metrics ──scrape──▶ Prometheus :9090 ──▶ Grafana :3000
```

### Module overview

| File | Role |
|---|---|
| `src/monitor/api.py` | FastAPI application, endpoint routing, startup |
| `src/monitor/dag_parser.py` | Parse YAML into `DagSpec`, validate (no cycles, no unknown deps) |
| `src/monitor/models.py` | Pydantic models: `DagSpec`, `DagTaskSpec`, `RunView`, `RunResults` |
| `src/monitor/orchestrator.py` | Async DAG executor: dependency resolution, concurrency control, fail-fast |
| `src/monitor/runner.py` | Shell command execution, stdout/stderr capture, CPU/memory sampling |
| `src/monitor/storage.py` | SQLite CRUD for `runs` and `task_runs` tables |
| `src/monitor/metrics.py` | Prometheus gauges, counters, histogram definitions |
| `scripts/run_campaign.py` | Strong-scaling campaign script (varies `max_concurrency`, computes speedup) |

## Installation

Prerequisites: `uv` + Python 3.12 or 3.13. On some machines `uv` may pick Python 3.14 by default; the `.python-version` file and the command below lock to 3.12.

```bash
brew install uv
uv venv --python python3.12 --clear
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Running the API

```bash
PYTHONPATH=src uvicorn monitor.api:app --port 8000
```

Available endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health` | Health check |
| `POST` | `/runs` | Submit a DAG run (YAML body) |
| `GET`  | `/runs/{run_id}` | Get run status and task details |
| `GET`  | `/runs/{run_id}/results` | Get makespan and per-task durations |
| `GET`  | `/metrics/` | Prometheus metrics |

## Setting up monitoring (Prometheus + Grafana)

1. Start Prometheus and Grafana (Docker Desktop must be running):

```bash
docker compose up -d
```

2. Open:
   - Prometheus: `http://localhost:9090`
   - Grafana: `http://localhost:3000` (admin / admin)

3. The **"DAG Monitor (MVP)"** dashboard is provisioned automatically with 4 panels:
   - Runs currently running
   - Tasks currently running
   - Task throughput (5-minute rate)
   - Task duration p95

## Quick start

Submit the demo DAG:

```bash
curl -s -X POST http://localhost:8000/runs \
  -H 'content-type: application/yaml' \
  --data-binary @examples/demo_dag.yaml
```

The demo DAG (`examples/demo_dag.yaml`) models a 4-task workflow:

```
load ──▶ compute_1 ──▶ aggregate
     └─▶ compute_2 ──▶
```

Run a strong-scaling campaign (varies `max_concurrency`, writes results to `data/campaign.json`):

```bash
PYTHONPATH=src python scripts/run_campaign.py --dag examples/demo_dag.yaml --concurrency 1 2 4
```

## Future improvements

- Real Slurm backend: `sbatch` + `--dependency=afterok` + tracking via `squeue`/`sacct`
- Per-task resource model: CPU/memory/time-limit + queue-time vs run-time metrics
- Retry policy, timeouts, and run cancellation via API
- Production storage: PostgreSQL + artifacts in Azure Blob Storage
- Azure deployment: Azure Container Apps + Azure Managed Grafana

---

## Français

MVP d'un **orchestrateur de DAG** (workflow) avec une approche "plateforme HPC" : soumission d'un DAG en YAML, exécution des tâches avec dépendances (style Slurm `afterok`), stockage des runs, et **monitoring Prometheus/Grafana**.

### Fonctionnalités (MVP)

- API FastAPI : créer un run et suivre son exécution
- DAG YAML : tâches + dépendances + `max_concurrency`
- Runner local (commandes shell) : pratique pour développer sans cluster
- Persistance SQLite : run/task status + timestamps + chemins de logs
- Observabilité : endpoint Prometheus `GET /metrics/` + dashboard Grafana auto-provisionné
- Mini campagne scaling : calcule `speedup` et `efficiency` en variant `max_concurrency`

### Description des composants

**API** (`api.py`) – Point d'entrée HTTP. Reçoit un DAG en YAML, le valide, crée un run en base, puis délègue l'exécution à l'Orchestrator de façon asynchrone.

**DAG Parser** (`dag_parser.py` + `models.py`) – Désérialise le YAML en modèles Pydantic (`DagSpec`, `DagTaskSpec`). Valide : pas d'ID dupliqués, dépendances existantes, absence de cycle (DFS).

**Orchestrator** (`orchestrator.py`) – Moteur d'exécution asynchrone. Résout l'ordre des tâches en tenant compte du graphe de dépendances et de `max_concurrency`. Stratégie fail-fast : arrêt dès qu'une tâche échoue.

**Runner** (`runner.py`) – Lance les commandes shell via `asyncio.create_subprocess_shell`. Capture stdout/stderr dans des fichiers de log. Échantillonne CPU et mémoire RSS en arrière-plan toutes les 500 ms (`psutil`).

**Storage** (`storage.py`) – Couche SQLite. Deux tables : `runs` (cycle de vie du run) et `task_runs` (statut, timestamps, chemins de logs, code de retour par tâche).

**Metrics** (`metrics.py`) – Définit les métriques Prometheus : jauges (runs/tasks en cours), compteurs (runs/tasks terminés), histogramme (durée des tâches).

### Installation

Prérequis : `uv` + Python stable (3.12/3.13). Sur certaines machines, `uv` peut sélectionner Python 3.14 par défaut ; ce repo inclut `.python-version` et la commande ci-dessous force 3.12.

```bash
brew install uv
uv venv --python python3.12 --clear
source .venv/bin/activate
uv pip install -r requirements.txt
```

### Lancer l'API

```bash
PYTHONPATH=src uvicorn monitor.api:app --port 8000
```

### Monitoring (Prometheus + Grafana)

```bash
docker compose up -d
# Prometheus : http://localhost:9090
# Grafana    : http://localhost:3000  (admin/admin)
```

### Soumettre un DAG

```bash
curl -s -X POST http://localhost:8000/runs \
  -H 'content-type: application/yaml' \
  --data-binary @examples/demo_dag.yaml
```

### Campagne scaling

```bash
PYTHONPATH=src python scripts/run_campaign.py --dag examples/demo_dag.yaml --concurrency 1 2 4
```

Le résultat est écrit dans `data/campaign.json`.

### Améliorations futures

- Adapter Slurm réel : `sbatch` + `--dependency=afterok` + suivi via `squeue/sacct`
- Modèle de ressources par tâche : CPU/mémoire/time-limit + métriques queue-time vs run-time
- Retry policy + timeouts + cancellation (API)
- Stockage production : PostgreSQL + artifacts dans Azure Blob Storage
- Intégration Azure : déployer l'API sur Azure Container Apps + Azure Managed Grafana
