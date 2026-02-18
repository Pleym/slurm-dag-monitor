# slurm-dag-monitor

MVP d’un **orchestrateur de DAG** (workflow) avec une approche “plateforme HPC” : soumission d’un DAG en YAML, exécution des tâches avec dépendances (style Slurm `afterok`), stockage des runs, et **monitoring Prometheus/Grafana**.

## Fonctionnalités (MVP)

- API FastAPI : créer un run et suivre son exécution
- DAG YAML : tâches + dépendances + `max_concurrency`
- Runner local (commandes shell) : pratique pour développer sans cluster
- Persistance SQLite : run/task status + timestamps + chemins de logs
- Observabilité : endpoint Prometheus `GET /metrics/` + dashboard Grafana auto-provisionné
- Mini campagne scaling : calcule `speedup` et `efficiency` en variant `max_concurrency`

## Installation

Prérequis : `uv` + Python stable (3.12/3.13). Sur certaines machines, `uv` peut sélectionner Python 3.14 par défaut; ce repo inclut `.python-version` et la commande ci-dessous force 3.12.

```bash
brew install uv
uv venv --python python3.12 --clear
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Lancer l’API

```bash
PYTHONPATH=src uvicorn monitor.api:app --port 8000
```

Endpoints utiles :

- `GET /health`
- `POST /runs` (body YAML)
- `GET /runs/{run_id}`
- `GET /runs/{run_id}/results`
- `GET /metrics/`

## Tutoriel : installer le monitoring (Prometheus + Grafana)

1) Démarrer Prometheus + Grafana (Docker Desktop doit être lancé) :

```bash
docker compose up -d
```

2) Ouvrir :

- Prometheus : `http://localhost:9090`
- Grafana : `http://localhost:3000` (admin/admin)

3) Le dashboard “DAG Monitor (MVP)” est provisionné automatiquement.

## Usage rapide

Soumettre le DAG de démo :

```bash
curl -s -X POST http://localhost:8000/runs \
  -H 'content-type: application/yaml' \
  --data-binary @examples/demo_dag.yaml
```

Mini campagne scaling (strong scaling “au sens MVP”) :

```bash
PYTHONPATH=src python scripts/run_campaign.py --dag examples/demo_dag.yaml --concurrency 1 2 4
```

Le résultat est écrit dans `data/campaign.json`.

## Améliorations futures (idées)

- Adapter Slurm réel : `sbatch` + `--dependency=afterok` + suivi via `squeue/sacct`
- Modèle de ressources par tâche : CPU/mémoire/time-limit + métriques queue-time vs run-time
- Retry policy + timeouts + cancellation (API)
- Stockage production : PostgreSQL + artifacts dans Azure Blob Storage
- Intégration Azure : déployer l’API sur Azure Container Apps + Azure Managed Grafana

