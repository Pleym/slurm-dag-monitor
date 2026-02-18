# slurm-dag-monitor — Slurm-style DAG orchestration + Prometheus/Grafana + Azure-ready

Projet "portfolio" court et utile : un **orchestrateur de DAG** (workflow) qui soumet des tâches, suit leur état, stocke les résultats, et expose des métriques Prometheus pour des dashboards Grafana.

L’angle est volontairement **plateforme / observabilité / workflow state machine** (proche d’une "distributed compute platform") plutôt que micro-optimisation CPU.

## Ce que ça démontre (aligné plateforme HPC)

- **API DAG déclarative** (DAG YAML/JSON) + exécutions (`run_id`)
- **Orchestration** : dépendances `afterok` (style Slurm), états tâche/run, concurrence limitée
- **Observabilité** : métriques Prometheus `/metrics` (durées, succès/échec, tâches en cours)
- **Traçabilité** : logs par tâche, métadonnées en SQLite
- **Hooks HPC** : design "adapter" pour exécution locale aujourd’hui, Slurm réel demain (SSH + `sbatch/squeue/sacct`).

## Architecture (MVP)

- `FastAPI` (control plane) : soumission & query
- `Orchestrator loop` : planifie les tâches prêtes (deps satisfaites)
- `Runner (LocalCommandRunner)` : exécute des commandes shell localement (mock pour dev)
- `Storage (SQLite)` : runs/tasks + timings
- `Prometheus` : scrape `/metrics` → `Grafana`

## Démarrage rapide

### Option recommandée : avec `uv`

Installer `uv` (macOS) :

```bash
brew install uv
```

Ce repo inclut aussi `.python-version` (3.12.9) pour éviter les surprises.

Créer l’environnement et installer les dépendances :

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Note: ce projet nécessite une version stable de Python (3.12/3.13). Sur cette machine, `uv` peut sélectionner Python 3.14 par défaut, ce qui casse certaines wheels natives. La commande la plus robuste est :

```bash
uv venv --python python3.12 --clear
```

Lancer l’API :

```bash
PYTHONPATH=src uvicorn monitor.api:app --reload --port 8000
```

### Alternative : avec `venv` + `pip`

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

PYTHONPATH=src uvicorn monitor.api:app --reload --port 8000
```

Soumettre un DAG exemple :

```bash
curl -s -X POST http://localhost:8000/runs \
  -H 'content-type: application/yaml' \
  --data-binary @examples/demo_dag.yaml | jq
```

Lister l’état :

```bash
curl -s http://localhost:8000/runs/<RUN_ID> | jq
```

Résultats :

```bash
curl -s http://localhost:8000/runs/<RUN_ID>/results | jq
```

Métriques :

```bash
curl -s http://localhost:8000/metrics/ | head
```

## Strong scaling (mini-campagne)

Une façon rapide de montrer des métriques HPC (sans Slurm réel) est de relancer le même DAG en variant `max_concurrency` (parallélisme inter-tâches), puis de calculer :

$$speedup = T_1 / T_p$$
$$efficiency = speedup / p$$

```bash
source .venv/bin/activate
PYTHONPATH=src python scripts/run_campaign.py --dag examples/demo_dag.yaml --concurrency 1 2 4
```

Le script écrit aussi `data/campaign.json`.

## Grafana (dashboard prêt) — optionnel

Le service expose déjà `/metrics`. Pour afficher ça rapidement dans Grafana :

```bash
docker compose up -d
```

Si tu as une erreur “Cannot connect to the Docker daemon”, démarre Docker Desktop puis relance la commande.

- Prometheus : `http://localhost:9090`
- Grafana : `http://localhost:3000` (login `admin` / `admin`)
- Dashboard auto-provisionné : **DAG Monitor (MVP)** (URL typique: `http://localhost:3000/d/dag-monitor-mvp`)

## Déploiement Azure (réaliste, non-bloquant)

Ce repo est fait pour être **Azure-friendly** sans t’obliger à provisionner Azure :

- **Control plane** : Azure Container Apps (ou App Service) pour servir l’API FastAPI
- **Storage** : Azure Database for PostgreSQL (au lieu de SQLite)
- **Artifacts/logs** : Azure Blob Storage
- **Observability** : Azure Managed Grafana + Prometheus (Managed ou self-hosted)
- **HPC execution** : cluster Slurm (Azure CycleCloud ou on-prem) ; l’adapter Slurm serait une implémentation qui parle au headnode (SSH) et lit `squeue/sacct`.

## Pourquoi c’est utile (et pas trop ambitieux)

- Un recruteur plateforme voit immédiatement : DAG state machine, retries possibles, métriques exploitables, séparation control plane / data plane.
- Le périmètre est MVP : **un seul format de DAG**, **un seul runner local**, **Prometheus prêt**.

