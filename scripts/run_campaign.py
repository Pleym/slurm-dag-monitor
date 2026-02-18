from __future__ import annotations

import argparse
import json
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class RunOutcome:
    run_id: str
    status: str
    makespan_s: float | None


def http_json(method: str, url: str, *, body: bytes | None = None, headers: dict[str, str] | None = None) -> dict:
    req = urllib.request.Request(url, method=method)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    data = body
    with urllib.request.urlopen(req, data=data, timeout=60) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def submit_run(api_base: str, dag_yaml: str) -> str:
    payload = dag_yaml.encode("utf-8")
    resp = http_json(
        "POST",
        f"{api_base}/runs",
        body=payload,
        headers={"content-type": "application/yaml"},
    )
    return str(resp["run_id"])


def wait_run(api_base: str, run_id: str, *, poll_s: float = 0.2, timeout_s: float = 600) -> str:
    deadline = time.time() + timeout_s
    while True:
        run = http_json("GET", f"{api_base}/runs/{run_id}")
        status = str(run["status"])
        if status in ("succeeded", "failed"):
            return status
        if time.time() > deadline:
            raise TimeoutError(f"Run {run_id} timed out")
        time.sleep(poll_s)


def fetch_results(api_base: str, run_id: str) -> RunOutcome:
    res = http_json("GET", f"{api_base}/runs/{run_id}/results")
    return RunOutcome(run_id=run_id, status=str(res["status"]), makespan_s=res.get("makespan_s"))


def patch_dag_max_concurrency(dag_yaml: str, max_concurrency: int) -> str:
    data = yaml.safe_load(dag_yaml)
    if not isinstance(data, dict):
        raise ValueError("DAG YAML must be a mapping")
    data["max_concurrency"] = int(max_concurrency)
    return yaml.safe_dump(data, sort_keys=False)


def compute_speedup_efficiency(rows: list[dict]) -> list[dict]:
    baseline = None
    for r in rows:
        if r["max_concurrency"] == 1 and r["status"] == "succeeded" and r["makespan_s"] is not None:
            baseline = float(r["makespan_s"])
            break

    for r in rows:
        p = int(r["max_concurrency"])
        ms = r["makespan_s"]
        if baseline is None or ms is None or ms <= 0:
            r["baseline_s"] = baseline
            r["speedup"] = None
            r["efficiency"] = None
        else:
            speedup = baseline / float(ms)
            r["baseline_s"] = baseline
            r["speedup"] = speedup
            r["efficiency"] = speedup / float(p)

    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a simple strong-scaling campaign by varying DAG max_concurrency")
    parser.add_argument("--api", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--dag", default="examples/demo_dag.yaml", help="Path to DAG YAML")
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 2, 4], help="List of max_concurrency values")
    parser.add_argument("--timeout-s", type=float, default=600, help="Per-run timeout")
    parser.add_argument("--out", default="data/campaign.json", help="Output JSON path")
    args = parser.parse_args()

    dag_path = Path(args.dag)
    dag_yaml = dag_path.read_text(encoding="utf-8")

    rows: list[dict] = []
    for c in args.concurrency:
        patched = patch_dag_max_concurrency(dag_yaml, c)
        run_id = submit_run(args.api, patched)
        status = wait_run(args.api, run_id, timeout_s=float(args.timeout_s))
        outcome = fetch_results(args.api, run_id)

        rows.append(
            {
                "max_concurrency": int(c),
                "run_id": run_id,
                "status": status,
                "makespan_s": outcome.makespan_s,
            }
        )

    rows = sorted(rows, key=lambda r: r["max_concurrency"])
    rows = compute_speedup_efficiency(rows)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
