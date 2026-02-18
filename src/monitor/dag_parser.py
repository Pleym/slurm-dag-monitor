from __future__ import annotations

from typing import Any

import yaml

from .models import DagSpec


def parse_dag_from_yaml(raw: str) -> DagSpec:
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError("DAG YAML must be a mapping")
    return DagSpec.model_validate(data)


def validate_dag(dag: DagSpec) -> None:
    ids = [t.id for t in dag.tasks]
    if len(set(ids)) != len(ids):
        raise ValueError("Duplicate task id")

    known = set(ids)
    for t in dag.tasks:
        for dep in t.depends_on:
            if dep not in known:
                raise ValueError(f"Unknown dependency '{dep}' for task '{t.id}'")

    # cycle check (simple DFS)
    deps = {t.id: set(t.depends_on) for t in dag.tasks}
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(n: str) -> None:
        if n in visited:
            return
        if n in visiting:
            raise ValueError("Cycle detected in DAG")
        visiting.add(n)
        for d in deps[n]:
            dfs(d)
        visiting.remove(n)
        visited.add(n)

    for node in deps:
        dfs(node)
