#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH=src uvicorn monitor.api:app --reload --port 8000
