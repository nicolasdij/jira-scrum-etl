#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate
export DAGSTER_HOME="$(pwd)/dagster_home"
dg dev
