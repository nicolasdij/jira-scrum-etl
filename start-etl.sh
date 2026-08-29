#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate
export DAGSTER_HOME="$(pwd)/dagster_home"
# Port 3000 is dagster's default but is often taken by other local services,
# which surfaces as a confusing 404 from whatever else is bound there.
DAGSTER_PORT="${DAGSTER_PORT:-3001}"
dg dev -p "$DAGSTER_PORT"
