#!/usr/bin/env bash
# Starts ONLY this code location. The Dagster instance is shared with
# telus-mw-reporter: to bring both up at once, use
# ~/Dev/dagster-workspace/start.sh.
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate

# DAGSTER_HOME no longer lives in this repo but in the workspace: run history
# and schedule/sensor state belong to the shared instance, not to a project.
WORKSPACE_DAGSTER_HOME="$(pwd)/../dagster-workspace/dagster_home"
if [[ ! -d "$WORKSPACE_DAGSTER_HOME" ]]; then
  echo "Error: $WORKSPACE_DAGSTER_HOME not found" >&2
  echo "The workspace must sit next to this repo, at ~/Dev/dagster-workspace." >&2
  exit 1
fi
export DAGSTER_HOME="$(cd "$WORKSPACE_DAGSTER_HOME" && pwd)"
# Port 3000 is dagster's default but is often taken by other local services,
# which surfaces as a confusing 404 from whatever else is bound there.
DAGSTER_PORT="${DAGSTER_PORT:-3001}"
dg dev -p "$DAGSTER_PORT"
