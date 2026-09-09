#!/usr/bin/env bash
# Arranca SOLO este code location. La instancia de Dagster es compartida con
# telus-mw-reporter: para levantar los dos a la vez, usa
# ~/Dev/dagster-workspace/start.sh.
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate

# DAGSTER_HOME ya no vive en este repo, sino en el workspace, porque el
# historial de runs y el estado de schedules y sensores son de la instancia
# compartida y no de un proyecto.
WORKSPACE_DAGSTER_HOME="$(pwd)/../dagster-workspace/dagster_home"
if [[ ! -d "$WORKSPACE_DAGSTER_HOME" ]]; then
  echo "Error: no se encontró $WORKSPACE_DAGSTER_HOME" >&2
  echo "El workspace debe estar junto a este repo, en ~/Dev/dagster-workspace." >&2
  exit 1
fi
export DAGSTER_HOME="$(cd "$WORKSPACE_DAGSTER_HOME" && pwd)"
# Port 3000 is dagster's default but is often taken by other local services,
# which surfaces as a confusing 404 from whatever else is bound there.
DAGSTER_PORT="${DAGSTER_PORT:-3001}"
dg dev -p "$DAGSTER_PORT"
