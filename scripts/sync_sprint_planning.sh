#!/usr/bin/env bash
# One command to refresh sprint planning and capacity, before or during a sprint.
# Covers Jira changes (items added/removed, points edited) and input-sheet edits
# (Time Off, Public Holidays, Team Splits, Sprint Items).
#
# Steps:
#   1. Re-fetch Jira issues: one raw_jira_issues_job run per sprint-date
#      partition, for the current sprint (latest key <= today) and the next two.
#      They carry the duckdb concurrency tag, so they execute one at a time.
#   2. If all succeed, one full_pipeline_job run: re-reads every input sheet,
#      rebuilds the dbt models, refreshes the Sprint Items / Team Splits sheets
#      and writes every output tab.
#
# The Dagster webserver + daemon must be running (./start-etl.sh or
# ~/Dev/dagster-workspace/start.sh), since queued runs are launched by the daemon.
#
# Usage:
#   scripts/sync_sprint_planning.sh              # Jira issues + full pipeline
#   scripts/sync_sprint_planning.sh --skip-jira  # full pipeline only (sheet edits)
#   scripts/sync_sprint_planning.sh --dry-run    # only print the partitions
#
# Env overrides: DAGSTER_HOST (default localhost), DAGSTER_PORT (default 3001),
# SPRINT_COUNT (default 3: current + 2 following).
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate

DRY_RUN=0
SKIP_JIRA=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --skip-jira) SKIP_JIRA=1 ;;
    -h|--help) sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $arg (use --help)" >&2; exit 2 ;;
  esac
done

# Same shared instance as start-etl.sh: the partition keys live there.
WORKSPACE_DAGSTER_HOME="$(pwd)/../dagster-workspace/dagster_home"
if [[ ! -d "$WORKSPACE_DAGSTER_HOME" ]]; then
  echo "Error: $WORKSPACE_DAGSTER_HOME not found" >&2
  exit 1
fi
export DAGSTER_HOME="$(cd "$WORKSPACE_DAGSTER_HOME" && pwd)"
export DAGSTER_HOST="${DAGSTER_HOST:-localhost}"
export DAGSTER_PORT="${DAGSTER_PORT:-3001}"
export SPRINT_COUNT="${SPRINT_COUNT:-3}"
export DRY_RUN SKIP_JIRA

python - <<'PY'
import os
import sys
import time
from datetime import date

from dagster import DagsterInstance, DagsterRunStatus
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError

ISSUES_JOB = "raw_jira_issues_job"
PIPELINE_JOB = "full_pipeline_job"
LOCATION_NAME = "jira-scrum-etl"
PARTITIONS_DEF = "sprint_dates"
POLL_SECONDS = 10
TERMINAL = {DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED}

host, port = os.environ["DAGSTER_HOST"], int(os.environ["DAGSTER_PORT"])
client = DagsterGraphQLClient(host, port_number=port)


def select_partitions() -> list[str]:
    count = int(os.environ["SPRINT_COUNT"])
    today = date.today().isoformat()
    keys = sorted(DagsterInstance.get().get_dynamic_partitions(PARTITIONS_DEF))
    if not keys:
        sys.exit(f"No '{PARTITIONS_DEF}' partitions registered; run {PIPELINE_JOB} first.")
    started = [k for k in keys if k <= today]
    if not started:
        sys.exit(f"No sprint has started on or before {today}.")
    current_idx = keys.index(started[-1])
    selected = keys[current_idx : current_idx + count]

    print(f"Today: {today}")
    print(f"Partitions: {', '.join(selected)} (current: {selected[0]})")
    if len(selected) < count:
        print(
            f"Warning: only {len(selected)} of {count} sprints are registered — "
            f"create the future sprints in Jira and re-run {PIPELINE_JOB}.",
            file=sys.stderr,
        )
    return selected


def submit(job_name: str, label: str, tags: dict | None = None) -> str:
    try:
        run_id = client.submit_job_execution(
            job_name, repository_location_name=LOCATION_NAME, tags=tags
        )
    except DagsterGraphQLClientError as exc:
        sys.exit(f"Failed to submit {label}: {exc}\nIs the Dagster UI running on {host}:{port}?")
    print(f"  {label} -> http://{host}:{port}/runs/{run_id}")
    return run_id


def wait(runs: dict[str, str]) -> list[str]:
    """Poll until every run is terminal; return the labels that did not succeed."""
    statuses: dict[str, DagsterRunStatus] = {}
    while len(statuses) < len(runs):
        time.sleep(POLL_SECONDS)
        for label, run_id in runs.items():
            if label in statuses:
                continue
            status = client.get_run_status(run_id)
            if status in TERMINAL:
                statuses[label] = status
                print(f"  {label}: {status.value}")
    return [label for label, s in statuses.items() if s != DagsterRunStatus.SUCCESS]


if os.environ["SKIP_JIRA"] == "1":
    print("Skipping the Jira issue re-fetch.")
else:
    selected = select_partitions()
    if os.environ["DRY_RUN"] == "1":
        sys.exit(0)
    print(f"Submitting {ISSUES_JOB}:")
    issue_runs = {key: submit(ISSUES_JOB, key, {"dagster/partition": key}) for key in selected}
    print("Waiting for the issue runs to finish...")
    failed = wait(issue_runs)
    if failed:
        sys.exit(f"Not running {PIPELINE_JOB}: issue runs did not succeed for {', '.join(failed)}.")

if os.environ["DRY_RUN"] == "1":
    sys.exit(0)

print(f"Submitting {PIPELINE_JOB}:")
pipeline_run = submit(PIPELINE_JOB, PIPELINE_JOB)
print("Waiting for it to finish...")
if wait({PIPELINE_JOB: pipeline_run}):
    sys.exit(f"{PIPELINE_JOB} did not succeed — check the run in the Dagster UI.")
print("Done: Sprint Items, Sprint Plan, Capacity Plan and the other output tabs are up to date.")
PY
