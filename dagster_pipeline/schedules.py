"""Dagster schedules and sensors for the Jira SCRUM ETL pipeline."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    SkipReason,
    define_asset_job,
    job,
    op,
    sensor,
)
from dagster._core.storage.dagster_run import FINISHED_STATUSES

_STATE_RANK = {"active": 0, "future": 1, "closed": 2}


def _effective_state(states: list[str]) -> str:
    """Return the most permissive state from a list: active > future > closed."""
    return min(states, key=lambda s: _STATE_RANK.get(s, 99))

# ─────────────────────────────────────────────────────────────────────────────
# Jobs
# ─────────────────────────────────────────────────────────────────────────────

# Apply this run-level tag to every job that writes to DuckDB or the Sprint
# Planning Google Sheet. The QueuedRunCoordinator in dagster_home/dagster.yaml
# limits runs sharing this tag to 1 at a time, which is what prevents:
#   - DuckDB single-writer lock contention across runs
#   - concurrent prepop runs racing on gspread append_rows (the bug that left
#     100 blank rows interleaved with real data in the Sprint Items sheet)
# Op-level concurrency tags only serialise ops *within* a run, so they're
# necessary but not sufficient on their own.
_DUCKDB_RUN_TAGS = {"dagster/concurrency_key": "duckdb"}


def _duckdb_job(*, name, selection, description):
    """define_asset_job pre-tagged with _DUCKDB_RUN_TAGS.

    Every job in this file materialises at least one DuckDB-writing asset
    (or a gspread-writing asset that races on append_rows), so all of them
    must carry the run-level concurrency tag. Using this wrapper instead of
    define_asset_job(...) makes the tag impossible to forget on a new job.
    """
    return define_asset_job(
        name=name,
        selection=selection,
        tags=_DUCKDB_RUN_TAGS,
        description=description,
    )


# raw_jira_issues is partitioned per-sprint and managed by its own sensor/job below.
_partitioned_issues = AssetSelection.assets(AssetKey(["raw", "raw_jira_issues"]))

# Full pipeline: extract → dbt transform → export.
# raw_jira_issues is excluded because it is a partitioned asset handled separately.
full_pipeline_job = _duckdb_job(
    name="full_pipeline_job",
    selection=AssetSelection.all() - _partitioned_issues,
    description=(
        "Full ETL run: extract from Jira and Google Sheets, run dbt transforms, "
        "write results back to Google Sheets."
    ),
)

# On-demand extraction only (no export) — useful for mid-sprint data refresh
extraction_only_job = _duckdb_job(
    name="extraction_only_job",
    selection=AssetSelection.groups("raw_jira", "raw_gsheets", "raw_workday") - _partitioned_issues,
    description="Extract raw data from Jira, Google Sheets, and the Workday export into DuckDB without running dbt.",
)

# Per-sprint partitioned job — one run per sprint partition.
raw_jira_issues_job = _duckdb_job(
    name="raw_jira_issues_job",
    selection=_partitioned_issues,
    description="Materialises a single sprint partition of raw_jira_issues.",
)

# Pre-populate team splits for upcoming sprints
team_splits_prepopulate_job = _duckdb_job(
    name="team_splits_prepopulate_job",
    selection=AssetSelection.assets("gsheets_team_splits_prepopulator"),
    description="Insert skeleton rows into the Team Splits sheet for future sprints.",
)

# Refresh Sprint Plan tab after editing the Sprint Items input sheet.
# Runs: raw_sprint_planning → stg_sprint_planning → mart_sprint_plan → gsheets_sprint_plan_export
sprint_plan_update_job = _duckdb_job(
    name="sprint_plan_update_job",
    selection=AssetSelection.assets(
        AssetKey(["raw", "raw_sprint_planning"]),
        AssetKey(["staging", "stg_sprint_planning"]),
        AssetKey(["marts", "mart_sprint_plan"]),
        "gsheets_sprint_plan_export",
    ),
    description=(
        "Re-reads the Sprint Items input sheet and rebuilds the Sprint Plan output tab. "
        "Run this after editing planned_points or include values in the Sprint Items sheet."
    ),
)

# Refresh Capacity Plan and Sprint Plan tabs after editing the Time Off,
# Public Holidays, or Team Splits input sheets — the three inputs that drive
# member availability and therefore subteam capacity.
# Runs: raw_time_off / raw_workday_time_off / raw_public_holidays / raw_team_splits
#       → stg_* → int_member_sprint_availability → int_subteam_sprint_capacity_pct
#       → mart_capacity_planning → mart_sprint_plan
#       → gsheets_capacity_plan_export + gsheets_sprint_plan_export
# mart_rolling_velocity is unaffected by these inputs, so it isn't rebuilt;
# stg_sprint_planning is left alone too (its outputs feed mart_sprint_plan but
# don't depend on the capacity inputs).
capacity_inputs_sync_job = _duckdb_job(
    name="capacity_inputs_sync_job",
    selection=AssetSelection.assets(
        AssetKey(["raw", "raw_time_off"]),
        AssetKey(["raw", "raw_workday_time_off"]),
        AssetKey(["raw", "raw_public_holidays"]),
        AssetKey(["raw", "raw_team_splits"]),
        AssetKey(["staging", "stg_time_off_workday"]),
        AssetKey(["staging", "stg_time_off"]),
        AssetKey(["staging", "stg_public_holidays"]),
        AssetKey(["staging", "stg_team_splits"]),
        AssetKey(["intermediate", "int_member_sprint_availability"]),
        AssetKey(["intermediate", "int_subteam_sprint_capacity_pct"]),
        AssetKey(["marts", "mart_capacity_planning"]),
        AssetKey(["marts", "mart_sprint_plan"]),
        AssetKey(["marts", "mart_member_availability"]),
        "gsheets_capacity_plan_export",
        "gsheets_sprint_plan_export",
        "gsheets_member_availability_export",
    ),
    description=(
        "Re-reads Time Off / Public Holidays / Team Splits input sheets and rebuilds "
        "the Capacity Plan, Sprint Plan, and Member Availability output tabs. Run this "
        "after editing any of those three sheets so subteam capacity (and the Sprint "
        "Plan headroom column) reflects the new availability."
    ),
)

# Monthly cleanup of Dagster run history older than 30 days. Calls
# DagsterInstance.delete_run, which removes the row in history/runs.db and the
# associated rows in the event-log storage. Compute logs under storage/<run_id>/
# are not touched by delete_run — if those grow large, add a separate retention
# job for them. Only terminal runs (SUCCESS / FAILURE / CANCELED) are deleted;
# in-progress runs are left alone even if their last-updated timestamp is older
# than the cutoff (which can happen for genuinely stuck/zombie runs). Does not
# touch DuckDB or dbt artifacts, so no duckdb concurrency tag is needed. Asset
# materialisations emitted by deleted runs go with them — including dbt model
# materialisations — so any asset whose last materialisation is older than
# 30 days will appear "never materialised" in the UI until the next pipeline
# run rematerialises it.
_CLEANUP_RUN_RETENTION_DAYS = 30


@op(description="Delete Dagster runs older than the retention window.")
def cleanup_old_runs_op(context) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=_CLEANUP_RUN_RETENTION_DAYS)
    instance = context.instance
    filters = RunsFilter(updated_before=cutoff, statuses=list(FINISHED_STATUSES))

    deleted = 0
    failed_ids: set[str] = set()
    while True:
        runs = instance.get_runs(filters=filters, limit=200)
        # Skip runs we already failed to delete this invocation so a single
        # un-deletable run can't keep the same page coming back forever.
        runs = [r for r in runs if r.run_id not in failed_ids]
        if not runs:
            break
        progress_before = deleted
        for run in runs:
            try:
                instance.delete_run(run.run_id)
                deleted += 1
            except Exception as exc:
                failed_ids.add(run.run_id)
                context.log.warning(f"Failed to delete run {run.run_id}: {exc}")
        context.log.info(f"Deleted {deleted} runs so far...")
        # Defensive guard: if a page produced zero deletions, every row in it
        # is already in failed_ids — bail rather than re-fetch the same page.
        if deleted == progress_before:
            break

    context.log.info(
        f"Cleanup complete. Deleted {deleted} runs older than "
        f"{_CLEANUP_RUN_RETENTION_DAYS} days (cutoff: {cutoff.isoformat()})."
        + (f" Skipped {len(failed_ids)} run(s) that errored on delete." if failed_ids else "")
    )


@job(description="Monthly cleanup of Dagster run history older than 30 days.")
def cleanup_old_runs_job():
    cleanup_old_runs_op()


# One-click sync after editing a Jira issue (e.g. setting a t-shirt size):
# re-fetches the chosen sprint's issues from Jira, refreshes blank cells in the
# Sprint Items sheet, and rebuilds the Sprint Plan output tab.
# When launched, pick the future sprint's start_date (YYYY-MM-DD) as the partition key.
sprint_planning_sync_job = _duckdb_job(
    name="sprint_planning_sync_job",
    selection=AssetSelection.assets(
        AssetKey(["raw", "raw_jira_issues"]),
        "gsheets_sprint_planning_prepopulator",
        AssetKey(["raw", "raw_sprint_planning"]),
        AssetKey(["staging", "stg_sprint_planning"]),
        AssetKey(["marts", "mart_sprint_plan"]),
        "gsheets_sprint_plan_export",
    ),
    description=(
        "End-to-end sync after a Jira edit: re-fetches raw_jira_issues for the chosen "
        "sprint-date partition, refreshes blank planned_points/subteam/status cells in "
        "the Sprint Items sheet, then rebuilds the Sprint Plan output tab. "
        "Pick the future sprint's start_date (YYYY-MM-DD) as the partition when launching."
    ),
)

# ─────────────────────────────────────────────────────────────────────────────
# Schedules
# ─────────────────────────────────────────────────────────────────────────────

# Runs every Tuesday at 08:00 Madrid time (UTC+2 in summer, UTC+1 in winter).
# Using UTC 07:00 as a reasonable approximation (adjust for DST if needed).
weekly_tuesday_schedule = ScheduleDefinition(
    name="weekly_tuesday_pipeline",
    job=full_pipeline_job,
    cron_schedule="0 7 * * 2",   # 07:00 UTC every Tuesday
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Full pipeline run every Tuesday at ~08:00 Madrid time. "
        "Produces the capacity plan for the sprint that just started."
    ),
)

# Monthly run-history cleanup. 1st of each month at 05:00 UTC (~06:00–07:00 Madrid).
# Deletes Dagster runs older than _CLEANUP_RUN_RETENTION_DAYS.
cleanup_monthly_schedule = ScheduleDefinition(
    name="cleanup_old_runs_monthly",
    job=cleanup_old_runs_job,
    cron_schedule="0 5 1 * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "On the 1st of each month, delete Dagster run records, event logs, and "
        "compute logs older than 30 days to keep dagster_home/history/ from growing "
        "unbounded."
    ),
)

# Pre-populate team splits the Monday before sprint start (sprints start Tuesday).
team_splits_monday_schedule = ScheduleDefinition(
    name="team_splits_monday_prepopulate",
    job=team_splits_prepopulate_job,
    cron_schedule="0 6 * * 1",   # 06:00 UTC every Monday
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Inserts skeleton rows for future sprints into the Team Splits sheet "
        "every Monday morning, giving you time to fill in splits before Tuesday's run."
    ),
)

# ─────────────────────────────────────────────────────────────────────────────
# Jira sprint sensor (optional — polls for sprint state changes)
# ─────────────────────────────────────────────────────────────────────────────

@sensor(
    job=raw_jira_issues_job,
    minimum_interval_seconds=3600,
    description=(
        "Triggers raw_jira_issues materialisation for each sprint-date partition. "
        "One partition covers all boards (DELI + NFR) starting on that date. "
        "Active/future dates refresh daily; the last-closed date per board refreshes "
        "daily to capture late resolutions; older closed dates load once."
    ),
)
def raw_jira_issues_partition_sensor(context):
    """Dispatch raw_jira_issues runs for every sprint-date partition that needs updating."""
    import duckdb
    import os
    from datetime import date

    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    all_partitions = context.instance.get_dynamic_partitions("sprint_dates")
    if not all_partitions:
        yield SkipReason("No sprint date partitions registered yet; run raw_jira_sprints first.")
        return

    try:
        con = duckdb.connect(db_path, read_only=True)

        # Effective state per date: active > future > closed across all boards.
        date_states_map: dict[str, list[str]] = defaultdict(list)
        for sd, state in con.execute(
            "SELECT SUBSTR(start_date, 1, 10), state FROM raw.raw_jira_sprints WHERE start_date IS NOT NULL"
        ).fetchall():
            date_states_map[sd].append(state)
        effective_states = {
            sd: _effective_state(states) for sd, states in date_states_map.items()
        }

        # For fully-loaded check: all sprint_ids for a date must have rows in raw_jira_issues.
        date_to_sprint_ids: dict[str, set[int]] = defaultdict(set)
        for sd, sid in con.execute(
            "SELECT SUBSTR(start_date, 1, 10), sprint_id FROM raw.raw_jira_sprints WHERE start_date IS NOT NULL"
        ).fetchall():
            date_to_sprint_ids[sd].add(sid)

        loaded_sprint_ids: set[int] = {
            row[0]
            for row in con.execute("SELECT DISTINCT sprint_id FROM raw.raw_jira_issues").fetchall()
        }
        fully_loaded_dates: set[str] = {
            sd for sd, sids in date_to_sprint_ids.items() if sids.issubset(loaded_sprint_ids)
        }

        # Start dates of the most recently closed sprint per board — refreshed daily.
        last_closed_dates: set[str] = {
            row[0]
            for row in con.execute(
                """
                SELECT DISTINCT SUBSTR(start_date, 1, 10)
                FROM raw.raw_jira_sprints
                WHERE sprint_id IN (
                    SELECT sprint_id FROM (
                        SELECT sprint_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY board_id
                                   ORDER BY complete_date DESC NULLS LAST
                               ) AS rn
                        FROM raw.raw_jira_sprints
                        WHERE state = 'closed'
                    ) ranked
                    WHERE rn = 1
                ) AND start_date IS NOT NULL
                """
            ).fetchall()
        }

        con.close()
    except Exception:
        yield SkipReason("DuckDB not yet initialised; skipping.")
        return

    today = date.today().isoformat()
    run_requests = []
    for partition_key in all_partitions:
        state = effective_states.get(partition_key, "unknown")
        if state == "closed":
            if partition_key in last_closed_dates:
                # Re-fetch daily: may still have unresolved issues from the final day.
                run_requests.append(RunRequest(
                    run_key=f"sprint_date_{partition_key}_{today}",
                    partition_key=partition_key,
                ))
            elif partition_key not in fully_loaded_dates:
                # First-time load for an older closed sprint cycle.
                run_requests.append(RunRequest(
                    run_key=f"sprint_date_{partition_key}_closed_initial",
                    partition_key=partition_key,
                ))
            # Older closed date fully loaded: no RunRequest.
        else:
            # Active/future: refresh once per day.
            run_requests.append(RunRequest(
                run_key=f"sprint_date_{partition_key}_{today}",
                partition_key=partition_key,
            ))

    if run_requests:
        yield from run_requests
    else:
        yield SkipReason("All sprint date partitions are up to date.")


@sensor(
    job=full_pipeline_job,
    minimum_interval_seconds=3600,   # check at most once per hour
    description=(
        "Triggers a full pipeline run when a Jira sprint transitions from "
        "'active' to 'closed', without waiting for the Tuesday schedule."
    ),
)
def jira_sprint_closed_sensor(context):
    """Detect newly closed sprints and trigger the pipeline."""
    import duckdb
    import os

    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    # cursor key: the last sprint_id we fired on
    last_fired_sprint_id = int(context.cursor or "0")

    try:
        con = duckdb.connect(db_path, read_only=True)
        # Find closed sprints with ID > last cursor
        newly_closed = con.execute(
            """
            SELECT sprint_id, sprint_name
            FROM raw.raw_jira_sprints
            WHERE state = 'closed'
              AND sprint_id > ?
            ORDER BY sprint_id DESC
            LIMIT 1
            """,
            [last_fired_sprint_id],
        ).fetchone()
        con.close()
    except Exception:
        yield SkipReason("DuckDB not yet initialised; skipping sensor tick.")
        return

    if newly_closed:
        sprint_id, sprint_name = newly_closed
        context.update_cursor(str(sprint_id))
        yield RunRequest(
            run_key=f"sprint_closed_{sprint_id}",
            tags={"sprint_name": sprint_name, "trigger": "sensor"},
        )
    else:
        yield SkipReason("No newly closed sprints detected.")
