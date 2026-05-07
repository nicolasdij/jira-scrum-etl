"""Dagster schedules and sensors for the Jira SCRUM ETL pipeline."""

from __future__ import annotations

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    define_asset_job,
    sensor,
)

# ─────────────────────────────────────────────────────────────────────────────
# Jobs
# ─────────────────────────────────────────────────────────────────────────────

# raw_jira_issues is partitioned per-sprint and managed by its own sensor/job below.
_partitioned_issues = AssetSelection.assets(AssetKey("raw_jira_issues"))

# Full pipeline: extract → dbt transform → export.
# raw_jira_issues is excluded because it is a partitioned asset handled separately.
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all() - _partitioned_issues,
    description=(
        "Full ETL run: extract from Jira and Google Sheets, run dbt transforms, "
        "write results back to Google Sheets."
    ),
)

# On-demand extraction only (no export) — useful for mid-sprint data refresh
extraction_only_job = define_asset_job(
    name="extraction_only_job",
    selection=AssetSelection.groups("raw_jira", "raw_gsheets") - _partitioned_issues,
    description="Extract raw data from Jira and Google Sheets into DuckDB without running dbt.",
)

# Per-sprint partitioned job — one run per sprint partition.
raw_jira_issues_job = define_asset_job(
    name="raw_jira_issues_job",
    selection=_partitioned_issues,
    description="Materialises a single sprint partition of raw_jira_issues.",
)

# Pre-populate team splits for upcoming sprints
team_splits_prepopulate_job = define_asset_job(
    name="team_splits_prepopulate_job",
    selection=AssetSelection.assets("gsheets_team_splits_prepopulator"),
    description="Insert skeleton rows into the Team Splits sheet for future sprints.",
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
        "Triggers raw_jira_issues materialisation for each sprint partition: "
        "once for closed sprints (skipped internally if already loaded), "
        "and daily for active/future sprints to keep issue states fresh."
    ),
)
def raw_jira_issues_partition_sensor(context):
    """Dispatch a raw_jira_issues run for every sprint partition that needs updating."""
    import duckdb
    import os
    from datetime import date

    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    all_partitions = context.instance.get_dynamic_partitions("sprints")
    if not all_partitions:
        yield SkipReason("No sprint partitions registered yet; run raw_jira_sprints first.")
        return

    try:
        con = duckdb.connect(db_path, read_only=True)
        sprint_states = {
            str(row[0]): row[1]
            for row in con.execute(
                "SELECT sprint_id, state FROM raw.raw_jira_sprints"
            ).fetchall()
        }
        loaded_sprint_ids = {
            str(row[0])
            for row in con.execute(
                "SELECT DISTINCT sprint_id FROM raw.raw_jira_issues"
            ).fetchall()
        }
        con.close()
    except Exception:
        yield SkipReason("DuckDB not yet initialised; skipping.")
        return

    today = date.today().isoformat()
    run_requests = []
    for partition_key in all_partitions:
        state = sprint_states.get(partition_key, "unknown")
        if state == "closed":
            if partition_key not in loaded_sprint_ids:
                # First-time load for this closed sprint.
                run_requests.append(RunRequest(
                    run_key=f"sprint_{partition_key}_closed_initial",
                    partition_key=partition_key,
                ))
            # Already loaded closed sprint: asset guards internally, nothing to do.
        else:
            # Active/future sprints: refresh once per day.
            run_requests.append(RunRequest(
                run_key=f"sprint_{partition_key}_{today}",
                partition_key=partition_key,
            ))

    if run_requests:
        yield from run_requests
    else:
        yield SkipReason("All sprint partitions are up to date.")


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
