"""Dagster schedules and sensors for the Jira SCRUM ETL pipeline."""

from __future__ import annotations

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    SensorDefinition,
    SkipReason,
    define_asset_job,
    sensor,
)

# ─────────────────────────────────────────────────────────────────────────────
# Jobs
# ─────────────────────────────────────────────────────────────────────────────

# Full pipeline: extract → dbt (run externally via dagster-dbt) → export
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.groups(
        "raw_jira",
        "raw_gsheets",
        "exports",
    ),
    description=(
        "Full ETL run: extract from Jira and Google Sheets, trigger dbt, "
        "write results back to Google Sheets."
    ),
)

# On-demand extraction only (no export) — useful for mid-sprint data refresh
extraction_only_job = define_asset_job(
    name="extraction_only_job",
    selection=AssetSelection.groups("raw_jira", "raw_gsheets"),
    description="Extract raw data from Jira and Google Sheets into DuckDB without running dbt.",
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
