"""Dagster asset for writing dbt mart results back to Google Sheets.

Writes two tabs into the output spreadsheet:
    "Capacity Plan"  ← mart_capacity_planning
    "Sprint History" ← mart_sprint_history
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

import duckdb
from dagster import AssetKey, asset

from dagster_pipeline.resources.gsheets_resource import GSheetsResource

CAPACITY_PLAN_SHEET = "Capacity Plan"
SPRINT_HISTORY_SHEET = "Sprint History"


def _serialize(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _query_to_rows(db_path: str, query: str) -> list[list[Any]]:
    """Execute a query against DuckDB and return rows as a list of lists (header first)."""
    con = duckdb.connect(db_path, read_only=True)
    rel = con.execute(query)
    columns = [desc[0] for desc in rel.description]
    data = rel.fetchall()
    con.close()
    return [columns] + [[_serialize(v) for v in row] for row in data]


@asset(
    name="gsheets_capacity_plan_export",
    group_name="exports",
    deps=[AssetKey(["marts", "mart_capacity_planning"])],
    description=(
        "Writes mart_capacity_planning to the 'Capacity Plan' tab in the output spreadsheet."
    ),
)
def gsheets_capacity_plan_export(
    context,
    gsheets: GSheetsResource,
) -> None:
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    rows = _query_to_rows(
        db_path,
        """
        SELECT
            target_sprint,
            subteam,
            recommended_capacity_points,
            ROUND(next_sprint_availability_pct * 100, 0) || '%' AS availability_pct,
            rolling_avg_velocity,
            sprints_in_window,
            subteam_size,
            total_available_days,
            last_closed_sprint_name
        FROM marts.mart_capacity_planning
        ORDER BY subteam
        """,
    )

    context.log.info(f"Writing {len(rows) - 1} capacity plan rows to Google Sheets")
    gsheets.overwrite_output_sheet(CAPACITY_PLAN_SHEET, rows)
    context.log.info(f"'{CAPACITY_PLAN_SHEET}' tab updated successfully.")


@asset(
    name="gsheets_sprint_history_export",
    group_name="exports",
    deps=[AssetKey(["marts", "mart_sprint_history"])],
    description=(
        "Writes mart_sprint_history to the 'Sprint History' tab in the output spreadsheet."
    ),
)
def gsheets_sprint_history_export(
    context,
    gsheets: GSheetsResource,
) -> None:
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    rows = _query_to_rows(
        db_path,
        """
        SELECT
            sprint_name,
            subteam,
            sprint_start_date,
            burnt_points,
            ROUND(capacity_pct * 100, 0) || '%' AS capacity_pct,
            subteam_size,
            total_available_days,
            weighted_velocity,
            rolling_avg_velocity,
            sprints_in_window
        FROM marts.mart_sprint_history
        ORDER BY sprint_start_date, subteam
        """,
    )

    context.log.info(f"Writing {len(rows) - 1} sprint history rows to Google Sheets")
    gsheets.overwrite_output_sheet(SPRINT_HISTORY_SHEET, rows)
    context.log.info(f"'{SPRINT_HISTORY_SHEET}' tab updated successfully.")
