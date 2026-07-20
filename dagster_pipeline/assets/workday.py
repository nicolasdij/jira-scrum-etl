"""Dagster asset for the Workday "My Team's Upcoming Time Off" export.

Produces one raw table in DuckDB:
    raw.raw_workday_time_off

The Workday export (kept at data/workday-time_off.csv) carries a preamble
(title, an ``Organizations`` block, and ``Start Date`` / ``End Date`` rows)
before the real header, plus scattered blank rows. This asset strips the
preamble, keeps only the columns the pipeline needs, drops blank rows, and
writes the result to DuckDB so dbt can read it via source('raw', ...).

To refresh from a new export, drop the fresh file at data/workday-time_off.csv
and re-materialise this asset (it runs as part of the full pipeline and the
capacity-inputs sync job).
"""

from __future__ import annotations

import csv
import os
from datetime import datetime

from dagster import asset

from dagster_pipeline.assets.gsheets import _db

# Path to the raw Workday export, relative to the repo root (the Dagster
# working directory). Overridable for tests / alternate locations.
WORKDAY_EXPORT_PATH = os.environ.get(
    "WORKDAY_TIME_OFF_PATH", "data/workday-time_off.csv"
)

# Header cell that marks the start of the tabular section.
_HEADER_MARKER = "Time Off Date"

# Raw Workday column -> raw DuckDB column. Only these are carried through.
_COLUMN_MAP = {
    "Time Off Date": "time_off_date",
    "Employee ID": "employee_id",
    "Worker": "worker",
    "Time Off Type": "time_off_type",
    "Units": "units",
}


def _parse_export(path: str) -> list[list[str]]:
    """Return normalized data rows (values ordered per _COLUMN_MAP) from the export."""
    # utf-8-sig strips the BOM Workday prepends.
    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    header_idx = next(
        (i for i, row in enumerate(rows) if row and row[0].strip() == _HEADER_MARKER),
        None,
    )
    if header_idx is None:
        raise ValueError(f"Could not find a '{_HEADER_MARKER}' header row in {path}")

    raw_header = [c.strip() for c in rows[header_idx]]
    try:
        src_idx = {col: raw_header.index(col) for col in _COLUMN_MAP}
    except ValueError as exc:
        raise ValueError(f"Missing expected column in {path}: {exc}") from exc

    out: list[list[str]] = []
    for row in rows[header_idx + 1:]:
        # Skip blank / short separator rows and rows without a date.
        if len(row) <= max(src_idx.values()):
            continue
        values = [row[src_idx[col]].strip() for col in _COLUMN_MAP]
        if not values[0]:  # no time_off_date
            continue
        out.append(values)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_workday_time_off
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_workday_time_off",
    key_prefix=["raw"],
    group_name="raw_workday",
    op_tags={"dagster/concurrency_key": "duckdb"},
    description="Per-day time-off entries normalized from the Workday export CSV.",
)
def raw_workday_time_off(context) -> None:
    records = _parse_export(WORKDAY_EXPORT_PATH)
    context.log.info(f"Parsed {len(records)} rows from {WORKDAY_EXPORT_PATH}")

    con = _db()
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_workday_time_off (
            time_off_date VARCHAR,
            employee_id   VARCHAR,
            worker        VARCHAR,
            time_off_type VARCHAR,
            units         VARCHAR,
            _extracted_at VARCHAR
        )
    """)
    now = datetime.utcnow().isoformat()
    con.executemany(
        "INSERT INTO raw.raw_workday_time_off VALUES (?, ?, ?, ?, ?, ?)",
        [[*values, now] for values in records],
    )
    context.log.info("Loaded raw_workday_time_off into DuckDB")
    con.close()
