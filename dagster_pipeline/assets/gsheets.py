"""Dagster assets for reading from and pre-populating Google Sheets.

Produces raw tables in DuckDB:
    raw.raw_time_off
    raw.raw_public_holidays
    raw.raw_team_splits

Also provides gsheets_team_splits_prepopulator, which writes skeleton rows
into the raw_team_splits Google Sheet tab for upcoming (future) sprints.
"""

from __future__ import annotations

import os
from datetime import datetime

import duckdb
from dagster import asset

from dagster_pipeline.resources.gsheets_resource import GSheetsResource

# Google Sheets tab names (in the input spreadsheet)
SHEET_TIME_OFF = "Time Off"
SHEET_HOLIDAYS = "Public Holidays"
SHEET_TEAM_SPLITS = "Team Splits"


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    path = db_path or os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    return con


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_time_off
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_time_off",
    group_name="raw_gsheets",
    description="Time-off periods per team member read from Google Sheets.",
)
def raw_time_off(context, gsheets: GSheetsResource) -> None:
    records = gsheets.read_input_sheet(SHEET_TIME_OFF)
    context.log.info(f"Read {len(records)} time-off records")

    con = _db()
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_time_off (
            member_name VARCHAR,
            start_date  VARCHAR,
            end_date    VARCHAR,
            board       VARCHAR,
            _extracted_at VARCHAR
        )
    """)
    now = datetime.utcnow().isoformat()
    con.executemany(
        "INSERT INTO raw.raw_time_off VALUES (?, ?, ?, ?, ?)",
        [
            [
                r.get("member_name"), r.get("start_date"), r.get("end_date"),
                r.get("board"), now,
            ]
            for r in records
        ],
    )
    context.log.info("Loaded raw_time_off into DuckDB")
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_public_holidays
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_public_holidays",
    group_name="raw_gsheets",
    description="Public holidays per region (comma-separated) from Google Sheets.",
)
def raw_public_holidays(context, gsheets: GSheetsResource) -> None:
    records = gsheets.read_input_sheet(SHEET_HOLIDAYS)
    context.log.info(f"Read {len(records)} holiday records")

    con = _db()
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_public_holidays (
            holiday_date  VARCHAR,
            holiday_name  VARCHAR,
            regions       VARCHAR,
            _extracted_at VARCHAR
        )
    """)
    now = datetime.utcnow().isoformat()
    con.executemany(
        "INSERT INTO raw.raw_public_holidays VALUES (?, ?, ?, ?)",
        [
            [r.get("holiday_date"), r.get("holiday_name"), r.get("regions"), now]
            for r in records
        ],
    )
    context.log.info("Loaded raw_public_holidays into DuckDB")
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_team_splits
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_team_splits",
    group_name="raw_gsheets",
    description="Per-sprint, per-member board/sub-team allocation read from Google Sheets.",
)
def raw_team_splits(context, gsheets: GSheetsResource) -> None:
    records = gsheets.read_input_sheet(SHEET_TEAM_SPLITS)
    context.log.info(f"Read {len(records)} team-split records")

    con = _db()
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_team_splits (
            sprint_name     VARCHAR,
            member_name     VARCHAR,
            board           VARCHAR,
            subteam         VARCHAR,
            days_allocated  DOUBLE,
            _extracted_at   VARCHAR
        )
    """)
    now = datetime.utcnow().isoformat()
    rows = []
    for r in records:
        days = r.get("days_allocated")
        # Convert empty strings to NULL
        if days == "" or days is None:
            days = None
        else:
            try:
                days = float(days)
            except (TypeError, ValueError):
                days = None
        rows.append([
            r.get("sprint_name"), r.get("member_name"), r.get("board"),
            r.get("subteam"), days, now,
        ])
    con.executemany("INSERT INTO raw.raw_team_splits VALUES (?, ?, ?, ?, ?, ?)", rows)
    context.log.info("Loaded raw_team_splits into DuckDB")
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Asset: gsheets_team_splits_prepopulator
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="gsheets_team_splits_prepopulator",
    group_name="raw_gsheets",
    deps=["raw_jira_sprints"],
    description=(
        "Inserts skeleton rows into the Team Splits sheet for any (sprint, member, board) "
        "combination that is missing — only for sprints with state='future'. "
        "Active and closed sprints are never modified."
    ),
)
def gsheets_team_splits_prepopulator(
    context,
    gsheets: GSheetsResource,
) -> None:
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")
    con = duckdb.connect(db_path)

    # Fetch future sprints from the raw table
    future_sprints = con.execute(
        "SELECT sprint_name, board_id FROM raw.raw_jira_sprints WHERE state = 'future'"
    ).fetchall()

    # Load seed data for team members
    # Seeds are loaded by dbt; read from DuckDB if already seeded, else fall back to CSV.
    try:
        members = con.execute(
            "SELECT member_name, board, subteam FROM raw.team_members"
        ).fetchall()
    except Exception:
        # Seeds not yet loaded — read the CSV directly
        import csv, pathlib
        seed_path = pathlib.Path(__file__).parents[2] / "dbt_project" / "seeds" / "team_members.csv"
        with open(seed_path) as f:
            members = [(r["member_name"], r["board"], r["subteam"]) for r in csv.DictReader(f)]

    # Load board_config to resolve board_id → board_key
    try:
        board_map = {
            row[0]: row[1]
            for row in con.execute("SELECT board_id, board_key FROM raw.board_config").fetchall()
        }
    except Exception:
        board_map = {74: "DELI", 212: "NFR"}

    con.close()

    if not future_sprints:
        context.log.info("No future sprints found; nothing to pre-populate.")
        return

    # Read existing rows to avoid duplicates
    existing = gsheets.read_input_sheet(SHEET_TEAM_SPLITS)
    existing_keys: set[tuple] = {
        (r.get("sprint_name"), r.get("member_name"), r.get("board"))
        for r in existing
    }

    # Ensure header exists
    if not existing:
        gsheets.overwrite_sheet(
            gsheets.input_spreadsheet_id,
            SHEET_TEAM_SPLITS,
            [["sprint_name", "member_name", "board", "subteam", "days_allocated"]],
        )

    new_rows: list[list] = []
    for sprint_name, board_id in future_sprints:
        board_key = board_map.get(board_id, str(board_id))
        for member_name, member_board, subteam in members:
            if member_board != board_key:
                continue
            key = (sprint_name, member_name, board_key)
            if key not in existing_keys:
                # days_allocated left blank for manual entry
                new_rows.append([sprint_name, member_name, board_key, subteam, ""])
                existing_keys.add(key)

    if new_rows:
        context.log.info(
            f"Appending {len(new_rows)} skeleton rows to '{SHEET_TEAM_SPLITS}'"
        )
        gsheets.append_rows_to_input_sheet(SHEET_TEAM_SPLITS, new_rows)
    else:
        context.log.info("All future sprint/member/board combinations already present in sheet.")
