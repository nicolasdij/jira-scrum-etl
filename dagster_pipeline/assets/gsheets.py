"""Dagster assets for reading from and pre-populating Google Sheets.

Produces raw tables in DuckDB:
    raw.raw_time_off
    raw.raw_public_holidays
    raw.raw_team_splits

Also provides gsheets_team_splits_prepopulator, which writes skeleton rows
into the raw_team_splits Google Sheet tab for upcoming (future) sprints.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Optional

import time

import duckdb
from dagster import AssetKey, asset

from dagster_pipeline.resources.gsheets_resource import GSheetsResource

# Google Sheets tab names (in the input spreadsheet)
SHEET_TIME_OFF = "Time Off"
SHEET_HOLIDAYS = "Public Holidays"
SHEET_TEAM_SPLITS = "Team Splits"
SHEET_SPRINT_PLANNING = "Sprint Planning"

_BOARD_IDS = [74, 212]  # DELI=74, NFR=212


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(s: str | None) -> Optional[date]:
    """Parse a date string from Google Sheets or Jira ISO format into a date object."""
    if not s:
        return None
    s = s.strip()
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _compute_effective_days(
    sprint_start: str | None,
    sprint_end: str | None,
    regions: list[str],
    holiday_map: dict[str, set[str]],
    time_off_ranges: list[tuple[Optional[date], Optional[date]]],
) -> Optional[float]:
    """Count weekdays in [sprint_start, sprint_end] minus public holidays and time off."""
    start = _parse_date(sprint_start)
    end = _parse_date(sprint_end)
    if not start or not end or start > end:
        return None

    region_set = set(regions)
    count = 0
    current = start
    while current < end:
        if current.weekday() < 5:  # Mon–Fri
            date_str = current.strftime("%Y-%m-%d")
            holiday_regions = holiday_map.get(date_str, set())
            if not (region_set & holiday_regions):
                in_time_off = any(
                    to_start is not None and to_end is not None
                    and to_start <= current <= to_end
                    for to_start, to_end in time_off_ranges
                )
                if not in_time_off:
                    count += 1
        current += timedelta(days=1)
    return float(count)


def _db(db_path: str | None = None, retries: int = 8, delay: float = 5.0) -> duckdb.DuckDBPyConnection:
    path = db_path or os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            con.execute("CREATE SCHEMA IF NOT EXISTS raw")
            return con
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or attempt == retries - 1:
                raise
            time.sleep(delay)
    raise AssertionError("unreachable")


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_time_off
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_time_off",
    key_prefix=["raw"],
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
    key_prefix=["raw"],
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
    key_prefix=["raw"],
    group_name="raw_gsheets",
    deps=["gsheets_team_splits_prepopulator"],
    description="Per-sprint, per-member board/sub-team allocation read from Google Sheets.",
)
def raw_team_splits(context, gsheets: GSheetsResource) -> None:
    records = gsheets.read_input_sheet(SHEET_TEAM_SPLITS)
    context.log.info(f"Read {len(records)} team-split records")

    con = _db()
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_team_splits (
            sprint_name       VARCHAR,
            sprint_start_date VARCHAR,
            member_name       VARCHAR,
            board             VARCHAR,
            subteam           VARCHAR,
            days_allocated    DOUBLE,
            _extracted_at     VARCHAR
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
            r.get("sprint_name"), r.get("sprint_start_date"), r.get("member_name"),
            r.get("board"), r.get("subteam"), days, now,
        ])
    con.executemany("INSERT INTO raw.raw_team_splits VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    context.log.info("Loaded raw_team_splits into DuckDB")
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Asset: gsheets_team_splits_prepopulator
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="gsheets_team_splits_prepopulator",
    group_name="raw_gsheets",
    deps=[
        AssetKey(["raw", "raw_jira_sprints"]),
        AssetKey(["raw", "raw_time_off"]),
        AssetKey(["raw", "raw_public_holidays"]),
    ],
    description=(
        "Inserts skeleton rows into the Team Splits sheet for any (sprint, member, board) "
        "combination that is missing — only for sprints with state='future'. "
        "Active and closed sprints are never modified. "
        "days_allocated is pre-filled with each member's effective working days for the sprint, "
        "accounting for public holidays and logged time off."
    ),
)
def gsheets_team_splits_prepopulator(
    context,
    gsheets: GSheetsResource,
) -> None:
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")
    con = duckdb.connect(db_path)

    # Fetch future sprints with their date windows
    future_sprints_raw = con.execute(
        "SELECT sprint_name, board_id, start_date, end_date "
        "FROM raw.raw_jira_sprints WHERE state = 'future'"
    ).fetchall()

    # Deduplicate sprint dates by name (DELI and NFR boards share the same windows)
    sprint_dates: dict[str, tuple[str, str]] = {}
    future_sprints: list[tuple[str, int]] = []
    for sprint_name, board_id, start_date, end_date in future_sprints_raw:
        future_sprints.append((sprint_name, board_id))
        if sprint_name not in sprint_dates:
            sprint_dates[sprint_name] = (start_date, end_date)

    # Load seed data for team members
    try:
        members = con.execute(
            "SELECT member_name, board, subteam FROM raw.team_members"
        ).fetchall()
    except Exception:
        import csv, pathlib
        seed_path = pathlib.Path(__file__).parents[2] / "dbt_project" / "seeds" / "team_members.csv"
        with open(seed_path) as f:
            members = [(r["member_name"], r["board"], r["subteam"]) for r in csv.DictReader(f)]

    # Load member_regions: member_name → list[str]
    try:
        member_regions_map: dict[str, list[str]] = {
            row[0]: [r.strip() for r in row[1].split(",")]
            for row in con.execute("SELECT member_name, regions FROM raw.member_regions").fetchall()
        }
    except Exception:
        import csv, pathlib
        seed_path = pathlib.Path(__file__).parents[2] / "dbt_project" / "seeds" / "member_regions.csv"
        with open(seed_path) as f:
            member_regions_map = {
                r["member_name"]: [reg.strip() for reg in r["regions"].split(",")]
                for r in csv.DictReader(f)
            }

    # Load board_config to resolve board_id → board_key
    try:
        board_map = {
            row[0]: row[1]
            for row in con.execute("SELECT board_id, board_key FROM raw.board_config").fetchall()
        }
    except Exception:
        board_map = {74: "DELI", 212: "NFR"}

    # Load public holidays: date_str (YYYY-MM-DD) → set of regions
    holiday_map: dict[str, set[str]] = {}
    try:
        for hdate, hregions in con.execute(
            "SELECT holiday_date, regions FROM raw.raw_public_holidays"
        ).fetchall():
            date_key = _parse_date(hdate)
            if date_key:
                key_str = date_key.strftime("%Y-%m-%d")
                if key_str not in holiday_map:
                    holiday_map[key_str] = set()
                for region in [r.strip() for r in (hregions or "").split(",") if r.strip()]:
                    holiday_map[key_str].add(region)
    except Exception:
        context.log.warning("raw_public_holidays not available; holidays will not be deducted")

    # Load time off: member_name → list of (parsed_start, parsed_end)
    time_off_map: dict[str, list[tuple]] = {}
    try:
        for member_name, to_start, to_end in con.execute(
            "SELECT member_name, start_date, end_date FROM raw.raw_time_off"
        ).fetchall():
            if member_name not in time_off_map:
                time_off_map[member_name] = []
            time_off_map[member_name].append((_parse_date(to_start), _parse_date(to_end)))
    except Exception:
        context.log.warning("raw_time_off not available; time off will not be deducted")

    con.close()

    if not future_sprints:
        context.log.info("No future sprints found; nothing to pre-populate.")
        return

    # Pre-compute effective days for each (sprint_name, member_name) combination
    unique_members = {m[0] for m in members}
    effective_days_cache: dict[tuple[str, str], int | str] = {}
    for sprint_name, (sprint_start, sprint_end) in sprint_dates.items():
        for member_name in unique_members:
            regions = member_regions_map.get(member_name, [])
            time_offs = time_off_map.get(member_name, [])
            days = _compute_effective_days(sprint_start, sprint_end, regions, holiday_map, time_offs)
            effective_days_cache[(sprint_name, member_name)] = days if days is not None else ""

    # Read existing rows to avoid duplicates
    existing = gsheets.read_input_sheet(SHEET_TEAM_SPLITS)
    existing_keys: set[tuple] = {
        (r.get("sprint_name"), r.get("member_name"), r.get("board"), r.get("subteam"))
        for r in existing
    }

    new_rows: list[list] = []
    for sprint_name, board_id in future_sprints:
        board_key = board_map.get(board_id, str(board_id))
        start_date = (sprint_dates.get(sprint_name) or ("", ""))[0][:10]
        for member_name, member_board, subteam in members:
            if member_board != board_key:
                continue
            key = (sprint_name, member_name, board_key, subteam)
            if key not in existing_keys:
                days_allocated = effective_days_cache.get((sprint_name, member_name), "")
                new_rows.append([sprint_name, start_date, member_name, board_key, subteam, days_allocated])
                existing_keys.add(key)

    if not existing:
        gsheets.overwrite_sheet(
            gsheets.input_spreadsheet_id,
            SHEET_TEAM_SPLITS,
            [["sprint_name", "sprint_start_date", "member_name", "board", "subteam", "days_allocated"]],
        )

    if new_rows:
        context.log.info(f"Appending {len(new_rows)} skeleton rows to '{SHEET_TEAM_SPLITS}'")
        gsheets.append_rows_to_input_sheet(SHEET_TEAM_SPLITS, new_rows)
    else:
        context.log.info("All future sprint/member/board combinations already present in sheet.")


# ─────────────────────────────────────────────────────────────────────────────
# Asset: gsheets_sprint_planning_prepopulator
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="gsheets_sprint_planning_prepopulator",
    group_name="raw_gsheets",
    deps=[
        AssetKey(["raw", "raw_jira_issues"]),
        AssetKey(["raw", "raw_jira_sprints"]),
    ],
    description=(
        "Inserts skeleton rows into the Sprint Planning sheet for the immediately following "
        "sprint per board (first future sprint by start date). "
        "Only adds rows not already present — existing edits are never overwritten. "
        "planned_points is pre-filled from story_points (customfield_10105), falling back "
        "to t_shirt_size (customfield_10834). subteam is derived from issue labels via "
        "subteam_labels, with board default_subteam as fallback."
    ),
)
def gsheets_sprint_planning_prepopulator(context, gsheets: GSheetsResource) -> None:
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")
    con = _db(db_path)

    # Per board: find the first future sprint (earliest start_date)
    target_sprints: dict[int, tuple] = {}
    for board_id in _BOARD_IDS:
        row = con.execute(
            """
            SELECT sprint_id, sprint_name, start_date
            FROM raw.raw_jira_sprints
            WHERE board_id = ? AND state = 'future' AND start_date IS NOT NULL
            ORDER BY start_date ASC
            LIMIT 1
            """,
            [board_id],
        ).fetchone()
        if row:
            target_sprints[board_id] = row  # (sprint_id, sprint_name, start_date)

    if not target_sprints:
        context.log.info("No future sprints found; nothing to pre-populate.")
        con.close()
        return

    # board_id → board_key
    try:
        board_map: dict[int, str] = {
            row[0]: row[1]
            for row in con.execute("SELECT board_id, board_key FROM raw.board_config").fetchall()
        }
    except Exception:
        board_map = {74: "DELI", 212: "NFR"}

    # label → subteam (first label match wins, consistent with stg_jira_issues)
    try:
        subteam_label_map: dict[str, str] = {
            row[0]: row[1]
            for row in con.execute("SELECT label, subteam FROM raw.subteam_labels").fetchall()
        }
    except Exception:
        import csv, pathlib
        seed_path = pathlib.Path(__file__).parents[2] / "dbt_project" / "seeds" / "subteam_labels.csv"
        with open(seed_path) as f:
            subteam_label_map = {r["label"]: r["subteam"] for r in csv.DictReader(f)}

    # board_key → default subteam fallback
    try:
        default_subteam_map: dict[str, str] = {
            row[0]: row[1]
            for row in con.execute(
                "SELECT board_key, default_subteam FROM raw.board_config "
                "WHERE default_subteam IS NOT NULL AND default_subteam != ''"
            ).fetchall()
        }
    except Exception:
        default_subteam_map = {"NFR": "Infra"}

    # Fetch issues for each target sprint
    all_issues: list[tuple] = []
    for board_id, (sprint_id, sprint_name, _) in target_sprints.items():
        rows = con.execute(
            """
            SELECT issue_key, summary, story_points, t_shirt_size, status, labels
            FROM raw.raw_jira_issues
            WHERE sprint_id = ? AND board_id = ?
              AND status_category != 'cancelled'
            """,
            [sprint_id, board_id],
        ).fetchall()
        for row in rows:
            all_issues.append((board_id, sprint_name) + row)

    con.close()

    if not all_issues:
        context.log.info("No issues found for target sprints; nothing to pre-populate.")
        return

    def _derive_subteam(labels_json: str | None, board_key: str) -> str | None:
        if labels_json:
            try:
                for label in json.loads(labels_json):
                    if label in subteam_label_map:
                        return subteam_label_map[label]
            except Exception:
                pass
        return default_subteam_map.get(board_key)

    # Read existing rows to avoid duplicates
    existing = gsheets.read_input_sheet(SHEET_SPRINT_PLANNING)
    existing_keys: set[tuple] = {
        (r.get("issue_key"), r.get("sprint_name"))
        for r in existing
    }

    if not existing:
        gsheets.overwrite_sheet(
            gsheets.input_spreadsheet_id,
            SHEET_SPRINT_PLANNING,
            [["issue_key", "sprint_name", "board", "subteam", "summary", "status", "planned_points", "include"]],
        )

    jira_base_url = os.environ.get("JIRA_BASE_URL", "").rstrip("/")
    new_rows: list[list] = []
    for board_id, sprint_name, issue_key, summary, story_points, t_shirt_size, status, labels in all_issues:
        if (issue_key, sprint_name) in existing_keys:
            continue
        board_key = board_map.get(board_id, str(board_id))
        subteam = _derive_subteam(labels, board_key) or ""
        planned_points = story_points if story_points is not None else t_shirt_size
        issue_link = f'=HYPERLINK("{jira_base_url}/browse/{issue_key}", "{issue_key}")'
        new_rows.append([
            issue_link, sprint_name, board_key, subteam, summary or "",
            status or "", planned_points if planned_points is not None else "", "TRUE",
        ])
        existing_keys.add((issue_key, sprint_name))

    if new_rows:
        context.log.info(f"Appending {len(new_rows)} issue rows to '{SHEET_SPRINT_PLANNING}'")
        gsheets.append_rows_to_input_sheet(SHEET_SPRINT_PLANNING, new_rows)
    else:
        context.log.info("All issues for target sprints already present in sheet.")


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_sprint_planning
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_sprint_planning",
    key_prefix=["raw"],
    group_name="raw_gsheets",
    deps=["gsheets_sprint_planning_prepopulator"],
    description="Per-sprint issue planning data read from the Sprint Planning Google Sheet tab.",
)
def raw_sprint_planning(context, gsheets: GSheetsResource) -> None:
    records = gsheets.read_input_sheet(SHEET_SPRINT_PLANNING)
    context.log.info(f"Read {len(records)} sprint planning records")

    con = _db()
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_sprint_planning (
            issue_key      VARCHAR,
            sprint_name    VARCHAR,
            board          VARCHAR,
            subteam        VARCHAR,
            summary        VARCHAR,
            status         VARCHAR,
            planned_points VARCHAR,
            include        VARCHAR,
            _extracted_at  VARCHAR
        )
    """)
    now = datetime.utcnow().isoformat()
    con.executemany(
        "INSERT INTO raw.raw_sprint_planning VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [
                r.get("issue_key"), r.get("sprint_name"), r.get("board"), r.get("subteam"),
                r.get("summary"), r.get("status"), r.get("planned_points"),
                r.get("include"), now,
            ]
            for r in records
        ],
    )
    context.log.info("Loaded raw_sprint_planning into DuckDB")
    con.close()
