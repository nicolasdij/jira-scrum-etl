"""One-time cleanup: remove duplicate, blank, and past-target rows from the Sprint Planning sheet.

Three row types are removed:
  1. Duplicates — rows sharing the same (issue_key, sprint_name) as an earlier row.
     The first occurrence is kept; later occurrences are deleted.
  2. Blanks — rows whose issue_key cell is empty. These typically come from manual
     paste or interrupted writes and would otherwise collide in dedup logic.
  3. Past-target rows — rows whose sprint_name corresponds to a Jira sprint that
     is no longer in state='future' (i.e. has transitioned to active or closed).
     These linger from when their sprint was the current planning target.

The prepopulator already applies the same rules on every run, so this script exists
to clean up historical noise that predates those fixes.

Usage:
    uv run python scripts/cleanup_sprint_planning_duplicates.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parents[1]))

import duckdb
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

SHEET_SPRINT_PLANNING = "Sprint Items"
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def main() -> None:
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    spreadsheet_id = os.environ["GSHEETS_INPUT_SPREADSHEET_ID"]
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    # Sprint names that exist in Jira but are no longer in state='future' →
    # any sheet rows tagged with these names are past-target leftovers.
    con = duckdb.connect(db_path, read_only=True)
    past_target_sprint_names: set[str] = {
        row[0]
        for row in con.execute(
            "SELECT sprint_name FROM raw.raw_jira_sprints WHERE state != 'future'"
        ).fetchall()
        if row[0]
    }
    con.close()
    print(f"Loaded {len(past_target_sprint_names)} past-target sprint name(s) from DuckDB")

    creds = Credentials.from_service_account_file(credentials_path, scopes=_SCOPES)
    client = gspread.authorize(creds)
    wb = client.open_by_key(spreadsheet_id)
    ws = wb.worksheet(SHEET_SPRINT_PLANNING)

    all_values: list[list[str]] = ws.get_all_values()
    if not all_values:
        print("Sheet is empty — nothing to clean up.")
        return

    header = all_values[0]
    try:
        issue_key_col = header.index("issue_key")
        sprint_name_col = header.index("sprint_name")
    except ValueError as exc:
        sys.exit(f"Expected column not found in header: {exc}")

    seen: set[tuple[str, str]] = set()
    blank_rows: list[int] = []
    duplicate_rows: list[int] = []
    past_target_rows: list[int] = []
    for idx, row in enumerate(all_values[1:], start=2):
        issue_key = row[issue_key_col].strip() if issue_key_col < len(row) else ""
        if not issue_key:
            blank_rows.append(idx)
            continue
        sprint_name = row[sprint_name_col] if sprint_name_col < len(row) else ""
        if sprint_name in past_target_sprint_names:
            past_target_rows.append(idx)
            print(f"  past-target at row {idx}: {(issue_key, sprint_name)}")
            continue
        key = (issue_key, sprint_name)
        if key in seen:
            duplicate_rows.append(idx)
            print(f"  duplicate at row {idx}: {key}")
        else:
            seen.add(key)

    to_delete = sorted(set(blank_rows + duplicate_rows + past_target_rows))
    if not to_delete:
        print("No duplicate, blank, or past-target rows found.")
        return

    # Group consecutive row numbers into runs so a long blank tail can be deleted
    # with a single range API call instead of one call per row.
    runs: list[tuple[int, int]] = []  # list of (start, end) inclusive
    run_start = to_delete[0]
    prev = to_delete[0]
    for n in to_delete[1:]:
        if n == prev + 1:
            prev = n
        else:
            runs.append((run_start, prev))
            run_start = n
            prev = n
    runs.append((run_start, prev))

    print(
        f"Deleting {len(to_delete)} row(s) in {len(runs)} run(s): "
        f"{len(blank_rows)} blank + {len(duplicate_rows)} duplicate "
        f"+ {len(past_target_rows)} past-target"
    )
    # Process runs from highest to lowest so earlier deletions don't shift later
    # row numbers. Within a run, gspread's delete_rows(start, end) takes one API
    # call regardless of run length.
    deleted = 0
    skipped = 0
    for start, end in sorted(runs, reverse=True):
        try:
            ws.delete_rows(start, end)
            deleted += end - start + 1
        except gspread.exceptions.APIError as exc:
            skipped += end - start + 1
            print(f"  skipped run {start}-{end}: {exc}")
    print(f"Done. Deleted {deleted} row(s); skipped {skipped}.")


if __name__ == "__main__":
    main()
