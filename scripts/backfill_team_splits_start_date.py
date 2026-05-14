"""One-time backfill: populate sprint_start_date in the Team Splits Google Sheet tab.

For every row whose sprint_start_date is blank, looks up the sprint's start_date
from raw.raw_jira_sprints in DuckDB and fills it in (YYYY-MM-DD only).
Rows that already have a value are left untouched.

Usage:
    python scripts/backfill_team_splits_start_date.py
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

SHEET_TEAM_SPLITS = "Team Splits"
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def main() -> None:
    db_path = os.environ["DUCKDB_PATH"]
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    spreadsheet_id = os.environ["GSHEETS_INPUT_SPREADSHEET_ID"]

    # ── 1. Build sprint_name → start_date (YYYY-MM-DD) map from DuckDB ──
    con = duckdb.connect(db_path, read_only=True)
    sprint_date_map: dict[str, str] = {
        row[0]: row[1][:10]
        for row in con.execute(
            "SELECT sprint_name, start_date FROM raw.raw_jira_sprints WHERE start_date IS NOT NULL"
        ).fetchall()
        if row[1]
    }
    con.close()
    print(f"Loaded {len(sprint_date_map)} sprint → date mappings from DuckDB")

    # ── 2. Read Team Splits sheet (raw values, preserving blanks) ──
    creds = Credentials.from_service_account_file(credentials_path, scopes=_SCOPES)
    client = gspread.authorize(creds)
    wb = client.open_by_key(spreadsheet_id)
    ws = wb.worksheet(SHEET_TEAM_SPLITS)

    all_values: list[list[str]] = ws.get_all_values()
    if not all_values:
        print("Sheet is empty — nothing to backfill.")
        return

    header = all_values[0]
    print(f"Header: {header}")

    try:
        sprint_name_col = header.index("sprint_name")
        start_date_col = header.index("sprint_start_date")
    except ValueError as exc:
        sys.exit(f"Expected column not found in header: {exc}")

    # ── 3. Fill in missing sprint_start_date values ──
    updated = 0
    for row in all_values[1:]:
        # Pad short rows to cover the start_date column
        while len(row) <= start_date_col:
            row.append("")

        if row[start_date_col]:
            continue  # already has a value

        sprint_name = row[sprint_name_col]
        start_date = sprint_date_map.get(sprint_name)
        if start_date:
            row[start_date_col] = start_date
            updated += 1
        else:
            print(f"  WARNING: no start_date found for sprint '{sprint_name}'")

    print(f"Backfilled {updated} rows")

    if updated == 0:
        print("Nothing changed — sheet already fully populated.")
        return

    # ── 4. Write back the full sheet ──
    ws.clear()
    ws.update(all_values, value_input_option="USER_ENTERED")
    print(f"Sheet '{SHEET_TEAM_SPLITS}' updated successfully.")


if __name__ == "__main__":
    main()
