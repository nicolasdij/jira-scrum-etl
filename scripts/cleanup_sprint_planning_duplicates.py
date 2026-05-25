"""One-time cleanup: remove duplicate rows from the Sprint Planning Google Sheet tab.

A duplicate is two or more rows sharing the same (issue_key, sprint_name). The
first occurrence is kept; all later occurrences are deleted. The prepopulator
now applies the same rule on every run, so this script exists to clean up
historical duplicates that predate that change.

Usage:
    uv run python scripts/cleanup_sprint_planning_duplicates.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parents[1]))

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

SHEET_SPRINT_PLANNING = "Sprint Planning"
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def main() -> None:
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    spreadsheet_id = os.environ["GSHEETS_INPUT_SPREADSHEET_ID"]

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
    duplicate_rows: list[int] = []  # 1-based sheet row numbers
    for idx, row in enumerate(all_values[1:], start=2):
        if len(row) <= max(issue_key_col, sprint_name_col):
            continue
        key = (row[issue_key_col], row[sprint_name_col])
        if not key[0]:
            continue
        if key in seen:
            duplicate_rows.append(idx)
            print(f"  duplicate at row {idx}: {key}")
        else:
            seen.add(key)

    if not duplicate_rows:
        print("No duplicates found.")
        return

    print(f"Deleting {len(duplicate_rows)} duplicate row(s)...")
    # Delete from the bottom up so earlier deletions don't shift later row numbers.
    for sheet_row in sorted(duplicate_rows, reverse=True):
        ws.delete_rows(sheet_row)
    print("Done.")


if __name__ == "__main__":
    main()
