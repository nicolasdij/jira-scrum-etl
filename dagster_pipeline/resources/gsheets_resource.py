"""Dagster resource for Google Sheets (read and write).

Authentication: GCP service account JSON key file.
Set GOOGLE_APPLICATION_CREDENTIALS to the path of the key file.
"""

from __future__ import annotations

from typing import Any

import gspread
from dagster import ConfigurableResource, get_dagster_logger
from google.oauth2.service_account import Credentials
from pydantic import Field

# Scopes required for reading and writing Sheets
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


class GSheetsResource(ConfigurableResource):
    """Wrapper around gspread for reading from and writing to Google Sheets.

    Configure via environment variables (see .env.example):
        GOOGLE_APPLICATION_CREDENTIALS  path to GCP service account JSON key
        GSHEETS_INPUT_SPREADSHEET_ID    spreadsheet containing input tabs
        GSHEETS_OUTPUT_SPREADSHEET_ID   spreadsheet to write results into
    """

    credentials_path: str = Field(description="Path to GCP service account JSON key file")
    input_spreadsheet_id: str = Field(description="Spreadsheet ID for input data")
    output_spreadsheet_id: str = Field(description="Spreadsheet ID for output data")

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _client(self) -> gspread.Client:
        creds = Credentials.from_service_account_file(self.credentials_path, scopes=_SCOPES)
        return gspread.authorize(creds)

    # ------------------------------------------------------------------ #
    # Read helpers                                                         #
    # ------------------------------------------------------------------ #

    def read_sheet_as_records(self, spreadsheet_id: str, sheet_name: str) -> list[dict[str, Any]]:
        """Read all rows of a named sheet as a list of dicts (first row = header).

        Uses get_all_values() rather than get_all_records() to tolerate sheets that
        have trailing empty columns — gspread raises on duplicate blank header names.
        Only columns with a non-empty header are included in the returned dicts.
        """
        log = get_dagster_logger()
        client = self._client()
        wb = client.open_by_key(spreadsheet_id)
        ws = wb.worksheet(sheet_name)
        all_values = ws.get_all_values()
        if not all_values:
            log.info(f"Read 0 rows from '{sheet_name}' in {spreadsheet_id}")
            return []
        header = all_values[0]
        valid = [(i, h) for i, h in enumerate(header) if h.strip()]
        records = [
            {h: (row[i] if i < len(row) else "") for i, h in valid}
            for row in all_values[1:]
        ]
        log.info(f"Read {len(records)} rows from '{sheet_name}' in {spreadsheet_id}")
        return records

    def read_input_sheet(self, sheet_name: str) -> list[dict[str, Any]]:
        return self.read_sheet_as_records(self.input_spreadsheet_id, sheet_name)

    # ------------------------------------------------------------------ #
    # Write helpers                                                        #
    # ------------------------------------------------------------------ #

    def overwrite_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        rows: list[list[Any]],
        *,
        create_if_missing: bool = True,
    ) -> None:
        """Replace the contents of a named sheet with *rows* (first row = header)."""
        log = get_dagster_logger()
        client = self._client()
        wb = client.open_by_key(spreadsheet_id)
        try:
            ws = wb.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            if create_if_missing:
                ws = wb.add_worksheet(title=sheet_name, rows=1, cols=1)
            else:
                raise
        ws.clear()
        if rows:
            ws.update(rows, value_input_option="USER_ENTERED")
        log.info(f"Wrote {len(rows)} rows to '{sheet_name}' in {spreadsheet_id}")

    def overwrite_output_sheet(self, sheet_name: str, rows: list[list[Any]]) -> None:
        self.overwrite_sheet(self.output_spreadsheet_id, sheet_name, rows)

    # ------------------------------------------------------------------ #
    # Targeted upsert for team splits pre-population                      #
    # ------------------------------------------------------------------ #

    def append_rows_to_input_sheet(self, sheet_name: str, rows: list[list[Any]]) -> None:
        """Append *rows* to the end of a sheet without clearing existing data."""
        log = get_dagster_logger()
        if not rows:
            log.info(f"No new rows to append to '{sheet_name}'")
            return
        client = self._client()
        wb = client.open_by_key(self.input_spreadsheet_id)
        try:
            ws = wb.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            ws = wb.add_worksheet(title=sheet_name, rows=1, cols=1)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log.info(f"Appended {len(rows)} rows to '{sheet_name}' in {self.input_spreadsheet_id}")
