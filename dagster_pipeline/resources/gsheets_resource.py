"""Dagster resource for Google Sheets (read and write).

Authentication: GCP service account JSON key file.
Set GOOGLE_APPLICATION_CREDENTIALS to the path of the key file.
"""

from __future__ import annotations

import functools
from typing import Any

import gspread
from dagster import ConfigurableResource, get_dagster_logger
from google.oauth2.service_account import Credentials
from gspread.utils import a1_to_rowcol
from pydantic import Field

# Scopes required for reading and writing Sheets
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


@functools.lru_cache(maxsize=4)
def _build_client(credentials_path: str) -> gspread.Client:
    """Authorise a gspread client once per (process, credentials_path).

    Each call to gspread.authorize re-reads the service-account JSON and runs
    an OAuth handshake — ~0.5–1 s of pure overhead. The sprint-planning sync
    op alone fires 5 sheet operations back-to-back, so re-authorising each
    time was costing ~3–5 s before any data moved. Caching is per-process,
    which matches Dagster's one-subprocess-per-run model: a single run reuses
    one authorised session across all its gsheets calls.
    """
    creds = Credentials.from_service_account_file(credentials_path, scopes=_SCOPES)
    return gspread.authorize(creds)


def _parse_a1(a1: str) -> tuple[int, int]:
    """Parse worksheet-scoped A1 ('G5') into (row_index, col_index), both 0-based.

    Delegates to gspread.utils.a1_to_rowcol (which returns 1-based coords and
    validates the input via a regex) and shifts to 0-based for the Sheets API.
    """
    row, col = a1_to_rowcol(a1)
    return row - 1, col - 1


def _to_user_entered_value(val: Any) -> dict:
    """Convert a Python value to a Sheets userEnteredValue dict.

    Mimics value_input_option='USER_ENTERED': strings starting with '=' become
    formulas, 'TRUE'/'FALSE' become booleans, numeric strings become numbers.
    Returns an empty dict for None or '' so callers can omit the field on
    blank cells. bool is checked before int because in Python bool subclasses int.
    """
    if val is None or val == "":
        return {}
    if isinstance(val, bool):
        return {"boolValue": val}
    if isinstance(val, (int, float)):
        return {"numberValue": float(val)}
    s = str(val)
    if s.startswith("="):
        return {"formulaValue": s}
    if s.upper() == "TRUE":
        return {"boolValue": True}
    if s.upper() == "FALSE":
        return {"boolValue": False}
    try:
        return {"numberValue": float(s)}
    except ValueError:
        return {"stringValue": s}


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
        return _build_client(self.credentials_path)

    # ------------------------------------------------------------------ #
    # Read helpers                                                         #
    # ------------------------------------------------------------------ #

    def read_sheet_as_records(self, spreadsheet_id: str, sheet_name: str) -> list[dict[str, Any]]:
        """Read all rows of a named sheet as a list of dicts (first row = header).

        Uses get_all_values() rather than get_all_records() to tolerate sheets that
        have trailing empty columns — gspread raises on duplicate blank header names.
        Stops at the first empty-header column, which is treated as a divider between
        the data section and any reference/summary table on the right (e.g. Sprint
        Planning has its data in cols A-H and a per-subteam summary in cols J-P,
        separated by an empty col I).
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
        end = next((i for i, h in enumerate(header) if not h.strip()), len(header))
        valid = list(enumerate(header[:end]))
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

    def batch_modify_input_sheet(
        self,
        sheet_name: str,
        *,
        cell_updates: list[dict[str, Any]] | None = None,
        delete_row_indices: list[int] | None = None,
        append_rows: list[list[Any]] | None = None,
        sort_specs: list[tuple[int, str]] | None = None,
    ) -> None:
        """Apply scattered cell updates, row deletes, row appends, and a sort to
        one sheet in a single spreadsheets.batchUpdate call.

        Replaces what used to be four separate gspread calls (each paying client
        + open_by_key + worksheet round-trips) with one HTTP request. Operations
        run server-side in this fixed order:
            1. cell_updates      — use pre-deletion row numbers
            2. delete_row_indices — applied descending so each delete doesn't
                                    shift the numbering of later deletes
            3. append_rows       — added after the last data row
            4. sort_specs        — reorders the final grid state

        cell_updates: list of {"range": "G5", "value": ...} dicts (worksheet-scoped A1).
        delete_row_indices: 1-based row numbers; row 1 (header) is filtered out.
        append_rows: rows to add at the end; values follow USER_ENTERED parsing
            (formulas, booleans, numeric strings auto-detected).
        sort_specs: list of (1-based column, 'asc'|'desc') tuples; applied as
            primary/secondary/tertiary keys.
        """
        log = get_dagster_logger()
        cell_updates = cell_updates or []
        delete_row_indices = delete_row_indices or []
        append_rows = append_rows or []
        sort_specs = sort_specs or []

        if not any((cell_updates, delete_row_indices, append_rows, sort_specs)):
            log.info(f"No changes to apply to '{sheet_name}'")
            return

        client = self._client()
        wb = client.open_by_key(self.input_spreadsheet_id)
        ws = wb.worksheet(sheet_name)
        sheet_id = ws.id

        requests: list[dict[str, Any]] = []

        for u in cell_updates:
            uev = _to_user_entered_value(u["value"])
            if not uev:
                continue
            row_idx, col_idx = _parse_a1(u["range"])
            requests.append({
                "updateCells": {
                    "rows": [{"values": [{"userEnteredValue": uev}]}],
                    "fields": "userEnteredValue",
                    "start": {
                        "sheetId": sheet_id,
                        "rowIndex": row_idx,
                        "columnIndex": col_idx,
                    },
                }
            })

        unique_deletes = sorted({i for i in delete_row_indices if i > 1}, reverse=True)
        for row_num in unique_deletes:
            requests.append({
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_num - 1,
                        "endIndex": row_num,
                    }
                }
            })

        if append_rows:
            rows_payload = []
            for row in append_rows:
                cells = []
                for val in row:
                    uev = _to_user_entered_value(val)
                    cells.append({"userEnteredValue": uev} if uev else {})
                rows_payload.append({"values": cells})
            requests.append({
                "appendCells": {
                    "sheetId": sheet_id,
                    "rows": rows_payload,
                    "fields": "userEnteredValue",
                }
            })

        if sort_specs:
            # endRowIndex / endColumnIndex omitted → extends through the grid,
            # so this naturally covers rows added by the preceding appendCells.
            requests.append({
                "sortRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 0,
                    },
                    "sortSpecs": [
                        {
                            "dimensionIndex": col - 1,
                            "sortOrder": (
                                "ASCENDING"
                                if str(order).lower().startswith("asc")
                                else "DESCENDING"
                            ),
                        }
                        for col, order in sort_specs
                    ],
                }
            })

        wb.batch_update({"requests": requests})
        log.info(
            f"Batched edit on '{sheet_name}': "
            f"{len(cell_updates)} cell update(s), "
            f"{len(unique_deletes)} row delete(s), "
            f"{len(append_rows)} row append(s), "
            f"{'1 sort' if sort_specs else 'no sort'}"
        )
