"""Dagster assets for extracting raw data from the Jira Agile API.

Produces two raw tables in DuckDB:
    raw.raw_jira_sprints
    raw.raw_jira_issues
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import duckdb
from dagster import asset

from dagster_pipeline.resources.jira_resource import JiraResource

# Board IDs from CLAUDE.md
BOARD_IDS = [74, 212]  # DELI=74, NFR=212

# Regex to extract key=value pairs from the Jira sprint descriptor string
# e.g. com.atlassian...[ id=42, name=Sprint 34, state=CLOSED, startDate=..., endDate=... ]
_SPRINT_DESCRIPTOR_RE = re.compile(
    r"id=(?P<id>\d+)"
    r".*?name=(?P<name>[^,\]]+)"
    r".*?state=(?P<state>[^,\]]+)"
    r"(?:.*?startDate=(?P<startDate>[^,\]]+))?"
    r"(?:.*?endDate=(?P<endDate>[^,\]]+))?",
    re.DOTALL,
)


def _parse_sprint_descriptor(descriptor: str) -> dict[str, str | None]:
    """Parse a Jira sprint descriptor string into a plain dict.

    Jira returns customfield_10103 as a list of strings like:
        com.atlassian.greenhopper...[ id=42, name=Sprint 34, state=CLOSED, ... ]
    We extract the last entry since an issue can have been in multiple sprints.
    """
    m = _SPRINT_DESCRIPTOR_RE.search(descriptor)
    if not m:
        return {"id": None, "name": None, "state": None, "startDate": None, "endDate": None}
    return {
        "id": m.group("id"),
        "name": m.group("name").strip() if m.group("name") else None,
        "state": m.group("state").strip() if m.group("state") else None,
        "startDate": m.group("startDate").strip() if m.group("startDate") else None,
        "endDate": m.group("endDate").strip() if m.group("endDate") else None,
    }


def _resolve_sprint_from_field(field_value: Any) -> dict[str, str | None]:
    """Extract sprint info from customfield_10103 (list of descriptor strings or dicts)."""
    if not field_value:
        return {"id": None, "name": None, "state": None, "startDate": None, "endDate": None}
    last = field_value[-1]
    # Newer Jira versions may return dicts directly
    if isinstance(last, dict):
        return {
            "id": str(last.get("id", "")),
            "name": last.get("name"),
            "state": last.get("state"),
            "startDate": last.get("startDate"),
            "endDate": last.get("endDate"),
        }
    return _parse_sprint_descriptor(str(last))


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_jira_sprints
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_jira_sprints",
    group_name="raw_jira",
    description="All sprints for DELI (board 74) and NFR (board 212) boards.",
)
def raw_jira_sprints(context, jira: JiraResource) -> None:
    import os
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    rows: list[dict] = []
    for board_id in BOARD_IDS:
        context.log.info(f"Fetching sprints for board {board_id}")
        sprints = jira.get_sprints_for_board(board_id)
        for s in sprints:
            rows.append({
                "sprint_id": s.get("id"),
                "board_id": board_id,
                "sprint_name": s.get("name"),
                "state": s.get("state"),
                "start_date": s.get("startDate"),
                "end_date": s.get("endDate"),
                "complete_date": s.get("completeDate"),
                "_extracted_at": datetime.utcnow().isoformat(),
            })

    context.log.info(f"Total sprints fetched: {len(rows)}")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_jira_sprints (
            sprint_id       INTEGER,
            board_id        INTEGER,
            sprint_name     VARCHAR,
            state           VARCHAR,
            start_date      VARCHAR,
            end_date        VARCHAR,
            complete_date   VARCHAR,
            _extracted_at   VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO raw.raw_jira_sprints VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [
                r["sprint_id"], r["board_id"], r["sprint_name"], r["state"],
                r["start_date"], r["end_date"], r["complete_date"], r["_extracted_at"],
            ]
            for r in rows
        ],
    )
    context.log.info(f"Loaded {len(rows)} sprint rows into raw.raw_jira_sprints")
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_jira_issues
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_jira_issues",
    group_name="raw_jira",
    deps=["raw_jira_sprints"],
    description="All issues for all sprints, including story points, status, labels, and sprint ref.",
)
def raw_jira_issues(context, jira: JiraResource) -> None:
    import os
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    # Read sprint IDs from DuckDB (populated by raw_jira_sprints)
    con = duckdb.connect(db_path)
    sprint_rows = con.execute(
        "SELECT sprint_id, board_id, sprint_name FROM raw.raw_jira_sprints"
    ).fetchall()
    con.close()

    rows: list[dict] = []
    for sprint_id, board_id, sprint_name in sprint_rows:
        context.log.info(f"Fetching issues for sprint {sprint_id} ({sprint_name})")
        issues = jira.get_issues_for_sprint(sprint_id)
        for issue in issues:
            fields = issue.get("fields", {})

            # Story points
            story_points = fields.get("customfield_10105")

            # Status category
            status_obj = fields.get("status") or {}
            status_category = (
                (status_obj.get("statusCategory") or {}).get("key") or ""
            ).lower()

            # Labels — keep as JSON string; first sub-team label wins (resolved in staging)
            labels = fields.get("labels") or []

            # Sprint ref from customfield_10103
            sprint_ref = _resolve_sprint_from_field(fields.get("customfield_10103"))

            # Assignee
            assignee_obj = fields.get("assignee") or {}
            assignee = assignee_obj.get("displayName")

            rows.append({
                "issue_key": issue.get("key"),
                "sprint_id": sprint_id,
                "board_id": board_id,
                "summary": fields.get("summary"),
                "story_points": story_points,
                "status_category": status_category,
                "labels": json.dumps(labels),
                "assignee": assignee,
                "cf_sprint_id": sprint_ref["id"],
                "cf_sprint_name": sprint_ref["name"],
                "cf_sprint_state": sprint_ref["state"],
                "_extracted_at": datetime.utcnow().isoformat(),
            })

    context.log.info(f"Total issues fetched: {len(rows)}")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.raw_jira_issues (
            issue_key       VARCHAR,
            sprint_id       INTEGER,
            board_id        INTEGER,
            summary         VARCHAR,
            story_points    DOUBLE,
            status_category VARCHAR,
            labels          VARCHAR,
            assignee        VARCHAR,
            cf_sprint_id    VARCHAR,
            cf_sprint_name  VARCHAR,
            cf_sprint_state VARCHAR,
            _extracted_at   VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO raw.raw_jira_issues VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [
                r["issue_key"], r["sprint_id"], r["board_id"], r["summary"],
                r["story_points"], r["status_category"], r["labels"],
                r["assignee"], r["cf_sprint_id"], r["cf_sprint_name"],
                r["cf_sprint_state"], r["_extracted_at"],
            ]
            for r in rows
        ],
    )
    context.log.info(f"Loaded {len(rows)} issue rows into raw.raw_jira_issues")
    con.close()
