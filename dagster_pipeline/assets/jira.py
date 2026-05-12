"""Dagster assets for extracting raw data from the Jira Agile API.

Produces two raw tables in DuckDB:
    raw.raw_jira_sprints
    raw.raw_jira_issues
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from typing import Any

import duckdb
from dagster import AssetKey, DynamicPartitionsDefinition, asset
from dagster_pipeline.resources.jira_resource import JiraResource

sprint_partitions = DynamicPartitionsDefinition(name="sprints")


def _connect_duckdb(db_path: str, retries: int = 8, delay: float = 5.0) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB write connection, retrying on file-lock conflicts.

    Concurrent partition runs race for the same DuckDB file. Rather than
    failing immediately, back off and retry to let the conflicting write finish.
    """
    for attempt in range(retries):
        try:
            return duckdb.connect(db_path)
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or attempt == retries - 1:
                raise
            time.sleep(delay)
    raise AssertionError("unreachable")


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
    """Extract sprint info from customfield_10103 (list of descriptor strings or dicts).

    An issue that spans sprints appears in multiple entries. We sort by startDate
    descending and pick the first, so the sprint the issue was completed in wins.
    """
    if not field_value:
        return {"id": None, "name": None, "state": None, "startDate": None, "endDate": None}
    parsed = []
    for entry in field_value:
        if isinstance(entry, dict):
            parsed.append({
                "id": str(entry.get("id", "")),
                "name": entry.get("name"),
                "state": entry.get("state"),
                "startDate": entry.get("startDate"),
                "endDate": entry.get("endDate"),
            })
        else:
            parsed.append(_parse_sprint_descriptor(str(entry)))
    # Entries with no startDate sort last (empty string < any ISO date string).
    parsed.sort(key=lambda d: d.get("startDate") or "", reverse=True)
    return parsed[0]


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_jira_sprints
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_jira_sprints",
    key_prefix=["raw"],
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

    con = _connect_duckdb(db_path)
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

    # Register any new sprint names as dynamic partition keys so raw_jira_issues
    # can be materialised per sprint.
    # Deduplicate with a set — both boards can share sprint names, so the list may
    # contain the same name twice, which would cause a UNIQUE constraint failure.
    new_ids = {r["sprint_name"] for r in rows if r["sprint_name"] is not None}
    existing = set(context.instance.get_dynamic_partitions("sprints"))
    to_add = list(new_ids - existing)
    if to_add:
        context.instance.add_dynamic_partitions("sprints", to_add)
        context.log.info(f"Registered {len(to_add)} new sprint partition(s): {to_add}")


# ─────────────────────────────────────────────────────────────────────────────
# Asset: raw_jira_issues
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="raw_jira_issues",
    key_prefix=["raw"],
    group_name="raw_jira",
    partitions_def=sprint_partitions,
    deps=[AssetKey(["raw", "raw_jira_sprints"])],
    op_tags={"dagster/concurrency_key": "duckdb"},
    description=(
        "Issues for a single sprint partition. Closed-sprint partitions are skipped when "
        "rows already exist in DuckDB — active and future sprints are always refreshed."
    ),
)
def raw_jira_issues(context, jira: JiraResource) -> None:
    import os
    if not context.has_partition_key:
        context.log.error(
            "raw_jira_issues must be run with a sprint partition key. "
            "Trigger via raw_jira_issues_partition_sensor or select a partition in the UI."
        )
        return
    sprint_name = context.partition_key
    db_path = os.environ.get("DUCKDB_PATH", "data/scrum.duckdb")

    con = _connect_duckdb(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Initialise the table once; never wipe it — other partitions' rows must be preserved.
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_jira_issues (
            issue_key       VARCHAR,
            sprint_id       INTEGER,
            board_id        INTEGER,
            summary         VARCHAR,
            story_points    DOUBLE,
            t_shirt_size    DOUBLE,
            status_category VARCHAR,
            labels          VARCHAR,
            assignee        VARCHAR,
            cf_sprint_id    VARCHAR,
            cf_sprint_name  VARCHAR,
            cf_sprint_state VARCHAR,
            _extracted_at   VARCHAR
        )
    """)
    sprint_row = con.execute(
        "SELECT sprint_id, board_id, state FROM raw.raw_jira_sprints WHERE sprint_name = ?",
        [sprint_name],
    ).fetchone()

    if sprint_row is None:
        context.log.warning(f"Sprint '{sprint_name}' not found in raw_jira_sprints; skipping.")
        con.close()
        return

    sprint_id, board_id, state = sprint_row

    # Closed sprints are immutable — skip the Jira fetch if we already have their issues,
    # UNLESS this is the most recently closed sprint for its board. That sprint may still
    # have issues resolved after the last materialisation but before the sprint closed.
    if state == "closed":
        existing_row = con.execute(
            "SELECT COUNT(*) FROM raw.raw_jira_issues WHERE sprint_id = ?", [sprint_id]
        ).fetchone()
        existing = existing_row[0] if existing_row is not None else 0
        if existing > 0:
            last_closed_row = con.execute(
                """
                SELECT sprint_name FROM raw.raw_jira_sprints
                WHERE board_id = ? AND state = 'closed'
                ORDER BY complete_date DESC NULLS LAST
                LIMIT 1
                """,
                [board_id],
            ).fetchone()
            is_last_closed = last_closed_row is not None and last_closed_row[0] == sprint_name
            if not is_last_closed:
                context.log.info(
                    f"Sprint {sprint_id} ({sprint_name}) is closed and already has "
                    f"{existing} issues in DuckDB — skipping fetch."
                )
                con.close()
                return
            context.log.info(
                f"Sprint {sprint_id} ({sprint_name}) is the last closed sprint for board "
                f"{board_id} — re-fetching to capture any late resolutions."
            )

    con.close()

    context.log.info(f"Fetching issues for sprint {sprint_id} ({sprint_name}, state={state})")
    issues = jira.get_issues_for_sprint(sprint_id)

    rows: list[dict] = []
    for issue in issues:
        fields = issue.get("fields", {})
        story_points = fields.get("customfield_10105")
        t_shirt_size = fields.get("customfield_10834")
        status_obj = fields.get("status") or {}
        status_category = (
            (status_obj.get("statusCategory") or {}).get("key") or ""
        ).lower()
        # Jira maps "Cancelled" to statusCategory "done", but cancelled issues must not
        # count toward velocity or burndown — override to a distinct value.
        if (status_obj.get("name") or "").lower() == "cancelled":
            status_category = "cancelled"
        labels = fields.get("labels") or []
        sprint_ref = _resolve_sprint_from_field(fields.get("customfield_10103"))
        assignee_obj = fields.get("assignee") or {}
        rows.append({
            "issue_key": issue.get("key"),
            # Use the canonical sprint (latest startDate) rather than the partition key so
            # issues that were started in one sprint but finished in the next are attributed
            # to the sprint they were completed in.
            "sprint_id": int(sprint_ref["id"]) if sprint_ref.get("id") else sprint_id,
            "board_id": board_id,
            "summary": fields.get("summary"),
            "story_points": story_points,
            "t_shirt_size": t_shirt_size,
            "status_category": status_category,
            "labels": json.dumps(labels),
            "assignee": assignee_obj.get("displayName"),
            "cf_sprint_id": sprint_ref["id"],
            "cf_sprint_name": sprint_ref["name"],
            "cf_sprint_state": sprint_ref["state"],
            "_extracted_at": datetime.utcnow().isoformat(),
        })

    context.log.info(f"Fetched {len(rows)} issues for sprint {sprint_id} ({sprint_name})")

    con = _connect_duckdb(db_path)
    # Delete rows for this partition (covers normal re-materialisation).
    con.execute("DELETE FROM raw.raw_jira_issues WHERE sprint_id = ?", [sprint_id])
    if rows:
        # Also delete by issue_key so that any row previously attributed to a different
        # sprint (e.g. a carry-over issue stored under an earlier sprint_id) is removed
        # before we insert the authoritative version.
        issue_keys = list({r["issue_key"] for r in rows if r["issue_key"]})
        placeholders = ",".join("?" * len(issue_keys))
        con.execute(
            f"DELETE FROM raw.raw_jira_issues WHERE issue_key IN ({placeholders})",
            issue_keys,
        )
        con.executemany(
            """
            INSERT INTO raw.raw_jira_issues (
                issue_key, sprint_id, board_id, summary,
                story_points, t_shirt_size, status_category, labels,
                assignee, cf_sprint_id, cf_sprint_name, cf_sprint_state, _extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    r["issue_key"], r["sprint_id"], r["board_id"], r["summary"],
                    r["story_points"], r["t_shirt_size"], r["status_category"], r["labels"],
                    r["assignee"], r["cf_sprint_id"], r["cf_sprint_name"],
                    r["cf_sprint_state"], r["_extracted_at"],
                ]
                for r in rows
            ],
        )
    context.log.info(f"Upserted {len(rows)} issues for sprint {sprint_id} into raw.raw_jira_issues")
    con.close()
