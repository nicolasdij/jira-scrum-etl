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
from dagster import AssetKey, DynamicPartitionsDefinition, MaterializeResult, MetadataValue, asset
from dagster_pipeline.resources.jira_resource import JiraResource

sprint_partitions = DynamicPartitionsDefinition(name="sprint_dates")


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

    # Register sprint start_dates as dynamic partition keys. One date = one sprint cycle
    # covering all boards. Sprints without a start_date (some future sprints) are skipped.
    # DELI and NFR share the same start_date so the set naturally deduplicates to one key.
    new_dates: set[str] = set()
    for r in rows:
        raw_sd = r.get("start_date")
        if raw_sd:
            new_dates.add(raw_sd[:10])  # "2026-04-14T08:44:00Z" → "2026-04-14"

    existing = set(context.instance.get_dynamic_partitions("sprint_dates"))
    to_add = list(new_dates - existing)
    if to_add:
        context.instance.add_dynamic_partitions("sprint_dates", to_add)
        context.log.info(f"Registered {len(to_add)} new sprint date partition(s): {to_add}")


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
        "Issues for a sprint-date partition. One partition covers all boards whose sprint "
        "starts on that date (DELI + NFR). Per-board skip logic: closed boards are skipped "
        "when rows already exist unless it is that board's last closed sprint."
    ),
)
def raw_jira_issues(context, jira: JiraResource) -> MaterializeResult | None:
    import os
    if not context.has_partition_key:
        context.log.error(
            "raw_jira_issues must be run with a sprint date partition key. "
            "Trigger via raw_jira_issues_partition_sensor or select a partition in the UI."
        )
        return

    start_date = context.partition_key  # e.g. "2026-04-14"
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
            status          VARCHAR,
            labels          VARCHAR,
            assignee        VARCHAR,
            cf_sprint_id    VARCHAR,
            cf_sprint_name  VARCHAR,
            cf_sprint_state VARCHAR,
            _extracted_at   VARCHAR
        )
    """)
    con.execute("ALTER TABLE raw.raw_jira_issues ADD COLUMN IF NOT EXISTS status VARCHAR")

    # All board-sprints starting on this date (standard case: one row per board).
    sprint_rows = con.execute(
        """
        SELECT sprint_id, board_id, sprint_name, state
        FROM raw.raw_jira_sprints
        WHERE start_date LIKE ? || '%'
        ORDER BY board_id
        """,
        [start_date],
    ).fetchall()

    if not sprint_rows:
        context.log.warning(f"No sprints found for start_date '{start_date}'; skipping.")
        con.close()
        return

    # Pre-compute the most recently closed sprint_id per board so the per-board skip
    # logic below can allow a final re-fetch to capture late resolutions.
    last_closed_ids: set[int] = {
        row[0]
        for row in con.execute(
            """
            SELECT sprint_id FROM (
                SELECT sprint_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY board_id ORDER BY complete_date DESC NULLS LAST
                       ) AS rn
                FROM raw.raw_jira_sprints
                WHERE state = 'closed'
            ) ranked
            WHERE rn = 1
            """
        ).fetchall()
    }

    con.close()

    all_rows: list[dict] = []
    fetched_sprint_ids: list[int] = []

    for sprint_id, board_id, sprint_name, state in sprint_rows:
        # Per-board skip logic: closed sprints with existing rows are skipped unless
        # this is the last closed sprint for that board.
        if state == "closed":
            con = _connect_duckdb(db_path)
            existing = con.execute(
                "SELECT COUNT(*) FROM raw.raw_jira_issues WHERE sprint_id = ?", [sprint_id]
            ).fetchone()[0]
            con.close()
            if existing > 0:
                if sprint_id not in last_closed_ids:
                    context.log.info(
                        f"Board {board_id} sprint {sprint_id} ({sprint_name}) is closed "
                        f"and already loaded — skipping."
                    )
                    continue
                context.log.info(
                    f"Board {board_id} sprint {sprint_id} ({sprint_name}) is the last "
                    f"closed sprint for board {board_id} — re-fetching."
                )

        context.log.info(
            f"Fetching issues for board {board_id} sprint {sprint_id} ({sprint_name}, state={state})"
        )
        issues = jira.get_issues_for_sprint(sprint_id)
        fetched_sprint_ids.append(sprint_id)

        for issue in issues:
            fields = issue.get("fields", {})
            story_points = fields.get("customfield_10105")
            t_shirt_size = fields.get("customfield_10834")
            status_obj = fields.get("status") or {}
            status_name = status_obj.get("name")
            status_category = (
                (status_obj.get("statusCategory") or {}).get("key") or ""
            ).lower()
            # Jira maps "Cancelled" to statusCategory "done", but cancelled issues must not
            # count toward velocity or burndown — override to a distinct value.
            if (status_name or "").lower() == "cancelled":
                status_category = "cancelled"
            labels = fields.get("labels") or []
            sprint_ref = _resolve_sprint_from_field(fields.get("customfield_10103"))
            assignee_obj = fields.get("assignee") or {}
            all_rows.append({
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
                "status": status_name,
                "labels": json.dumps(labels),
                "assignee": assignee_obj.get("displayName"),
                "cf_sprint_id": sprint_ref["id"],
                "cf_sprint_name": sprint_ref["name"],
                "cf_sprint_state": sprint_ref["state"],
                "_extracted_at": datetime.utcnow().isoformat(),
            })

        context.log.info(
            f"Fetched {len(issues)} issues for board {board_id} sprint {sprint_id} ({sprint_name})"
        )

    if not fetched_sprint_ids:
        context.log.info(f"All boards for {start_date} already loaded — nothing to write.")
        return

    con = _connect_duckdb(db_path)
    # Delete only the sprint_ids we re-fetched; skipped boards' rows stay intact.
    for sid in fetched_sprint_ids:
        con.execute("DELETE FROM raw.raw_jira_issues WHERE sprint_id = ?", [sid])
    if all_rows:
        # Also delete by issue_key so that carry-over issues attributed to an earlier
        # sprint_id are replaced with the authoritative version from the current fetch.
        issue_keys = list({r["issue_key"] for r in all_rows if r["issue_key"]})
        placeholders = ",".join("?" * len(issue_keys))
        con.execute(
            f"DELETE FROM raw.raw_jira_issues WHERE issue_key IN ({placeholders})",
            issue_keys,
        )
        con.executemany(
            """
            INSERT INTO raw.raw_jira_issues (
                issue_key, sprint_id, board_id, summary,
                story_points, t_shirt_size, status_category, status, labels,
                assignee, cf_sprint_id, cf_sprint_name, cf_sprint_state, _extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    r["issue_key"], r["sprint_id"], r["board_id"], r["summary"],
                    r["story_points"], r["t_shirt_size"], r["status_category"], r["status"], r["labels"],
                    r["assignee"], r["cf_sprint_id"], r["cf_sprint_name"],
                    r["cf_sprint_state"], r["_extracted_at"],
                ]
                for r in all_rows
            ],
        )
    context.log.info(
        f"Upserted {len(all_rows)} issues for start_date {start_date} "
        f"(sprint_ids: {fetched_sprint_ids}) into raw.raw_jira_issues"
    )
    con.close()
    return MaterializeResult(
        metadata={
            "issue_keys": MetadataValue.json(
                sorted({r["issue_key"] for r in all_rows if r["issue_key"]})
            ),
            "num_issues": MetadataValue.int(len(all_rows)),
            "sprint_ids": MetadataValue.json(fetched_sprint_ids),
            "boards": MetadataValue.json(sorted({r["board_id"] for r in all_rows})),
        }
    )
