# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Jira Scrum ETL is a data pipeline that:
1. Extracts sprint and issue data from Jira (DELI board 74, NFR board 212)
2. Reads team capacity data (time off, public holidays, team splits) from a Google Sheet
3. Transforms everything using dbt/DuckDB to compute capacity planning recommendations and sprint history
4. Writes results back to a Google Sheets output spreadsheet

## Development Commands

```bash
# Run the Dagster dev UI (http://localhost:3000)
dagster dev

# Initialize dbt (required on first run or after adding packages)
cd dbt_project && dbt deps && dbt parse

# Run all dbt models
cd dbt_project && dbt build

# Run a single dbt model
cd dbt_project && dbt run --select <model_name>

# Lint Python
ruff check .

# Run tests
python -m pytest

# Run a single test file
python -m pytest tests/path/to/test_file.py -v
```

**First-time setup**: Must run `dbt deps && dbt parse` before Dagster can load the dbt assets, as Dagster reads the compiled dbt manifest.

## Environment Variables

Copy `.env.example` to `.env`. Required variables:
- `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN` — Jira Agile API access
- `GOOGLE_APPLICATION_CREDENTIALS` — Path to GCP service account JSON (needs Sheets + Drive scopes)
- `GSHEETS_INPUT_SPREADSHEET_ID`, `GSHEETS_OUTPUT_SPREADSHEET_ID`
- `DUCKDB_PATH` — Path to DuckDB file (default: `data/scrum.duckdb`)
- `DAGSTER_HOME` — Path to Dagster home dir (default: `dagster_home/`)

## Architecture

### Data Flow

```
Jira API ──────────────────────────────────────┐
                                               ▼
Google Sheets (input) ──► Dagster assets ──► DuckDB raw tables
                                               │
                                               ▼
                                           dbt models
                                       (staging → intermediate → marts)
                                               │
                                               ▼
                                    Dagster export assets ──► Google Sheets (output)
```

### Google Sheet structure

`GSHEETS_INPUT_SPREADSHEET_ID` and `GSHEETS_OUTPUT_SPREADSHEET_ID` typically point at the same workbook. Tabs are organised into sections, each fronted by a navigation "header" tab whose name ends with ` >`:

- **`ETL Input >`** — `Time Off`, `Public Holidays`, `Team Splits`. The first two are pure manual inputs; `Team Splits` is hybrid — `gsheets_team_splits_prepopulator` drafts skeleton rows weekly and the user fills `days_allocated`.
- **`ETL Output >`** — `Member Availability`, `Sprint History`, `Capacity Plan`, `Sprint Items`, `Sprint Plan`. All but `Sprint Items` are written by `exports.py` from dbt marts; `Sprint Items` is hybrid — `gsheets_sprint_planning_prepopulator` drafts one row per candidate issue and the user edits `planned_points` and `include`.
- **`SHEET Analysis >`** — sheet-side cross-checks (formulas inside the sheet, no Dagster involvement): `Validation`.

(There is also an empty `Hibrid >` header tab between the input and output sections — a placeholder, currently unused.)

Each header tab carries hyperlinks in column A to its subsection tabs and short purpose descriptions in column B; the rest of the cells are unused. Dagster references the tabs by exact name via constants at the top of `dagster_pipeline/assets/gsheets.py` (`SHEET_TIME_OFF`, `SHEET_HOLIDAYS`, `SHEET_TEAM_SPLITS`, `SHEET_SPRINT_PLANNING`) and via the `exports.py` mapping — renaming a tab in the sheet requires updating the corresponding constant.

### Layer Breakdown

**`dagster_pipeline/assets/`** — Python extraction and export:
- `jira.py`: Materializes `raw_jira_sprints` (all sprints from both boards) and `raw_jira_issues` (partitioned by sprint ID for concurrency). Captures exact Jira status name (`status`) alongside `status_category`.
- `gsheets.py`: Reads input sheet tabs into raw DuckDB tables; writes team split prepopulation; pre-populates Sprint Items sheet for upcoming sprints.
- `workday.py`: Materializes `raw_workday_time_off` by normalizing the Workday export CSV (`data/workday-time_off.csv`) — strips the export preamble/blank rows — into a raw DuckDB table. See "Time-off sources" below.
- `exports.py`: Reads dbt mart tables and writes to output spreadsheet tabs ("Capacity Plan", "Sprint History", "Member Availability", "Sprint Plan")

**`dbt_project/models/`** — Three-layer transformation:
- **staging/** — Clean and normalize raw tables. `stg_jira_sprints` computes `sprint_working_days` (actual Mon–Fri count, exclusive end_date). `stg_jira_issues` exposes both `subteam_from_label` (strict label match, NULL if no match) and `subteam` (label match with board default_subteam as fallback), plus `is_subtask` and `parent_key`. `stg_sprint_planning` cleans the Sprint Items input sheet (casts `planned_points`, normalises `include` to BOOLEAN, excludes cancelled issues). `stg_time_off` combines two time-off sources at day grain (see "Time-off sources" below): the manual `Time Off` sheet (`raw_time_off`, ranges) and `stg_time_off_workday` (the Workday export via `raw_workday_time_off`, already per-day), de-duplicating overlapping days before aggregating to days-off per (member, sprint, board).
- **intermediate/** — `int_effective_issues` applies the parent/subtask retention rule (see below). Computes member availability per sprint (deducts holidays, time off, cross-board allocations); computes velocity (`int_*` tables). `int_member_sprint_availability` exposes `sprint_start_date` for downstream marts.
- **marts/** — Final outputs: `mart_capacity_planning` (recommended story points per subteam for the currently active sprint *and* the earliest future sprint per board — see "Capacity Plan scope" below), `mart_sprint_history`, `mart_rolling_velocity` (5-sprint rolling average over closed sprints; widens to 6 when the currently active sprint qualifies — see "Active-sprint inclusion" below), `mart_member_availability` (per-member available days excl. splits, active/future sprints only), `mart_sprint_plan` (planned/committed points vs subteam capacity for the currently active sprint *and* the next future sprint per board — see "Sprint Plan scope" below)

**`dbt_project/seeds/`** — Static reference data in CSV: `team_members`, `board_config`, `subteam_labels`, `member_regions`, and `workday_member_map` (`employee_id` → ETL `member_name`; an INNER join on this map is what drops untracked Workday workers — see "Time-off sources" below).

### Key Design Decisions

**DuckDB concurrency**: DuckDB only allows one writer at a time. `dagster_home/dagster.yaml` configures `QueuedRunCoordinator` with `tag_concurrency_limits` so runs tagged `dagster/concurrency_key: duckdb` are serialised (limit: 1). Two layers of tagging are needed:
  - **Op-level** (`op_tags={"dagster/concurrency_key": "duckdb"}`): every asset that opens a write connection — currently `raw_jira_sprints`, `raw_jira_issues`, `raw_time_off`, `raw_public_holidays`, `raw_team_splits`, `raw_sprint_planning`, `gsheets_team_splits_prepopulator`, and `gsheets_sprint_planning_prepopulator`. This serialises ops *within* a single run.
  - **Run-level** (`tags={"dagster/concurrency_key": "duckdb"}` on every `define_asset_job` in `schedules.py`, via the `_DUCKDB_RUN_TAGS` constant): this serialises ops *across* runs, which is what the `QueuedRunCoordinator` actually looks at. Without run-level tags, multiple partition runs of `sprint_planning_sync_job` could execute their prepop ops concurrently, racing on `gspread.append_rows` and producing interleaved blank rows in the Sprint Items sheet.

Read-only connections (exports, sensors, scripts) do not need the tag — the DuckDB write lock does not block read-only opens. As a defensive second line, the `_db()` helper in `gsheets.py` and `_connect_duckdb()` in `jira.py` retry on lock contention.

**Sprint partitioning**: `raw_jira_issues` uses `DynamicPartitionsDefinition(name="sprint_dates")` with YYYY-MM-DD keys. One partition = one sprint cycle = all boards (DELI + NFR share the same start date). `raw_jira_sprints` registers `start_date[:10]` as partition keys; sprints without a start_date are skipped. The asset keeps the registered partition list strictly sorted by date — if the existing order differs from sorted, it deletes and re-adds all keys so the Dagster partition picker shows dates chronologically. The asset fetches issues for every board whose sprint starts on the partition date in a single run, applying per-board skip logic independently. The hourly sensor refreshes active/future dates daily, loads older closed dates once, and daily-refreshes the most recently closed date per board to capture issues resolved after the last active-sprint fetch but before close.

**Boards and sub-teams**: Board membership and sub-team groupings are defined in `dbt_project/seeds/board_config.csv` and `subteam_labels.csv`. The DELI board contains DevOps, Backend, and SolArch sub-teams; NFR contains Infra. Adding a new board or sub-team requires updating these seeds.

**Parent/subtask retention rule** (`int_effective_issues`): The Jira extractor pulls Stories, Tasks, Bugs, and Sub-tasks for each sprint. To avoid double-counting work that exists on both a parent and its subtasks, each unit of work is reduced to exactly one "effective" row per sprint:
1. A subtask is kept iff it carries a subteam-matching label (`subteam_from_label IS NOT NULL`); the board-default fallback does *not* apply to subtasks.
2. A parent is kept iff none of its in-sprint subtasks is labelled. If at least one of its in-sprint subtasks is labelled, the parent is dropped and its labelled subtasks supersede it (their points and statuses drive burndown under their own subteams).
3. A labelled subtask whose parent is not in the sprint is kept as a standalone item.
4. Story points are coalesced to 0 in `int_effective_issues`, so a labelled subtask with no points contributes zero (not NULL) to burndown.

The same rule is applied inline in `gsheets_sprint_planning_prepopulator` so the Sprint Items sheet matches what downstream models see.

**Capacity formula**: Base capacity = actual weekdays in the sprint window (Mon–Fri, end_date exclusive) per member, minus public holidays (region-specific), minus time-off days, minus allocation days when a member works across multiple boards. `int_member_sprint_availability` exposes both `availability_days` (includes team-split override) and `availability_days_excl_splits` (formula only, no override) for use as a reference when entering splits. A `days_allocated = 0` entry in Team Splits is a valid override meaning the member is fully unavailable for that sprint.

**Time-off sources**: Member time off is merged from two sources at day grain inside `stg_time_off`, so `int_member_sprint_availability` and everything downstream see a single deduplicated feed:
- **`Time Off` sheet** (`raw_time_off`) — manual entries stored as `start_date`/`end_date` ranges with an explicit (possibly comma-separated) `board`. Ranges are expanded to working days.
- **Workday export** (`data/workday-time_off.csv` → `raw_workday_time_off` asset → `stg_time_off_workday`) — a per-day extract from Workday. The `raw_workday_time_off` Dagster asset (`assets/workday.py`) strips the export's preamble/blank rows and writes a raw DuckDB table, which dbt reads via `source('raw', 'raw_workday_time_off')` — exactly like the sheet path. Names are mapped to ETL `member_name` via the `workday_member_map` seed (an INNER join, which also drops workers not on the tracked DELI/NFR teams). `board` is derived by joining `team_members`, so a dual-board member (e.g. Pablo) has each off-day counted on every board they belong to — matching the sheet's comma-split semantics. Half-days (`units = 0.5`) are rounded up to whole days.

`stg_time_off` `UNION`s the two day-grain sets (`UNION`, not `UNION ALL`) so a day present in both sources is counted **once**, then joins sprints and aggregates `COUNT(DISTINCT off_date)`. The output contract (`member_name`, `sprint_id`, `board`, `days_off`) is unchanged, so no downstream model needed changes. To refresh from a new Workday export, drop the fresh file at `data/workday-time_off.csv` and re-materialize `raw_workday_time_off` (it runs as part of `full_pipeline_job`, `extraction_only_job`, and `capacity_inputs_sync_job` — no separate `dbt seed` step). The export path is overridable via the `WORKDAY_TIME_OFF_PATH` env var.

**Active-sprint inclusion in velocity**: `int_sprint_velocity` admits the currently active sprint per board once its calendar duration is at least **70% elapsed** (`(CURRENT_DATE - start_date) / (end_date - start_date) >= 0.70`). Below the threshold the rolling velocity is closed-only, identical to the previous behaviour. Above it, raw partial burndown is used — no projection. `mart_rolling_velocity` then applies a state-aware window: closed-sprint rows average the 5 most recent closed sprints (themselves included); active-sprint rows average the 5 most recent closed sprints **plus** themselves (6 data points), so the closed-sprint history isn't displaced. The `sprint_state` column flows all the way to the "Sprint History" tab so the user can tell whether a row references closed or in-flight data. Because `int_sprint_burndown` only emits rows for (sprint, subteam) combinations that have at least one done issue, an active sprint with zero burned points for a subteam silently doesn't contribute — the rolling average for that subteam falls back to the latest closed sprint until something is completed.

**Capacity Plan scope**: `mart_capacity_planning` emits two target rows per board — the currently active sprint and the earliest future sprint — so the Capacity Plan tab shows both what the team is working against today and what's about to be planned, ordered by `sprint_start_date` ascending. The two targets pick different rolling-velocity sources: the active target uses the most recent *closed* sprint's rolling row per board (preserving the velocity that drove its planning, even after the active sprint's own partial burndown starts feeding back into `mart_rolling_velocity` at 70%); the future target uses the latest rolling row overall per subteam (existing behaviour, which may include the active sprint past the threshold). `last_velocity_sprint_name` identifies the source row, and the target sprint's own state is exposed as `target_sprint_state` in the mart (aliased to `sprint_state` on the tab) to disambiguate which of the two rows per board is which. The mart column is deliberately distinct from `mart_rolling_velocity.sprint_state` (which describes the velocity-source sprint, not the target) to avoid name collisions when downstream analyses join the two. Note: `mart_sprint_plan`'s join into `mart_capacity_planning` is keyed on `(sprint_name, subteam)` (not just `subteam`) so the new active-target rows don't multiply planned-row counts.

**Sprint Plan scope**: `mart_sprint_plan` emits one row per (board, subteam) for **both** the currently active sprint and the earliest future sprint — the Sprint Plan tab now answers "did we plan above capacity for next sprint?" *and* "if I drop a new backlog item into the current sprint, do we still fit under capacity?" The two halves source `planned_points` from different places:
- **Future rows** are sheet-driven: points come from `stg_sprint_planning`, gated by the user's `include` flag (an opt-in selection of issues to count).
- **Active rows** are Jira-driven: points come from `int_effective_issues` directly, with no opt-in — being in the active sprint *is* the inclusion. Cancelled issues are excluded for parity with the sheet path; done and in-flight items both count toward total commitment (the recommended capacity was set against the same totality).

The output `sprint_state` column ('active' / 'future') disambiguates which semantics apply. Capacity is sourced uniformly from `mart_capacity_planning` for both halves: future rows pick up the future-target capacity (latest rolling row overall), active rows pick up the active-target capacity (closed-only rolling row that drove the original planning). The Sprint Plan tab is ordered by `sprint_start_date` ascending, so the active row appears above the future row for each board.

**Sprint planning workflow**: Before committing changes in Jira, use the Sprint Items input sheet to plan the next sprint. The `gsheets_sprint_planning_prepopulator` fills in one row per *effective* issue currently assigned to the first future sprint per board (`status_category = 'cancelled'` issues excluded). The parent/subtask retention rule above is applied, so a parent with labelled subtasks is replaced by its labelled subtasks in the sheet. `planned_points` is pre-filled from `COALESCE(story_points, t_shirt_size)`; `subteam` is derived from issue labels via `subteam_labels` seed (board default subteam applies only to parents, never to subtasks). The `issue_key` column contains a `=HYPERLINK()` formula linking directly to the Jira issue. Edit `planned_points` and `include` (TRUE/FALSE) as needed, then run `sprint_plan_update_job` to refresh the Sprint Plan output tab. The Sprint Plan tab shows `planned_points` vs `subteam_capacity` (= `recommended_capacity_points` from `mart_capacity_planning`) per subteam, for both the active and future sprint, alongside a computed `difference = subteam_capacity − planned_points` column (positive = headroom, negative = over-commitment). To check whether a mid-sprint addition fits, look at the active row for the affected subteam, subtract the new item's points from `difference`, and confirm the result is still ≥ 0. The `difference` column is computed in the export SELECT, not in the mart — adjusting it is an export-only change, no dbt rebuild needed.

The prepopulator is mostly non-destructive: it appends new rows and **refreshes blank `planned_points` / `subteam` / `status` cells only** on existing rows (manually entered values are preserved). It deletes three categories of rows: (a) **current-target orphans** — issues that no longer appear in `raw_jira_issues` for their target sprint (Jira moves like DELI-281 → EIR-1733, deletions, or parents superseded by labelled subtasks); (b) **duplicate** rows sharing the same `(issue_key, sprint_name)` as an earlier row (the first occurrence is kept); (c) **past-target rows** whose `sprint_name` corresponds to a Jira sprint that has transitioned out of `state='future'` (active or closed) — these are leftovers from when that sprint was the planning target and would otherwise pollute `mart_sprint_plan`. Rows with blank `issue_key` are ignored entirely (neither deduped nor counted as orphans). Rows for sprints that don't exist in `raw_jira_sprints` at all are left alone.

**Active-sprint rollover candidates**: the prepopulator also surfaces still-open work from the *currently active* sprint per board once that sprint's calendar duration is at least **70% elapsed** (same threshold as `int_sprint_velocity` — `_ACTIVE_SPRINT_ROLLOVER_THRESHOLD` in `gsheets.py`). Any active-sprint issue with `status_category` in `('new', 'indeterminate')` is appended as a new row under the *future* sprint's name with `include = FALSE`, so the user can opt-in to counting it toward next-sprint capacity. Below 70%, no rollover rows are added. If a rolled-over issue completes (`status_category = 'done'`) or moves out of the active sprint between runs, its stale row is detected as an orphan and removed.

**One-click sync after editing in Jira**: `sprint_planning_sync_job` chains `raw_jira_issues` (partitioned) → `gsheets_sprint_planning_prepopulator` → `raw_sprint_planning` → `stg_sprint_planning` → `mart_sprint_plan` → `gsheets_sprint_plan_export`. Launch from the Dagster UI, pick the future sprint's `start_date` (YYYY-MM-DD) as the partition, and it re-fetches that sprint's issues from Jira, refreshes blank cells / removes orphan rows in the Sprint Items sheet, and rebuilds the Sprint Plan output tab in one run.

**One-click sync after editing availability inputs**: `capacity_inputs_sync_job` chains the availability inputs into the dependent outputs: `raw_time_off` / `raw_workday_time_off` / `raw_public_holidays` / `raw_team_splits` → `stg_*` → `int_member_sprint_availability` → `int_subteam_sprint_capacity_pct` → `mart_capacity_planning` + `mart_sprint_plan` + `mart_member_availability` → `gsheets_capacity_plan_export` + `gsheets_sprint_plan_export` + `gsheets_member_availability_export`. Run this after editing Time Off, Public Holidays, or Team Splits so subteam capacity (and the Sprint Plan headroom column) reflects the new availability, and the Member Availability reference tab stays in sync. `mart_rolling_velocity` is intentionally left out — it depends on Jira data, not on these inputs.

### Schedules and Jobs (Madrid timezone)

| Schedule / Job | What It Does |
|---|---|
| Tuesday 08:00 (schedule) | Full pipeline: extract → dbt → export |
| Monday 06:00 (schedule) | Pre-populate Team Splits sheet for upcoming sprints |
| Hourly sensor | Materialize `raw_jira_issues` partitions |
| Hourly sensor | Trigger full pipeline on sprint close |
| `sprint_plan_update_job` (on-demand) | Re-reads Sprint Items sheet → rebuilds `mart_sprint_plan` → updates Sprint Plan tab |
| `sprint_planning_sync_job` (on-demand, partitioned) | Re-fetches `raw_jira_issues` for the picked sprint date, refreshes/cleans the Sprint Items sheet, and rebuilds the Sprint Plan output tab in one run |
| `capacity_inputs_sync_job` (on-demand) | Re-reads Time Off / Workday export / Public Holidays / Team Splits → rebuilds `mart_capacity_planning`, `mart_sprint_plan`, and `mart_member_availability` → updates Capacity Plan, Sprint Plan, and Member Availability tabs |

## One-Time / Maintenance Scripts

**`scripts/backfill_team_splits_start_date.py`** — Populates the `sprint_start_date` column in the Team Splits Google Sheet tab for rows where it is blank, using the corresponding `start_date` from `raw.raw_jira_sprints` in DuckDB. Run with `uv run python scripts/backfill_team_splits_start_date.py`. Safe to re-run: rows that already have a value are skipped.

## Linting Configuration

Ruff is configured in `pyproject.toml`: line length 100, Python 3.11 target. Run `ruff check .` from the repo root.
