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

### Layer Breakdown

**`dagster_pipeline/assets/`** — Python extraction and export:
- `jira.py`: Materializes `raw_jira_sprints` (all sprints from both boards) and `raw_jira_issues` (partitioned by sprint ID for concurrency)
- `gsheets.py`: Reads input sheet tabs into raw DuckDB tables; writes team split prepopulation
- `exports.py`: Reads dbt mart tables and writes to output spreadsheet tabs ("Capacity Plan", "Sprint History", "Member Availability")

**`dbt_project/models/`** — Three-layer transformation:
- **staging/** — Clean and normalize raw tables (6 `stg_*` views). `stg_jira_sprints` computes `sprint_working_days` (actual Mon–Fri count, exclusive end_date) via `generate_series`.
- **intermediate/** — Compute member availability per sprint (deducts holidays, time off, cross-board allocations); compute velocity (4 `int_*` tables)
- **marts/** — Final outputs: `mart_capacity_planning` (recommended story points), `mart_sprint_history`, `mart_rolling_velocity` (5-sprint rolling average), `mart_member_availability` (per-member available days excl. splits, active/future sprints only — reference for filling in Team Splits)

**`dbt_project/seeds/`** — Static reference data in CSV: `team_members`, `board_config`, `subteam_labels`, `member_regions`

### Key Design Decisions

**DuckDB concurrency**: `raw_jira_issues` is partitioned by sprint ID, so Dagster materializes many partitions concurrently. DuckDB only allows one writer at a time, so `dagster_home/dagster.yaml` sets `QueuedRunCoordinator` with `max_concurrent_runs: 1` for assets tagged `dagster/concurrency_key: duckdb`. Assets that write to DuckDB must include this tag.

**Sprint partitioning**: `raw_jira_issues` uses `DynamicPartitionsDefinition(name="sprint_dates")` with YYYY-MM-DD keys. One partition = one sprint cycle = all boards (DELI + NFR share the same start date). `raw_jira_sprints` registers `start_date[:10]` as partition keys; sprints without a start_date are skipped. The asset fetches issues for every board whose sprint starts on the partition date in a single run, applying per-board skip logic independently. The hourly sensor refreshes active/future dates daily, loads older closed dates once, and daily-refreshes the most recently closed date per board to capture issues resolved after the last active-sprint fetch but before close.

**Boards and sub-teams**: Board membership and sub-team groupings are defined in `dbt_project/seeds/board_config.csv` and `subteam_labels.csv`. The DELI board contains DevOps, Backend, and SolArch sub-teams; NFR contains Infra. Adding a new board or sub-team requires updating these seeds.

**Capacity formula**: Base capacity = actual weekdays in the sprint window (Mon–Fri, end_date exclusive) per member, minus public holidays (region-specific), minus time-off days, minus allocation days when a member works across multiple boards. `int_member_sprint_availability` exposes both `availability_days` (includes team-split override) and `availability_days_excl_splits` (formula only, no override) for use as a reference when entering splits.

### Schedules (Madrid timezone)

| Schedule | What It Does |
|---|---|
| Tuesday 08:00 | Full pipeline: extract → dbt → export |
| Monday 06:00 | Pre-populate Team Splits sheet for upcoming sprints |
| Hourly sensor | Materialize `raw_jira_issues` partitions |
| Hourly sensor | Trigger full pipeline on sprint close |

## Linting Configuration

Ruff is configured in `pyproject.toml`: line length 100, Python 3.11 target. Run `ruff check .` from the repo root.
