-- stg_time_off_workday
-- Day-grain time-off rows sourced from the Workday export (raw_workday_time_off,
-- written by the raw_workday_time_off Dagster asset).
-- Emits the same (member_name, board, off_date) shape as the sheet-based path
-- so stg_time_off can UNION both sources and de-duplicate overlaps.
--
-- Notes:
--   * INNER JOIN to workday_member_map maps Workday legal names -> ETL member
--     names and silently drops untracked workers (not in the map).
--   * board is derived from team_members (one row per board), mirroring the
--     comma-split board handling in the sheet path — dual-board members get
--     time off counted on every board they belong to.
--   * Half-days (units = 0.5) are rounded up to a whole day: the day is simply
--     counted, regardless of units.

WITH source AS (
    SELECT
        TRIM(employee_id) AS employee_id,
        TRY_CAST(time_off_date AS DATE) AS off_date
    FROM {{ source('raw', 'raw_workday_time_off') }}
    WHERE TRY_CAST(time_off_date AS DATE) IS NOT NULL
),

-- Map to ETL member names; INNER JOIN drops untracked workers.
mapped AS (
    SELECT
        m.member_name,
        s.off_date
    FROM source s
    JOIN {{ ref('workday_member_map') }} m
        ON TRIM(m.employee_id) = s.employee_id
),

-- Fan out to one row per board the member belongs to.
per_board AS (
    SELECT DISTINCT
        mapped.member_name,
        tm.board,
        mapped.off_date
    FROM mapped
    JOIN {{ ref('team_members') }} tm
        ON tm.member_name = mapped.member_name
),

-- Keep only working days (Mon–Fri). DuckDB: 0=Sunday, 6=Saturday.
working_days AS (
    SELECT *
    FROM per_board
    WHERE DAYOFWEEK(off_date) NOT IN (0, 6)
)

SELECT
    member_name,
    board,
    off_date
FROM working_days
