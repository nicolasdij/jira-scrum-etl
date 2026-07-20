-- stg_time_off
-- Combine time-off from two sources at day grain, de-duplicate, then aggregate
-- to days_off_per_sprint_per_member (working days only):
--   1. The manual "Time Off" Google Sheet tab (raw_time_off), stored as ranges.
--   2. The Workday export seed (stg_time_off_workday), already at day grain.
-- De-duplicating at (member_name, board, off_date) means a day present in both
-- sources is counted exactly once.

WITH source AS (
    SELECT
        member_name,
        TRY_CAST(start_date AS DATE) AS start_date,
        TRY_CAST(end_date   AS DATE) AS end_date,
        TRIM(UNNEST(string_split(board, ','))) AS board
    FROM {{ source('raw', 'raw_time_off') }}
    WHERE start_date IS NOT NULL
      AND end_date   IS NOT NULL
),

-- Expand the sheet ranges into one row per calendar day, working days only.
sheet_days AS (
    SELECT
        member_name,
        board,
        off_date
    FROM (
        SELECT
            member_name,
            board,
            UNNEST(
                generate_series(start_date::TIMESTAMP, end_date::TIMESTAMP, INTERVAL '1 day')
            )::DATE AS off_date
        FROM source
    )
    WHERE DAYOFWEEK(off_date) NOT IN (0, 6)   -- DuckDB: 0=Sunday, 6=Saturday
),

-- Union both day-grain sources and de-duplicate overlapping days.
all_days AS (
    SELECT member_name, board, off_date FROM sheet_days
    UNION   -- UNION (not UNION ALL) drops duplicate (member, board, day) rows
    SELECT member_name, board, off_date FROM {{ ref('stg_time_off_workday') }}
),

-- Join to sprints to know which sprint each off-day falls in.
joined AS (
    SELECT
        ad.member_name,
        ad.board,
        ad.off_date,
        s.sprint_id
    FROM all_days ad
    JOIN {{ ref('stg_jira_sprints') }} s
        ON ad.off_date BETWEEN s.start_date::DATE AND s.end_date::DATE
),

aggregated AS (
    SELECT
        member_name,
        sprint_id,
        board,
        COUNT(DISTINCT off_date) AS days_off
    FROM joined
    GROUP BY 1, 2, 3
)

SELECT * FROM aggregated
