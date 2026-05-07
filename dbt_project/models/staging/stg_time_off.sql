-- stg_time_off
-- Expand date ranges into individual calendar days, then aggregate to
-- produce days_off_per_sprint_per_member (working days only).

WITH source AS (
    SELECT
        member_name,
        TRY_CAST(start_date AS DATE) AS start_date,
        TRY_CAST(end_date   AS DATE) AS end_date,
        board
    FROM {{ source('raw', 'raw_time_off') }}
    WHERE start_date IS NOT NULL
      AND end_date   IS NOT NULL
),

-- Generate one row per calendar day in each time-off range
days AS (
    SELECT
        member_name,
        board,
        UNNEST(
            generate_series(start_date::TIMESTAMP, end_date::TIMESTAMP, INTERVAL '1 day')
        )::DATE AS off_date
    FROM source
),

-- Keep only working days (Mon–Fri)
working_days AS (
    SELECT *
    FROM days
    WHERE DAYOFWEEK(off_date) NOT IN (0, 6)   -- DuckDB: 0=Sunday, 6=Saturday
),

-- Join to sprints to know which sprint each off-day falls in
joined AS (
    SELECT
        wd.member_name,
        wd.board,
        wd.off_date,
        s.sprint_id
    FROM working_days wd
    JOIN {{ ref('stg_jira_sprints') }} s
        ON wd.off_date BETWEEN s.start_date::DATE AND s.end_date::DATE
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
