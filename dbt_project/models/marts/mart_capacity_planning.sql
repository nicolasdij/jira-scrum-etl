-- mart_capacity_planning
-- Primary output: recommended story-point capacity for the upcoming (future) sprint.
--
-- Joins:
--   - The latest rolling average velocity per sub-team (from mart_rolling_velocity)
--   - The next-sprint capacity percentage (from int_subteam_sprint_capacity_pct)
--
-- "Next sprint" = the future sprint with the lowest start_date.

WITH next_sprint AS (
    SELECT sprint_id, sprint_name, board_id
    FROM (
        SELECT
            sprint_id,
            sprint_name,
            board_id,
            ROW_NUMBER() OVER (PARTITION BY board_id ORDER BY start_date) AS rn
        FROM {{ ref('stg_jira_sprints') }}
        WHERE sprint_state = 'future'
    ) sub
    WHERE rn = 1
),

-- Most recent rolling velocity per sub-team
latest_velocity AS (
    SELECT DISTINCT ON (subteam)
        subteam,
        rolling_avg_velocity,
        sprint_id        AS last_closed_sprint_id,
        sprint_name      AS last_closed_sprint_name,
        sprints_in_window
    FROM {{ ref('mart_rolling_velocity') }}
    ORDER BY subteam, sprint_start_date DESC
),

-- Next-sprint capacity for each sub-team
next_capacity AS (
    SELECT
        c.sprint_id,
        c.subteam,
        c.capacity_pct,
        c.subteam_size,
        c.total_available_days
    FROM {{ ref('int_subteam_sprint_capacity_pct') }} c
    JOIN next_sprint ns USING (sprint_id)
),

combined AS (
    SELECT
        ns.sprint_name                          AS target_sprint,
        nc.subteam,
        lv.rolling_avg_velocity,
        lv.sprints_in_window,
        nc.capacity_pct                         AS next_sprint_availability_pct,
        nc.subteam_size,
        nc.total_available_days,
        FLOOR(lv.rolling_avg_velocity * nc.capacity_pct)
                                                AS recommended_capacity_points,
        lv.last_closed_sprint_name
    FROM next_sprint ns
    JOIN next_capacity nc ON nc.sprint_id = ns.sprint_id
    JOIN latest_velocity lv ON lv.subteam = nc.subteam
)

SELECT * FROM combined
ORDER BY subteam
