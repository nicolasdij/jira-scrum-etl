-- mart_capacity_planning
-- Recommended story-point capacity per subteam for two target sprints per board:
--   * the currently active sprint — what the team is working against today;
--     the row preserves the capacity that drove its planning so the user
--     can still look it up after the sprint has started.
--   * the earliest future sprint — the next one to be planned.
--
-- Capacity = FLOOR(rolling_avg_velocity x capacity_pct).
--   * For the ACTIVE target, rolling_avg_velocity is the velocity that was
--     available when this sprint was being planned — i.e. the most recent
--     closed-sprint rolling row per board. The active sprint's own partial
--     burndown does not feed back into its own recommendation.
--   * For the FUTURE target, rolling_avg_velocity is the latest rolling row
--     available now per subteam, which may include the currently active
--     sprint once it is at least 70 % elapsed (see int_sprint_velocity).
--
-- Output ordering: sprint_start_date ASC, target_sprint ASC, subteam ASC —
-- so the active sprint (earlier start_date) appears before the future one.

WITH target_sprints AS (
    SELECT sprint_id, sprint_name, board_id, sprint_state, sprint_start_date
    FROM (
        SELECT
            sprint_id, sprint_name, board_id, sprint_state,
            start_date AS sprint_start_date,
            ROW_NUMBER() OVER (
                PARTITION BY board_id, sprint_state ORDER BY start_date
            ) AS rn
        FROM {{ ref('stg_jira_sprints') }}
        WHERE sprint_state IN ('active', 'future')
          AND start_date IS NOT NULL
    ) ranked
    WHERE rn = 1
),

-- Rolling-velocity row used for ACTIVE targets: most recent CLOSED sprint per
-- (board, subteam). This is exactly what was on the Capacity Plan tab when
-- the now-active sprint was the future target.
active_planning_velocity AS (
    SELECT DISTINCT ON (board_id, subteam)
        board_id,
        subteam,
        rolling_avg_velocity,
        sprint_name        AS last_velocity_sprint_name,
        sprints_in_window
    FROM {{ ref('mart_rolling_velocity') }}
    WHERE sprint_state = 'closed'
    ORDER BY board_id, subteam, sprint_start_date DESC
),

-- Rolling-velocity row used for FUTURE targets: most recent rolling row
-- overall per subteam, which may be the active sprint past the 70 % threshold.
future_planning_velocity AS (
    SELECT DISTINCT ON (subteam)
        subteam,
        rolling_avg_velocity,
        sprint_name        AS last_velocity_sprint_name,
        sprints_in_window
    FROM {{ ref('mart_rolling_velocity') }}
    ORDER BY subteam, sprint_start_date DESC
),

target_capacity AS (
    SELECT
        ts.sprint_id,
        ts.sprint_name,
        ts.sprint_state,
        ts.sprint_start_date,
        ts.board_id,
        c.subteam,
        c.capacity_pct,
        c.subteam_size,
        c.total_available_days
    FROM target_sprints ts
    JOIN {{ ref('int_subteam_sprint_capacity_pct') }} c USING (sprint_id)
)

SELECT
    tc.sprint_name                                              AS target_sprint,
    strftime(tc.sprint_start_date::DATE, '%Y-%m-%d')            AS sprint_start_date,
    tc.sprint_state                                             AS sprint_state,
    tc.subteam,
    COALESCE(apv.rolling_avg_velocity, fpv.rolling_avg_velocity)
                                                                AS rolling_avg_velocity,
    COALESCE(apv.sprints_in_window, fpv.sprints_in_window)
                                                                AS sprints_in_window,
    tc.capacity_pct                                             AS next_sprint_availability_pct,
    tc.subteam_size,
    tc.total_available_days,
    FLOOR(COALESCE(apv.rolling_avg_velocity, fpv.rolling_avg_velocity) * tc.capacity_pct)
                                                                AS recommended_capacity_points,
    COALESCE(apv.last_velocity_sprint_name, fpv.last_velocity_sprint_name)
                                                                AS last_velocity_sprint_name
FROM target_capacity tc
LEFT JOIN active_planning_velocity apv
    ON apv.board_id = tc.board_id
   AND apv.subteam  = tc.subteam
   AND tc.sprint_state = 'active'
LEFT JOIN future_planning_velocity fpv
    ON fpv.subteam  = tc.subteam
   AND tc.sprint_state = 'future'
ORDER BY sprint_start_date, target_sprint, subteam
