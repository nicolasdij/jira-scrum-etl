-- mart_sprint_history
-- Full history for dashboarding: one row per (sprint, sub-team) with burndown,
-- capacity %, weighted velocity, and rolling average.
-- Includes all closed sprints where velocity could be computed.

WITH velocity AS (
    SELECT * FROM {{ ref('int_sprint_velocity') }}
),

rolling AS (
    SELECT
        sprint_id,
        subteam,
        rolling_avg_velocity,
        sprints_in_window
    FROM {{ ref('mart_rolling_velocity') }}
),

capacity AS (
    SELECT
        sprint_id,
        subteam,
        capacity_pct,
        subteam_size,
        total_available_days,
        ideal_days
    FROM {{ ref('int_subteam_sprint_capacity_pct') }}
),

burndown AS (
    SELECT
        sprint_id,
        subteam,
        burnt_points
    FROM {{ ref('int_sprint_burndown') }}
)

SELECT
    v.sprint_id,
    v.sprint_name,
    v.board_id,
    v.subteam,
    strftime(v.sprint_start_date::DATE, '%Y-%m-%d') AS sprint_start_date,
    b.burnt_points,
    c.capacity_pct,
    c.subteam_size,
    c.total_available_days,
    c.ideal_days,
    v.weighted_velocity,
    r.rolling_avg_velocity,
    r.sprints_in_window
FROM velocity v
JOIN capacity  c USING (sprint_id, subteam)
JOIN burndown  b USING (sprint_id, subteam)
LEFT JOIN rolling r USING (sprint_id, subteam)
ORDER BY v.sprint_start_date, v.subteam
