-- int_sprint_velocity
-- Weighted velocity per sprint per sub-team.
--
-- Formula: FLOOR( burnt_points / capacity_pct )
-- Closed sprints are always included. The currently active sprint per board
-- is also included once its calendar duration is at least 70 % elapsed —
-- before that point the partial burndown is too noisy to be useful. The
-- active row uses raw partial burndown (no projection); downstream marts
-- handle the window-size adjustment.
-- Future sprints are excluded (no burndown yet).

WITH burndown AS (
    SELECT * FROM {{ ref('int_sprint_burndown') }}
),

capacity AS (
    SELECT * FROM {{ ref('int_subteam_sprint_capacity_pct') }}
),

sprints AS (
    SELECT sprint_id, sprint_state, start_date
    FROM {{ ref('stg_jira_sprints') }}
    WHERE sprint_state = 'closed'
       OR (
           sprint_state = 'active'
           AND start_date IS NOT NULL
           AND end_date   IS NOT NULL
           AND (CURRENT_DATE - start_date::DATE)::DOUBLE
               / NULLIF((end_date::DATE - start_date::DATE)::DOUBLE, 0)
               >= 0.70
       )
)

SELECT
    b.sprint_id,
    b.sprint_name,
    b.board_id,
    b.subteam,
    sp.sprint_state,
    b.burnt_points,
    c.capacity_pct,
    c.subteam_size,
    c.total_available_days,
    -- Guard against division by zero (capacity_pct = 0 means nobody available)
    CASE
        WHEN c.capacity_pct > 0
        THEN FLOOR(b.burnt_points / c.capacity_pct)
        ELSE NULL
    END AS weighted_velocity,
    sp.start_date AS sprint_start_date
FROM burndown b
JOIN capacity  c  USING (sprint_id, subteam)
JOIN sprints   sp USING (sprint_id)
