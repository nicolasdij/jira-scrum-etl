{{ config(materialized='table') }}
-- mart_sprint_plan
-- Planned story points vs capacity per subteam for the next target sprint.
-- Aggregates only issues where include = TRUE.
--
-- Scope: only the *current* target sprint per board (earliest state='future' sprint),
-- mirroring what the prepopulator targets. Rows for sprints that have since
-- transitioned to active/closed linger in stg_sprint_planning (the prepop never
-- deletes rows for past sprints), but they're filtered out here so the Sprint Plan
-- output reflects only the sprint actually being planned right now.

WITH next_future_sprint AS (
    SELECT bc.board_key AS board, s.sprint_name
    FROM (
        SELECT board_id, sprint_name,
               ROW_NUMBER() OVER (PARTITION BY board_id ORDER BY start_date ASC) AS rn
        FROM {{ ref('stg_jira_sprints') }}
        WHERE sprint_state = 'future' AND start_date IS NOT NULL
    ) s
    JOIN {{ ref('board_config') }} bc ON s.board_id = bc.board_id
    WHERE s.rn = 1
),

planned AS (
    SELECT
        sp.sprint_name,
        sp.board,
        sp.subteam,
        SUM(CASE WHEN sp.include THEN COALESCE(sp.planned_points, 0) ELSE 0 END) AS planned_points,
        COUNT(CASE WHEN sp.include THEN 1 END)                                   AS included_issues,
        COUNT(*)                                                                  AS total_issues
    FROM {{ ref('stg_sprint_planning') }} sp
    JOIN next_future_sprint nfs USING (board, sprint_name)
    WHERE sp.subteam IS NOT NULL AND TRIM(sp.subteam) != ''
    GROUP BY sp.sprint_name, sp.board, sp.subteam
),

capacity AS (
    SELECT
        c.sprint_name,
        bc.board_key                AS board,
        c.subteam,
        c.subteam_size,
        c.total_available_days,
        c.ideal_days
    FROM {{ ref('int_subteam_sprint_capacity_pct') }} c
    JOIN {{ ref('board_config') }} bc ON c.board_id = bc.board_id
),

-- recommended_capacity_points already accounts for rolling velocity × this sprint's
-- availability percentage, making it the most useful capacity reference for planning.
recommended AS (
    SELECT subteam, recommended_capacity_points AS subteam_capacity
    FROM {{ ref('mart_capacity_planning') }}
)

SELECT
    p.sprint_name,
    p.board,
    p.subteam,
    p.planned_points,
    r.subteam_capacity,
    p.included_issues,
    p.total_issues,
    c.subteam_size,
    c.total_available_days,
    c.ideal_days
FROM planned p
LEFT JOIN capacity c USING (sprint_name, board, subteam)
LEFT JOIN recommended r USING (subteam)
ORDER BY p.board, p.subteam
