{{ config(materialized='table') }}
-- mart_sprint_plan
-- Planned story points vs capacity per subteam for the next target sprint.
-- Aggregates only issues where include = TRUE.

WITH planned AS (
    SELECT
        sprint_name,
        board,
        subteam,
        SUM(CASE WHEN include THEN COALESCE(planned_points, 0) ELSE 0 END) AS planned_points,
        COUNT(CASE WHEN include THEN 1 END)                                 AS included_issues,
        COUNT(*)                                                             AS total_issues
    FROM {{ ref('stg_sprint_planning') }}
    GROUP BY sprint_name, board, subteam
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
