{{ config(materialized='table') }}
-- mart_sprint_plan
-- Planned/committed story points vs subteam capacity for two target sprints per
-- board: the currently active sprint and the earliest future sprint.
--
-- Future rows  → planned points come from the Sprint Items sheet
--                (stg_sprint_planning, gated by include = TRUE).
-- Active rows  → planned points come straight from Jira via int_effective_issues
--                (no opt-in: anything committed in the sprint counts).
--
-- The two halves share the same shape and are UNION ALL'd before joining
-- capacity and recommended-capacity references. sprint_state on the output row
-- tells the reader which semantics apply to planned_points.

WITH target_sprints AS (
    -- Earliest start_date per (board, sprint_state) — picks the single
    -- in-flight sprint and the single next-up sprint per board.
    SELECT
        bc.board_key   AS board,
        s.sprint_id,
        s.sprint_name,
        s.sprint_state,
        s.start_date   AS sprint_start_date
    FROM (
        SELECT
            sprint_id, sprint_name, board_id, sprint_state, start_date,
            ROW_NUMBER() OVER (
                PARTITION BY board_id, sprint_state ORDER BY start_date ASC
            ) AS rn
        FROM {{ ref('stg_jira_sprints') }}
        WHERE sprint_state IN ('active', 'future') AND start_date IS NOT NULL
    ) s
    JOIN {{ ref('board_config') }} bc ON s.board_id = bc.board_id
    WHERE s.rn = 1
),

-- Future-sprint planned points: sheet-driven, same logic the mart has always
-- used. include filter + retention rule already applied upstream.
future_planned AS (
    SELECT
        ts.sprint_name,
        ts.board,
        sp.subteam,
        SUM(CASE WHEN sp.include THEN COALESCE(sp.planned_points, 0) ELSE 0 END)
                                                AS planned_points,
        COUNT(CASE WHEN sp.include THEN 1 END)  AS included_issues,
        COUNT(*)                                AS total_issues
    FROM target_sprints ts
    JOIN {{ ref('stg_sprint_planning') }} sp USING (board, sprint_name)
    WHERE ts.sprint_state = 'future'
      AND sp.subteam IS NOT NULL AND TRIM(sp.subteam) != ''
    GROUP BY ts.sprint_name, ts.board, sp.subteam
),

-- Active-sprint committed points: Jira-driven via int_effective_issues
-- (parent/subtask retention already applied). Cancelled excluded for parity
-- with the sheet path; everything else counts toward commitment regardless
-- of status (done items are part of the original commitment).
active_committed AS (
    SELECT
        ts.sprint_name,
        bc.board_key AS board,
        ei.subteam,
        SUM(COALESCE(ei.story_points, 0)) AS planned_points,
        COUNT(*)                          AS included_issues,
        COUNT(*)                          AS total_issues
    FROM target_sprints ts
    JOIN {{ ref('int_effective_issues') }} ei
      ON ei.sprint_id = ts.sprint_id
    JOIN {{ ref('board_config') }} bc ON ei.board_id = bc.board_id
    WHERE ts.sprint_state = 'active'
      AND ei.status_category != 'cancelled'
      AND ei.subteam IS NOT NULL AND TRIM(ei.subteam) != ''
    GROUP BY ts.sprint_name, bc.board_key, ei.subteam
),

planned_or_committed AS (
    SELECT * FROM future_planned
    UNION ALL
    SELECT * FROM active_committed
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

-- mart_capacity_planning carries both the active and future target rows per
-- subteam. Joining on (sprint_name, subteam) naturally picks up the matching
-- row: active rows get the closed-only velocity that drove their planning,
-- future rows get the latest rolling row overall.
recommended AS (
    SELECT
        target_sprint                 AS sprint_name,
        subteam,
        recommended_capacity_points   AS subteam_capacity
    FROM {{ ref('mart_capacity_planning') }}
)

SELECT
    p.sprint_name,
    ts.sprint_state,
    strftime(ts.sprint_start_date::DATE, '%Y-%m-%d') AS sprint_start_date,
    p.board,
    p.subteam,
    p.planned_points,
    r.subteam_capacity,
    p.included_issues,
    p.total_issues,
    c.subteam_size,
    c.total_available_days,
    c.ideal_days
FROM planned_or_committed p
JOIN target_sprints ts USING (sprint_name, board)
LEFT JOIN capacity     c USING (sprint_name, board, subteam)
LEFT JOIN recommended  r USING (sprint_name, subteam)
ORDER BY ts.sprint_start_date, p.board, p.subteam
