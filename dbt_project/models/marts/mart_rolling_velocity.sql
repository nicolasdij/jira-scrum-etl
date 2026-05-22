-- mart_rolling_velocity
-- Rolling-N average weighted velocity per sub-team, ordered by sprint start date.
-- Window size is controlled by the `rolling_window_size` dbt variable (default 5).
--
-- Rows from closed sprints use a window of the last N closed sprints (including
-- themselves). The currently active sprint, when surfaced by int_sprint_velocity
-- (i.e. ≥70 % elapsed), is averaged together with the N most recent closed
-- sprints — so its window contains N+1 data points. This preserves the full
-- closed-sprint history rather than displacing the oldest one.
--
-- The most recent row per sub-team is consumed by mart_capacity_planning.

WITH velocity AS (
    SELECT
        sprint_id,
        sprint_name,
        board_id,
        subteam,
        sprint_state,
        weighted_velocity,
        sprint_start_date,
        -- Rank sprints per sub-team to support window ordering
        ROW_NUMBER() OVER (
            PARTITION BY subteam
            ORDER BY sprint_start_date
        ) AS sprint_rank
    FROM {{ ref('int_sprint_velocity') }}
    WHERE weighted_velocity IS NOT NULL
),

with_rolling AS (
    SELECT
        sprint_id,
        sprint_name,
        board_id,
        subteam,
        sprint_state,
        weighted_velocity,
        sprint_start_date,
        sprint_rank,
        ROUND(
            CASE
                WHEN sprint_state = 'active' THEN
                    AVG(weighted_velocity) OVER (
                        PARTITION BY subteam
                        ORDER BY sprint_start_date
                        ROWS BETWEEN {{ var('rolling_window_size') }} PRECEDING AND CURRENT ROW
                    )
                ELSE
                    AVG(weighted_velocity) OVER (
                        PARTITION BY subteam
                        ORDER BY sprint_start_date
                        ROWS BETWEEN {{ var('rolling_window_size') - 1 }} PRECEDING AND CURRENT ROW
                    )
            END,
            2
        ) AS rolling_avg_velocity,
        CASE
            WHEN sprint_state = 'active' THEN
                COUNT(*) OVER (
                    PARTITION BY subteam
                    ORDER BY sprint_start_date
                    ROWS BETWEEN {{ var('rolling_window_size') }} PRECEDING AND CURRENT ROW
                )
            ELSE
                COUNT(*) OVER (
                    PARTITION BY subteam
                    ORDER BY sprint_start_date
                    ROWS BETWEEN {{ var('rolling_window_size') - 1 }} PRECEDING AND CURRENT ROW
                )
        END AS sprints_in_window
    FROM velocity
)

SELECT * FROM with_rolling
