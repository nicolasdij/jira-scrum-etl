-- mart_rolling_velocity
-- Rolling-N average weighted velocity per sub-team, ordered by sprint start date.
-- Window size is controlled by the `rolling_window_size` dbt variable (default 5).
--
-- Only rows from closed sprints are included. The mart exposes the rolling
-- average as of each sprint — the most recent row per sub-team is used by
-- mart_capacity_planning.

WITH velocity AS (
    SELECT
        sprint_id,
        sprint_name,
        board_id,
        subteam,
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
        weighted_velocity,
        sprint_start_date,
        sprint_rank,
        ROUND(
            AVG(weighted_velocity) OVER (
                PARTITION BY subteam
                ORDER BY sprint_start_date
                ROWS BETWEEN {{ var('rolling_window_size') - 1 }} PRECEDING AND CURRENT ROW
            ),
            2
        ) AS rolling_avg_velocity,
        COUNT(*) OVER (
            PARTITION BY subteam
            ORDER BY sprint_start_date
            ROWS BETWEEN {{ var('rolling_window_size') - 1 }} PRECEDING AND CURRENT ROW
        ) AS sprints_in_window
    FROM velocity
)

SELECT * FROM with_rolling
