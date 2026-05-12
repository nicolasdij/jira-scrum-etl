-- int_subteam_sprint_capacity_pct
-- Sub-team capacity as a percentage of ideal capacity, per sprint.
--
-- Formula: FLOOR( SUM(availability_days) / (sprint_working_days * COUNT(member_name)) )
-- Expressed as a decimal fraction (e.g. 0.67 for 67%).
-- Members who appear in multiple sub-teams contribute to each independently.

WITH per_member AS (
    SELECT
        sprint_id,
        sprint_name,
        board_id,
        subteam,
        member_name,
        availability_days,
        base_days
    FROM {{ ref('int_member_sprint_availability') }}
),

aggregated AS (
    SELECT
        sprint_id,
        sprint_name,
        board_id,
        subteam,
        SUM(availability_days)                                          AS total_available_days,
        COUNT(DISTINCT member_name)                                     AS subteam_size,
        MAX(base_days) * COUNT(DISTINCT member_name)                    AS ideal_days,
        FLOOR(
            SUM(availability_days) /
            NULLIF(MAX(base_days) * COUNT(DISTINCT member_name), 0)
            * 100
        ) / 100.0                                                       AS capacity_pct
    FROM per_member
    GROUP BY 1, 2, 3, 4
)

SELECT
    sprint_id,
    sprint_name,
    board_id,
    subteam,
    subteam_size,
    total_available_days,
    ideal_days,
    capacity_pct
FROM aggregated
