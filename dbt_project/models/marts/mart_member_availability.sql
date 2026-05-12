-- mart_member_availability
-- Per-member available working days per sprint, excluding team splits.
-- Covers active and future sprints only — intended as a reference when
-- entering split allocations in the Team Splits input sheet.

SELECT
    a.sprint_name,
    a.member_name,
    a.board,
    a.subteam,
    a.availability_days_excl_splits,
    a.base_days,
    a.days_off,
    a.holiday_days
FROM {{ ref('int_member_sprint_availability') }} a
WHERE a.sprint_state IN ('active', 'future')
ORDER BY a.sprint_name, a.subteam, a.member_name
