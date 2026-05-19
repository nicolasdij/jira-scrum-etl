-- int_member_sprint_availability
-- Availability in working days per member per sprint, after deducting:
--   1. Time-off days (from stg_time_off)
--   2. Public holiday days (from stg_public_holidays via stg_member_regions)
--   3. Days allocated to OTHER board/subteam combinations (from stg_team_splits)
--
-- Base capacity per member per sprint = actual weekdays in the sprint window.
-- Availability is floored at 0 (cannot go negative).

WITH base AS (
    -- One row per (member, sprint) combination derived from team_members seed
    -- cross-joined with all closed/active sprints.
    SELECT
        tm.member_name,
        tm.board,
        tm.subteam,
        sp.sprint_id,
        sp.sprint_name,
        sp.start_date::DATE             AS sprint_start_date,
        sp.board_id,
        sp.sprint_state,
        sp.sprint_working_days::DOUBLE AS base_days
    FROM {{ ref('team_members') }} tm
    JOIN {{ ref('stg_jira_sprints') }} sp
        ON tm.board = (
            SELECT board_key
            FROM {{ ref('board_config') }}
            WHERE board_id = sp.board_id
        )
    WHERE sp.sprint_state IN ('closed', 'active', 'future')
),

-- Days lost to time off
time_off_deductions AS (
    SELECT
        t.member_name,
        t.sprint_id,
        t.board,
        SUM(t.days_off) AS days_off
    FROM {{ ref('stg_time_off') }} t
    GROUP BY 1, 2, 3
),

-- Days lost to public holidays per member (via their region mapping)
holiday_deductions AS (
    SELECT
        mr.member_name,
        s.sprint_id,
        COUNT(DISTINCT h.holiday_date) AS holiday_days
    FROM {{ ref('stg_public_holidays') }} h
    JOIN {{ ref('stg_member_regions') }} mr ON h.region = mr.region
    JOIN {{ ref('stg_jira_sprints') }} s
        ON h.holiday_date BETWEEN s.start_date::DATE AND s.end_date::DATE
    GROUP BY 1, 2
),

-- Days allocated to *other* boards or sub-teams (team_splits rows for different
-- board/subteam combos represent time NOT available for the primary assignment)
split_deductions AS (
    SELECT
        ts.member_name,
        ts.sprint_id,
        ts.board,
        ts.subteam,
        SUM(ts.days_allocated) AS days_split_away
    FROM {{ ref('stg_team_splits') }} ts
    GROUP BY 1, 2, 3, 4
),

combined AS (
    SELECT
        b.member_name,
        b.board,
        b.subteam,
        b.sprint_id,
        b.sprint_name,
        b.sprint_start_date,
        b.board_id,
        b.sprint_state,
        b.base_days,
        COALESCE(tod.days_off, 0)           AS days_off,
        COALESCE(hd.holiday_days, 0)        AS holiday_days,
        -- days_allocated for THIS specific subteam/board combo (not deducted from itself)
        COALESCE(
            (
                SELECT SUM(ts2.days_allocated)
                FROM {{ ref('stg_team_splits') }} ts2
                WHERE ts2.member_name = b.member_name
                  AND ts2.sprint_id   = b.sprint_id
                  AND (ts2.board != b.board OR ts2.subteam != b.subteam)
            ),
            0
        )                                   AS days_split_away
    FROM base b
    LEFT JOIN time_off_deductions tod
        ON b.member_name = tod.member_name
       AND b.sprint_id   = tod.sprint_id
       AND b.board        = tod.board
    LEFT JOIN holiday_deductions hd
        ON b.member_name = hd.member_name
       AND b.sprint_id   = hd.sprint_id
)

SELECT
    c.member_name,
    c.board,
    c.subteam,
    c.sprint_id,
    c.sprint_name,
    c.sprint_start_date,
    c.board_id,
    c.sprint_state,
    GREATEST(
        COALESCE(
            -- If Team Splits has an explicit entry for this member's own subteam, use it
            -- directly (the prepopulator pre-fills it with effective days and the user
            -- adjusts it to reflect actual planned capacity).
            (
                SELECT ts.days_allocated
                FROM {{ ref('stg_team_splits') }} ts
                WHERE ts.member_name = c.member_name
                  AND ts.sprint_id   = c.sprint_id
                  AND ts.board       = c.board
                  AND ts.subteam     = c.subteam
            ),
            c.base_days - c.days_off - c.holiday_days - c.days_split_away
        ),
        0.0
    ) AS availability_days,
    GREATEST(c.base_days - c.days_off - c.holiday_days, 0.0) AS availability_days_excl_splits,
    c.base_days,
    c.days_off,
    c.holiday_days,
    c.days_split_away
FROM combined c
