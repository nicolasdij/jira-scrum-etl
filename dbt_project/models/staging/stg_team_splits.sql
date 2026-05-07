-- stg_team_splits
-- Validate and join team-split entries to known sprint IDs.
-- Rows with unresolvable sprint names are excluded with a warning comment.

WITH source AS (
    SELECT
        sprint_name,
        member_name,
        board,
        subteam,
        days_allocated::DOUBLE AS days_allocated
    FROM {{ source('raw', 'raw_team_splits') }}
    WHERE sprint_name IS NOT NULL
      AND member_name IS NOT NULL
      AND board       IS NOT NULL
      -- Only include rows where days_allocated > 0 (blanks = no split)
      AND days_allocated IS NOT NULL
      AND days_allocated > 0
),

-- Resolve sprint_name → sprint_id
validated AS (
    SELECT
        s.sprint_name,
        sp.sprint_id,
        s.member_name,
        s.board,
        s.subteam,
        s.days_allocated
    FROM source s
    INNER JOIN {{ ref('stg_jira_sprints') }} sp
        ON s.sprint_name = sp.sprint_name
)

SELECT * FROM validated
