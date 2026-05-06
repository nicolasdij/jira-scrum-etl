-- int_sprint_burndown
-- Story points of "done" issues per sprint per sub-team.
-- This is the numerator for weighted velocity.
--
-- Burndown definition: issues whose status_category = 'done' at the time
-- of extraction are counted as burnt down on their current sprint.

WITH done_issues AS (
    SELECT
        issue_key,
        sprint_id,
        board_id,
        subteam,
        COALESCE(story_points, 0) AS story_points
    FROM {{ ref('stg_jira_issues') }}
    WHERE status_category = 'done'
      AND subteam IS NOT NULL
),

aggregated AS (
    SELECT
        d.sprint_id,
        s.sprint_name,
        d.board_id,
        d.subteam,
        SUM(d.story_points) AS burnt_points
    FROM done_issues d
    JOIN {{ ref('stg_jira_sprints') }} s USING (sprint_id)
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM aggregated
