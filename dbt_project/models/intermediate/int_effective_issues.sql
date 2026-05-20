-- int_effective_issues
-- Apply the parent/subtask retention rule so each unit of work is counted once.
--
-- Rules:
--   1. Subtasks are kept iff they carry a subteam-matching label
--      (subteam_from_label IS NOT NULL — board-default fallback does NOT count
--       for subtasks).
--   2. A subteam-matching subtask is treated as a first-class work item.
--   3. A parent (non-subtask) is kept iff none of its in-sprint subtasks is
--      labelled. If at least one of its in-sprint subtasks carries a subteam
--      label, the parent is dropped (its labelled subtasks supersede it).
--   4. Parents retain their existing subteam resolution (label → board default
--      → NULL); the board-default fallback continues to apply to them.
--   5. A labelled subtask whose parent is not in the sprint is kept as a
--      standalone item — it has no parent to displace, so rule #3 is moot.
--
-- story_points are coalesced to 0 so a labelled subtask with no points
-- contributes zero (not NULL) to downstream sums.

WITH issues AS (
    SELECT * FROM {{ ref('stg_jira_issues') }}
),

labelled_subtasks_per_parent AS (
    SELECT DISTINCT
        sprint_id,
        parent_key
    FROM issues
    WHERE is_subtask = TRUE
      AND subteam_from_label IS NOT NULL
      AND parent_key IS NOT NULL
),

kept_subtasks AS (
    SELECT
        i.issue_key,
        i.sprint_id,
        i.board_id,
        i.summary,
        COALESCE(i.story_points, 0) AS story_points,
        i.t_shirt_size,
        i.status_category,
        i.status,
        i.labels,
        i.assignee,
        i.cf_sprint_id,
        i.cf_sprint_name,
        i.cf_sprint_state,
        i.issue_type,
        i.is_subtask,
        i.parent_key,
        i.subteam_from_label,
        i.subteam_from_label AS subteam,
        i._extracted_at
    FROM issues i
    WHERE i.is_subtask = TRUE
      AND i.subteam_from_label IS NOT NULL
),

kept_parents AS (
    SELECT
        i.issue_key,
        i.sprint_id,
        i.board_id,
        i.summary,
        COALESCE(i.story_points, 0) AS story_points,
        i.t_shirt_size,
        i.status_category,
        i.status,
        i.labels,
        i.assignee,
        i.cf_sprint_id,
        i.cf_sprint_name,
        i.cf_sprint_state,
        i.issue_type,
        i.is_subtask,
        i.parent_key,
        i.subteam_from_label,
        i.subteam,
        i._extracted_at
    FROM issues i
    LEFT JOIN labelled_subtasks_per_parent ls
        ON ls.sprint_id = i.sprint_id AND ls.parent_key = i.issue_key
    WHERE COALESCE(i.is_subtask, FALSE) = FALSE
      AND ls.parent_key IS NULL
)

SELECT * FROM kept_subtasks
UNION ALL
SELECT * FROM kept_parents
