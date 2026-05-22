-- stg_jira_issues
-- Clean issues: resolve story points, status category, and sub-team via the
-- first matching sub-team label in the labels array.
-- Issues with no matching sub-team label are kept with subteam_from_label = NULL;
-- `subteam` additionally falls back to the board's default_subteam when no label matches.
--
-- `labels` is stored as a JSON array string, e.g. '["delivery-sol-arch","other"]'.
-- We use DuckDB's list functions (from_json, list_contains, list_position) to
-- identify the first array element that matches a known subteam label, avoiding
-- GENERATE_SUBSCRIPTS / LATERAL syntax which are PostgreSQL-specific.
--
-- Deduplication: the same Jira issue can be returned by multiple boards' JQL
-- queries (e.g. a DELI issue picked up by the NFR board), producing multiple
-- rows in raw_jira_issues with the same (issue_key, sprint_id) but different
-- board_id values. We collapse these to a single row per (issue_key, sprint_id),
-- preferring the row whose board_id matches the sprint's owning board (from
-- stg_jira_sprints). The retained `board_id` is then overridden to the sprint's
-- owning board so downstream models can rely on `board_id` as the sprint board.

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_jira_issues') }}
),

sprint_owner AS (
    SELECT sprint_id, board_id AS owner_board_id
    FROM {{ ref('stg_jira_sprints') }}
),

-- One row per (issue_key, sprint_id). Tie-break by:
--   1. board_id matching the sprint's owning board
--   2. most recent _extracted_at
deduped AS (
    SELECT s.*
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.issue_key, s.sprint_id
                ORDER BY
                    CASE
                        WHEN so.owner_board_id IS NULL          THEN 1
                        WHEN so.owner_board_id = s.board_id     THEN 0
                        ELSE 2
                    END,
                    s._extracted_at DESC
            ) AS rn
        FROM source s
        LEFT JOIN sprint_owner so ON so.sprint_id = s.sprint_id
    ) s
    WHERE s.rn = 1
),

-- For each issue, find the subteam whose label appears earliest in the labels array.
-- list_position returns the 1-based index of the element, or 0 if not present.
-- DISTINCT ON + ORDER BY pos picks the match with the lowest position (= first label).
first_subteam AS (
    SELECT DISTINCT ON (s.issue_key)
        s.issue_key,
        sl.subteam
    FROM deduped s
    CROSS JOIN {{ ref('subteam_labels') }} sl
    WHERE list_contains(
        from_json(s.labels, '["VARCHAR"]'),
        sl.label
    )
    ORDER BY
        s.issue_key,
        list_position(from_json(s.labels, '["VARCHAR"]'), sl.label)
),

base AS (
    SELECT
        s.issue_key,
        s.sprint_id,
        -- Attribute the issue to the sprint's owning board so a DELI issue
        -- fetched via the NFR board is still counted as DELI work.
        COALESCE(so.owner_board_id, s.board_id) AS board_id,
        s.summary,
        s.story_points::DOUBLE        AS story_points,
        s.t_shirt_size::DOUBLE        AS t_shirt_size,
        LOWER(s.status_category)      AS status_category,
        s.status,
        s.labels,
        s.assignee,
        s.cf_sprint_id,
        s.cf_sprint_name,
        s.cf_sprint_state,
        s.issue_type,
        s.is_subtask,
        s.parent_key,
        s._extracted_at
    FROM deduped s
    LEFT JOIN sprint_owner so ON so.sprint_id = s.sprint_id
)

SELECT
    b.*,
    fs.subteam                                            AS subteam_from_label,
    COALESCE(fs.subteam, NULLIF(bc.default_subteam, '')) AS subteam
FROM base b
LEFT JOIN first_subteam fs USING (issue_key)
LEFT JOIN {{ ref('board_config') }} bc ON b.board_id = bc.board_id
