-- stg_jira_issues
-- Clean issues: resolve story points, status category, and sub-team via the
-- first matching sub-team label in the labels array.
-- Issues with no matching sub-team label are kept with subteam = NULL.
--
-- `labels` is stored as a JSON array string, e.g. '["delivery-sol-arch","other"]'.
-- We use DuckDB's list functions (from_json, list_contains, list_position) to
-- identify the first array element that matches a known subteam label, avoiding
-- GENERATE_SUBSCRIPTS / LATERAL syntax which are PostgreSQL-specific.

WITH source AS (
    SELECT * FROM raw.raw_jira_issues
),

-- For each issue, find the subteam whose label appears earliest in the labels array.
-- list_position returns the 1-based index of the element, or 0 if not present.
-- DISTINCT ON + ORDER BY pos picks the match with the lowest position (= first label).
first_subteam AS (
    SELECT DISTINCT ON (s.issue_key)
        s.issue_key,
        sl.subteam
    FROM source s
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
        s.board_id,
        s.summary,
        s.story_points::DOUBLE        AS story_points,
        LOWER(s.status_category)      AS status_category,
        s.labels,
        s.assignee,
        s.cf_sprint_id,
        s.cf_sprint_name,
        s.cf_sprint_state,
        s._extracted_at
    FROM source s
)

SELECT
    b.*,
    fs.subteam
FROM base b
LEFT JOIN first_subteam fs USING (issue_key)
