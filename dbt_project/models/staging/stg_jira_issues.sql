-- stg_jira_issues
-- Clean issues: resolve story points, status category, and sub-team via the
-- first matching sub-team label in the labels array.
-- Issues with no matching sub-team label are kept with subteam = NULL.

WITH source AS (
    SELECT * FROM raw.raw_jira_issues
),

-- Explode the JSON labels array into individual rows, keeping position index
-- so we can pick the first matching sub-team label.
labels_exploded AS (
    SELECT
        issue_key,
        sprint_id,
        board_id,
        TRIM(label_value)          AS label,
        -- json_array_length / unnest index — DuckDB uses generate_subscripts
        ROW_NUMBER() OVER (
            PARTITION BY issue_key
            ORDER BY label_index
        )                          AS label_pos
    FROM source,
    LATERAL (
        SELECT
            UNNEST(json_extract_string(labels, '$[*]')) AS label_value,
            GENERATE_SUBSCRIPTS(
                json_extract_string(labels, '$[*]'), 1
            ) AS label_index
    )
),

-- Join labels to the subteam_labels seed.
-- Label uniqueness across boards is guaranteed by design, so no board filter needed.
label_subteam_clean AS (
    SELECT
        le.issue_key,
        sl.subteam,
        le.label_pos
    FROM labels_exploded le
    INNER JOIN {{ ref('subteam_labels') }} sl
        ON le.label = sl.label
),

first_subteam AS (
    SELECT DISTINCT ON (issue_key)
        issue_key,
        subteam
    FROM label_subteam_clean
    ORDER BY issue_key, label_pos
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
