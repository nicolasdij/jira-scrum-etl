-- stg_jira_sprints
-- Clean sprint records: parse dates, normalise state, derive sprint_number.

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_jira_sprints') }}
),

cleaned AS (
    SELECT
        sprint_id::INTEGER                          AS sprint_id,
        board_id::INTEGER                           AS board_id,
        sprint_name,
        LOWER(state)                                AS sprint_state,
        -- Attempt to parse ISO-8601 date strings; NULL if unparseable
        TRY_CAST(start_date AS TIMESTAMP)           AS start_date,
        TRY_CAST(end_date   AS TIMESTAMP)           AS end_date,
        TRY_CAST(complete_date AS TIMESTAMP)        AS complete_date,

        -- Derive sprint_number from name pattern "Sprint N" (case-insensitive)
        TRY_CAST(
            REGEXP_EXTRACT(sprint_name, '[Ss]print\s+(\d+)', 1) AS INTEGER
        )                                           AS sprint_number,

        _extracted_at
    FROM source
    WHERE sprint_id IS NOT NULL
)

SELECT * FROM cleaned
