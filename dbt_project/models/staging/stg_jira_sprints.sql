{{ config(materialized='table') }}
-- stg_jira_sprints
-- Clean sprint records: parse dates, normalise state, derive sprint_number.
--
-- Deduplication: when a board's JQL also includes sprints from other boards
-- (e.g. the NFR board's filter pulls in DELI sprints), the same sprint_id can
-- appear in raw_jira_sprints under multiple board_id values. We keep one row
-- per sprint_id, preferring the row whose board_key (from board_config) is a
-- prefix of the sprint_name (e.g. 'DELI Sprint 6' → board 'DELI' = 74).

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_jira_sprints') }}
),

deduped AS (
    SELECT s.*
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.sprint_id
                ORDER BY
                    CASE
                        WHEN bc.board_key IS NULL                          THEN 2
                        WHEN s.sprint_name LIKE bc.board_key || '%'        THEN 0
                        ELSE 1
                    END,
                    s._extracted_at DESC
            ) AS rn
        FROM source s
        LEFT JOIN {{ ref('board_config') }} bc
            ON bc.board_id = s.board_id::INTEGER
    ) s
    WHERE s.rn = 1
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
    FROM deduped
    WHERE sprint_id IS NOT NULL
)

SELECT
    c.*,
    COALESCE(
        CASE WHEN c.start_date IS NOT NULL AND c.end_date IS NOT NULL THEN (
            SELECT COUNT(*)::INTEGER
            FROM generate_series(c.start_date::DATE, c.end_date::DATE - INTERVAL '1 day', INTERVAL '1 day') gs(d)
            WHERE DAYOFWEEK(d) BETWEEN 1 AND 5
        ) END,
        10
    ) AS sprint_working_days
FROM cleaned c
