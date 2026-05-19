{{ config(materialized='view') }}
-- stg_sprint_planning
-- Clean sprint planning rows from the Sprint Planning input sheet.
-- planned_points is cast to DOUBLE; include is normalised to BOOLEAN.

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_sprint_planning') }}
)

SELECT
    issue_key,
    sprint_name,
    board,
    subteam,
    summary,
    status,
    CASE
        WHEN planned_points = '' OR planned_points IS NULL THEN NULL
        ELSE TRY_CAST(planned_points AS DOUBLE)
    END                                                             AS planned_points,
    LOWER(TRIM(COALESCE(include, 'true'))) NOT IN ('false', '0', 'no') AS include,
    _extracted_at
FROM source
WHERE issue_key IS NOT NULL AND issue_key != ''
  AND LOWER(TRIM(COALESCE(status, ''))) != 'cancelled'
