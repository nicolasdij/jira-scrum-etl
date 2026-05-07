-- stg_public_holidays
-- Explode the comma-separated `regions` column into one row per region.
-- Filters to working days only.

WITH source AS (
    SELECT
        TRY_CAST(holiday_date AS DATE) AS holiday_date,
        holiday_name,
        regions
    FROM {{ source('raw', 'raw_public_holidays') }}
    WHERE holiday_date IS NOT NULL
),

exploded AS (
    SELECT
        holiday_date,
        holiday_name,
        TRIM(region_value) AS region
    FROM source,
    UNNEST(string_split(regions, ',')) AS t(region_value)
),

working_days_only AS (
    SELECT *
    FROM exploded
    WHERE DAYOFWEEK(holiday_date) NOT IN (0, 6)   -- DuckDB: 0=Sunday, 6=Saturday
)

SELECT * FROM working_days_only
