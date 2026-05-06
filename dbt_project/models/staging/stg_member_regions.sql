-- stg_member_regions
-- Explode the comma-separated `regions` column from the member_regions seed
-- into one row per (member_name, region) pair.

WITH source AS (
    SELECT member_name, regions
    FROM {{ ref('member_regions') }}
),

exploded AS (
    SELECT
        member_name,
        TRIM(region_value) AS region
    FROM source,
    UNNEST(string_split(regions, ',')) AS t(region_value)
)

SELECT * FROM exploded
