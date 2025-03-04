{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS species_id,
        name,
        classification,
        designation,
        -- Fix: Remove 'n/a' and 'unknown' before casting
        CAST(NULLIF(REGEXP_REPLACE(average_height, '[^0-9.]', '', 'g'), '') AS FLOAT) AS avg_height_cm,
        CAST(NULLIF(REGEXP_REPLACE(average_lifespan, '[^0-9.]', '', 'g'), '') AS INTEGER) AS avg_lifespan,
        hair_colors,
        skin_colors,
        eye_colors,
        language
    FROM {{ source('raw', 'species') }}
)

SELECT * FROM source
