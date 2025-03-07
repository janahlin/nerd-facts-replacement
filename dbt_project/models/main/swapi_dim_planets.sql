{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS planet_id,
        name,
        CAST(COALESCE(NULLIF(diameter, 'unknown'), '0') AS INTEGER) AS diameter_km,
        climate,
        terrain,
        CAST(COALESCE(NULLIF(population, 'unknown'), '0') AS BIGINT) AS population,
        -- Fix: Extract only numeric/decimal values, default to NULL if empty
        CAST(NULLIF(REGEXP_REPLACE(gravity, '[^0-9.]', '', 'g'), '') AS FLOAT) AS gravity
    FROM {{ source('raw', 'planets') }}
)

SELECT * FROM source
