{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS starship_id,
        name,
        model,
        manufacturer,
        CAST(COALESCE(NULLIF(REPLACE(length, ',', ''), 'unknown'), '0') AS FLOAT) AS length_m,
        CAST(
            COALESCE(
                NULLIF(REGEXP_REPLACE(crew, '[^0-9]', '', 'g'), ''), '0'
            ) AS INTEGER
        ) AS crew_size,
        CAST(
            COALESCE(
                NULLIF(REGEXP_REPLACE(passengers, '[^0-9]', '', 'g'), ''), '0'
            ) AS INTEGER
        ) AS passenger_capacity,
        starship_class,
        CAST(COALESCE(NULLIF(hyperdrive_rating, 'unknown'), '0') AS FLOAT) AS hyperdrive_rating
    FROM {{ source('raw', 'starships') }}
)

SELECT * FROM source
