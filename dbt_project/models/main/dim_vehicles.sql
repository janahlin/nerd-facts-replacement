{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS vehicle_id,
        name,
        model,
        manufacturer,
        CAST(NULLIF(REPLACE(length, ',', ''), 'unknown') AS FLOAT) AS length_m,
        CAST(COALESCE(NULLIF(crew, 'unknown'), '0') AS INTEGER) AS crew_size,
        CAST(COALESCE(NULLIF(passengers, 'unknown'), '0') AS INTEGER) AS passenger_capacity,
        vehicle_class
    FROM {{ source('raw', 'vehicles') }}
)

SELECT * FROM source
