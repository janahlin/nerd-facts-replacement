{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(film_id AS INTEGER) AS film_id,
        CAST(vehicle_id AS INTEGER) AS vehicle_id
    FROM {{ ref('films_vehicles') }}
)

SELECT * FROM source
