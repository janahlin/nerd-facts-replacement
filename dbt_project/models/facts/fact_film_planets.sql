{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(film_id AS INTEGER) AS film_id,
        CAST(planet_id AS INTEGER) AS planet_id
    FROM {{ ref('films_planets') }}
)

SELECT * FROM source
