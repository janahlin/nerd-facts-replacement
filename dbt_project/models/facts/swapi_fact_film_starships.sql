{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(film_id AS INTEGER) AS film_id,
        CAST(starship_id AS INTEGER) AS starship_id
    FROM {{ ref('swapi_junction_films_starships') }}
)

SELECT * FROM source
