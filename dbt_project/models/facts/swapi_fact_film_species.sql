{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(film_id AS INTEGER) AS film_id,
        CAST(species_id AS INTEGER) AS species_id
    FROM {{ ref('swapi_junction_films_species') }}
)

SELECT * FROM source
