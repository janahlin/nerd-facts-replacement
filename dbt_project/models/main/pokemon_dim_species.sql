{{ config(materialized='table') }}

WITH source AS (
    SELECT
        id AS species_id,
        name AS species_name,
        color.name as color,
        habitat,
        shape,
        base_happiness,
        capture_rate
    FROM {{ source('raw', 'pokemon_pokemon_species') }}
)

SELECT * FROM source
