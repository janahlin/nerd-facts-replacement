{{ config(materialized='table') }}

WITH source AS (
    SELECT
        id AS pokemon_id,
        name AS pokemon_name,
        height,
        weight,
        types,
        base_experience
    FROM {{ source('raw', 'pokemon_pokemon') }}
)

SELECT * FROM source
