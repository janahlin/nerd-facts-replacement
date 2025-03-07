{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(id AS INTEGER) AS pokemon_id,
        UNNEST(STRING_SPLIT(abilities, ','))::VARCHAR AS ability_name
    FROM {{ source('raw', 'pokemon_pokemon') }}
)

SELECT * FROM source
