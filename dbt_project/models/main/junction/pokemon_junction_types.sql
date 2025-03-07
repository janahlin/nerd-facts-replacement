{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(id AS INTEGER) AS pokemon_id,
        UNNEST(STRING_SPLIT(types, ','))::VARCHAR AS type_name
    FROM {{ source('raw', 'pokemon_pokemon') }}
)

SELECT * FROM source
