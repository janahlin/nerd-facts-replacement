{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(pokemon_id AS INTEGER) AS pokemon_id,
        CAST(move_id AS INTEGER) AS move_id
    FROM {{ ref('pokemon_junction_moves') }}
)

SELECT * FROM source
