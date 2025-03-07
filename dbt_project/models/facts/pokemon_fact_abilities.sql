{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(pokemon_id AS INTEGER) AS pokemon_id,
        CAST(ability_id AS INTEGER) AS ability_id
    FROM {{ ref('pokemon_junction_abilities') }}
)

SELECT * FROM source
