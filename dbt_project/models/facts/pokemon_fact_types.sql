{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(pokemon_id AS INTEGER) AS pokemon_id,
        CAST(type_id AS INTEGER) AS type_id
    FROM {{ ref('pokemon_junction_types') }}
)

SELECT * FROM source
