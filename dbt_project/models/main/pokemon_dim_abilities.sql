{{ config(materialized='table') }}

WITH source AS (
    SELECT
        id AS ability_id,
        name AS ability_name,
        is_main_series
    FROM {{ source('raw', 'pokemon_ability') }}
)

SELECT * FROM source
