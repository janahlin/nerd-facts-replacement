{{ config(materialized='table') }}

WITH source AS (
    SELECT
        id AS type_id,
        name AS type_name
    FROM {{ source('raw', 'pokemon_type') }}
)

SELECT * FROM source
