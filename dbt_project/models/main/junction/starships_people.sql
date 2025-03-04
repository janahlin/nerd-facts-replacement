{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(REGEXP_REPLACE(UNNEST(pilots), '.*/people/', '') AS INTEGER) AS character_id,
        CAST(uid AS INTEGER) AS starship_id
    FROM {{ source('raw', 'starships') }}
    WHERE pilots IS NOT NULL AND pilots <> '[]'
)

SELECT * FROM source
