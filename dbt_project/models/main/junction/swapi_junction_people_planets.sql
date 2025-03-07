{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS character_id,
        CAST(REGEXP_REPLACE(homeworld, '.*/planets/', '') AS INTEGER) AS planet_id
    FROM {{ source('raw', 'people') }}
)

SELECT * FROM source
