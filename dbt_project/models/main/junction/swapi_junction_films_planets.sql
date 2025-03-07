{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS film_id,
        UNNEST(STRING_SPLIT(planets, ','))::INTEGER AS planet_id
    FROM {{ source('raw', 'films') }}
)

SELECT * FROM source
