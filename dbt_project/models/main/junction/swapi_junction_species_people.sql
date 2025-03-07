{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(REGEXP_REPLACE(UNNEST(people), '.*/people/', '') AS INTEGER) AS character_id,
        CAST(uid AS INTEGER) AS species_id
    FROM {{ source('raw', 'species') }}
    WHERE people IS NOT NULL AND people <> '[]'
)

SELECT * FROM source
