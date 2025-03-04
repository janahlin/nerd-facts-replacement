{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(film_id AS INTEGER) AS film_id,
        CAST(character_id AS INTEGER) AS character_id
    FROM {{ ref('films_characters') }}
)

SELECT * FROM source
