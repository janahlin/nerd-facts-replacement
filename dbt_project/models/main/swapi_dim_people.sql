{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS character_id,
        name,
        gender,
        skin_color,
        hair_color,
        eye_color,
        CAST(COALESCE(NULLIF(height, 'unknown'), '0') AS INTEGER) AS height_cm,
        CAST(COALESCE(NULLIF(REPLACE(mass, ',', ''), 'unknown'), '0') AS FLOAT) AS weight_kg,
        birth_year,
        CAST(REGEXP_REPLACE(homeworld, '.*/planets/', '') AS INTEGER) AS homeworld_id
    FROM {{ source('raw', 'people') }}
)

SELECT * FROM source
