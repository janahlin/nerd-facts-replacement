{{
  config(
    materialized = 'table'
  )
}}

WITH source AS (
  SELECT 
    url, 
    name,
    height,
    mass,
    hair_color,
    skin_color,
    eye_color,
    birth_year,
    gender,
    homeworld,
    species,
    created,
    edited
  FROM {{ source('raw', 'people') }}
)

SELECT
  CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) AS person_id,
  name AS person_name,
  CASE WHEN height = 'unknown' THEN NULL ELSE CAST(height AS INTEGER) END AS height_cm,
  CASE WHEN mass = 'unknown' THEN NULL ELSE CAST(REPLACE(mass, ',', '') AS FLOAT) END AS mass_kg,
  hair_color,
  skin_color,
  eye_color,
  birth_year,
  gender,
  CAST(REGEXP_EXTRACT(homeworld, '/([0-9]+)/?$', 1) AS INTEGER) AS planet_id,
  species,
  created::TIMESTAMP AS created_at,
  edited::TIMESTAMP AS updated_at
FROM source