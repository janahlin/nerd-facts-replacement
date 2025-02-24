{{
  config(
    materialized = 'table'
  )
}}

WITH source AS (
  SELECT 
    url, 
    name,
    rotation_period,
    orbital_period,
    diameter,
    climate,
    gravity,
    terrain,
    surface_water,
    population,
    created,
    edited
  FROM {{ source('raw', 'planets') }}
)

SELECT
  CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) AS planet_id,
  name AS planet_name,
  CASE WHEN rotation_period = 'unknown' THEN NULL ELSE rotation_period::INTEGER END AS rotation_period_hours,
  CASE WHEN orbital_period = 'unknown' THEN NULL ELSE orbital_period::INTEGER END AS orbital_period_days,
  CASE WHEN diameter = 'unknown' THEN NULL ELSE diameter::INTEGER END AS diameter_km,
  climate,
  gravity,
  terrain,
  CASE WHEN surface_water = 'unknown' THEN NULL ELSE surface_water::INTEGER END AS surface_water_percentage,
  CASE WHEN population = 'unknown' THEN NULL ELSE population::BIGINT END AS population,
  created::TIMESTAMP AS created_at,
  edited::TIMESTAMP AS updated_at
FROM source