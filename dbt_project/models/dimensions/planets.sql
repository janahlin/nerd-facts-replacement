{{ config(
    materialized = 'table',    
    alias = 'planets',
    on_schema_change = 'append'
) }}

with raw_planets as (
    select 
        *,
        CASE WHEN rotation_period = 'unknown' THEN NULL ELSE rotation_period::INTEGER END AS rotation_period_hours,
        CASE WHEN orbital_period = 'unknown' THEN NULL ELSE orbital_period::INTEGER END AS orbital_period_days,
        CASE WHEN diameter = 'unknown' THEN NULL ELSE diameter::INTEGER END AS diameter_km,
        CASE WHEN surface_water = 'unknown' THEN NULL ELSE surface_water::INTEGER END AS surface_water_percentage,
        CASE WHEN population = 'unknown' THEN NULL ELSE population::BIGINT END AS population,
    from {{ source('nerd_facts_raw', 'planets') }}
)

select 
    CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as planet_id,
    name,
    rotation_period_hours,
    orbital_period_days,
    diameter_km,
    climate,
    gravity,
    terrain,
    surface_water_percentage,
    population,
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,
    url
from raw_planets