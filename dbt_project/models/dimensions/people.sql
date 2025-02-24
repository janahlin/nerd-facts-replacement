{{ config(
    materialized = 'table',    
    alias = 'people',
    on_schema_change = 'append'
) }}

with raw_people as (
    select 
        *,
        CASE 
            WHEN height = 'unknown' THEN NULL 
            ELSE CAST(height AS INTEGER)
        END AS height_cm,
        CASE 
            WHEN mass = 'unknown' THEN NULL 
            ELSE CAST(REPLACE(mass, ',', '') AS FLOAT)
        END AS mass_kg
    from {{ source('nerd_facts_raw', 'people') }}
)


select 
    CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as person_id,
    name as person_name,
    height_cm,
    mass_kg,
    hair_color,
    skin_color,        
    eye_color,
    birth_year,
    gender,    
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,
    url  -- om det behövs för felsökning
from raw_people
