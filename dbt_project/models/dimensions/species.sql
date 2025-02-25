{{ config(
    materialized = 'table',    
    alias = 'species',
    on_schema_change = 'append'
) }}

select 
    CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as species_id,
    name,
    classification,
    designation,
    average_height,
    skin_colors,
    hair_colors,
    eye_colors,
    average_lifespan,    
    language,        
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,
    url
from {{ source('nerd_facts_raw', 'species') }}