{{ config(
    materialized = 'table',    
    alias = 'vehicles',
    on_schema_change = 'append'
) }}

select 
    CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as vehicle_id,
    name,
    model,
    manufacturer,
    cost_in_credits,
    length,
    max_atmosphering_speed,
    crew,
    passengers,
    cargo_capacity,
    consumables,
    vehicle_class,    
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,
    url
from {{ source('nerd_facts_raw', 'vehicles') }}