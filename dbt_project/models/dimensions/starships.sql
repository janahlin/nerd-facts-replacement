{{ config(
    materialized = 'table',    
    alias = 'starships',
    on_schema_change = 'append'
) }}

select 
    CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as starship_id,
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
    hyperdrive_rating,
    mglt,
    starship_class,   
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,
    url
from {{ source('nerd_facts_raw', 'starships') }}