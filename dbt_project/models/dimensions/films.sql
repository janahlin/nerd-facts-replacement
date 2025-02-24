{{ config(
    materialized = 'table',    
    alias = 'films',
    on_schema_change = 'append'
) }}

select     
    CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) AS film_id,
    title as film_title,  -- använd film_title här om det är den transformerade kolumnen
    episode_id,
    opening_crawl,
    director,
    producer,
    release_date,    
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,
    url  -- om det behövs för felsökning
from {{ source('nerd_facts_raw', 'films') }}
