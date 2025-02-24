{{ config(
    materialized = 'table',    
    alias = 'film_planets',
    on_schema_change = 'append'
) }}

with raw_films as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as film_id,
        planets
    from {{ source('nerd_facts_raw', 'films') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/planets/" som delimiter.
    select
        film_id,
        regexp_split_to_array(planets, 'https://swapi.dev/api/planets/') as planet_array
    from raw_films
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        film_id,
        unnest(planet_array) as planet_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        film_id,
        planet_part
    from unnested
    where planet_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        film_id,
        trim(planet_part, '/') as planet_number
    from filtered
)
select
    film_id,
    CAST(planet_number AS INTEGER) as planet_id
from cleaned
