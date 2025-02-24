{{ config(
    materialized = 'table',    
    alias = 'species_planets',
    on_schema_change = 'append'
) }}

with raw_species as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as species_id,
        homeworld
    from {{ source('nerd_facts_raw', 'species') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/homeworld/" som delimiter.
    select
        species_id,
        regexp_split_to_array(homeworld, 'https://swapi.dev/api/planets/') as planet_array
    from raw_species
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        species_id,
        unnest(planet_array) as planet_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        species_id,
        planet_part
    from unnested
    where planet_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        species_id,
        trim(planet_part, '/') as planet_number
    from filtered
)
select
    species_id,
    CAST(planet_number AS INTEGER) as planet_id
from cleaned
