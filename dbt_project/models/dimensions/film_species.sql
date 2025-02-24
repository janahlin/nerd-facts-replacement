{{ config(
    materialized = 'table',    
    alias = 'film_species',
    on_schema_change = 'append'
) }}

with raw_films as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as film_id,
        species
    from {{ source('nerd_facts_raw', 'films') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/species/" som delimiter.
    select
        film_id,
        regexp_split_to_array(species, 'https://swapi.dev/api/species/') as species_array
    from raw_films
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        film_id,
        unnest(species_array) as species_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        film_id,
        species_part
    from unnested
    where species_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        film_id,
        trim(species_part, '/') as species_number
    from filtered
)
select
    film_id,
    CAST(species_number AS INTEGER) as species_id
from cleaned
