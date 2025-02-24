{{ config(
    materialized = 'table',    
    alias = 'people_species',
    on_schema_change = 'append'
) }}

with raw_people as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as person_id,
        species
    from {{ source('nerd_facts_raw', 'people') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/species/" som delimiter.
    select
        person_id,
        regexp_split_to_array(species, 'https://swapi.dev/api/species/') as species_array
    from raw_people
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        person_id,
        unnest(species_array) as species_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        person_id,
        species_part
    from unnested
    where species_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        person_id,
        trim(species_part, '/') as species_number
    from filtered
)
select
    person_id,
    CAST(species_number AS INTEGER) as species_id
from cleaned
