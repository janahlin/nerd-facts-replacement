{{ config(
    materialized = 'table',    
    alias = 'film_characters',
    on_schema_change = 'append'
) }}

with raw_films as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as film_id,
        characters
    from {{ source('nerd_facts_raw', 'films') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/people/" som delimiter.
    select
        film_id,
        regexp_split_to_array(characters, 'https://swapi.dev/api/people/') as character_array
    from raw_films
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        film_id,
        unnest(character_array) as character_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        film_id,
        character_part
    from unnested
    where character_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        film_id,
        trim(character_part, '/') as person_number
    from filtered
)
select
    film_id,
    CAST(person_number AS INTEGER) as person_id
from cleaned
