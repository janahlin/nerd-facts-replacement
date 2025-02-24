{{ config(
    materialized = 'table',    
    alias = 'people_starships',
    on_schema_change = 'append'
) }}

with raw_people as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as person_id,
        starships
    from {{ source('nerd_facts_raw', 'people') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/starships/" som delimiter.
    select
        person_id,
        regexp_split_to_array(starships, 'https://swapi.dev/api/starships/') as starship_array
    from raw_people
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        person_id,
        unnest(starship_array) as starship_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        person_id,
        starship_part
    from unnested
    where starship_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        person_id,
        trim(starship_part, '/') as starship_number
    from filtered
)
select
    person_id,
    CAST(starship_number AS INTEGER) as starship_id
from cleaned
