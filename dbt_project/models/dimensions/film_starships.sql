{{ config(
    materialized = 'table',    
    alias = 'film_starships',
    on_schema_change = 'append'
) }}

with raw_films as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as film_id,
        starships
    from {{ source('nerd_facts_raw', 'films') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/starships/" som delimiter.
    select
        film_id,
        regexp_split_to_array(starships, 'https://swapi.dev/api/starships/') as starship_array
    from raw_films
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        film_id,
        unnest(starship_array) as starship_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        film_id,
        starship_part
    from unnested
    where starship_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        film_id,
        trim(starship_part, '/') as starship_number
    from filtered
)
select
    film_id,
    CAST(starship_number AS INTEGER) as starship_id
from cleaned
