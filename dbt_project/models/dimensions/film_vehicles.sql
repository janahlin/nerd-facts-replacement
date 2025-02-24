{{ config(
    materialized = 'table',    
    alias = 'film_vehicles',
    on_schema_change = 'append'
) }}

with raw_films as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as film_id,
        vehicles
    from {{ source('nerd_facts_raw', 'films') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/vehicles/" som delimiter.
    select
        film_id,
        regexp_split_to_array(vehicles, 'https://swapi.dev/api/vehicles/') as vehicle_array
    from raw_films
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        film_id,
        unnest(vehicle_array) as vehicle_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        film_id,
        vehicle_part
    from unnested
    where vehicle_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        film_id,
        trim(vehicle_part, '/') as vehicle_number
    from filtered
)
select
    film_id,
    CAST(vehicle_number AS INTEGER) as vehicle_id
from cleaned
