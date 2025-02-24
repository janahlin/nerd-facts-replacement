{{ config(
    materialized = 'table',    
    alias = 'people_vehicles',
    on_schema_change = 'append'
) }}

with raw_people as (
    select
        CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) as person_id,
        vehicles
    from {{ source('nerd_facts_raw', 'people') }}
),
split as (
    -- Dela upp characters-strängen med hjälp av regexp_split_to_array.
    -- Detta skapar en array baserat på "https://swapi.dev/api/vehicles/" som delimiter.
    select
        person_id,
        regexp_split_to_array(vehicles, 'https://swapi.dev/api/vehicles/') as vehicle_array
    from raw_people
),
unnested as (
    -- Konvertera arrayen till individuella rader
    select
        person_id,
        unnest(vehicle_array) as vehicle_part
    from split
),
filtered as (
    -- Filtrera bort tomma strängar (första elementet blir ofta tomt)
    select
        person_id,
        vehicle_part
    from unnested
    where vehicle_part <> ''
),
cleaned as (
    -- Trimma bort eventuella snedstreck så att endast siffrorna blir kvar
    select
        person_id,
        trim(vehicle_part, '/') as vehicle_number
    from filtered
)
select
    person_id,
    CAST(vehicle_number AS INTEGER) as vehicle_id
from cleaned
