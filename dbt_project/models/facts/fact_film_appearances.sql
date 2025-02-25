{{ config(
    materialized = 'table',
    schema = 'main',
    on_schema_change = 'append'
) }}

with film_dim as (
    -- Use the transformed films dimension; note that it is aliased as 'films'
    select film_id, title, release_date, director
    from {{ ref('films') }}
),
people_dim as (
    -- Use the transformed people dimension; note that it is aliased as 'people'
    select person_id, person_name, gender, homeworld
    from {{ ref('people') }}
),
raw_appearances as (
    -- Assume you have a raw fact table or a junction table that captures film-person relationships.
    -- For example, this could be a junction table created by normalizing a JSON/CSV array.
    -- If you don't have one yet, you might generate it by unnesting arrays from your seeds.
    select
      film_id,
      person_id,
      -- Optionally, add measures such as "appearance_score" or "screen_time"
      1 as appearance_flag  -- Example measure: every row indicates a film appearance event.
    from {{ ref('film_characters') }}
)
select
    a.film_id,
    f.title,
    a.person_id,
    p.person_name,
    a.appearance_flag,
    f.release_date,
    f.director,
    p.gender
from raw_appearances a
left join film_dim f on a.film_id = f.film_id
left join people_dim p on a.person_id = p.person_id
