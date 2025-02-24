{{
  config(
    materialized = 'table'
  )
}}

SELECT
  f.film_id,
  f.film_title,
  f.episode_id,
  f.release_date,
  p.person_id,
  p.person_name,
  p.gender,
  p.species,
  pl.planet_id,
  pl.planet_name,
  pl.climate,
  pl.terrain,
  pl.population
FROM {{ ref('fact_film_appearances') }} fa
JOIN {{ ref('dim_films') }} f ON fa.film_id = f.film_id
JOIN {{ ref('dim_people') }} p ON fa.person_id = p.person_id
JOIN {{ ref('dim_planets') }} pl ON fa.planet_id = pl.planet_id