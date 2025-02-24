{{
  config(
    materialized = 'table'
  )
}}

WITH film_characters AS (
  SELECT
     CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) AS film_id,
    UNNEST(STRING_SPLIT(characters, ',')) AS character_url
  FROM {{ source('raw', 'films') }}
),

film_planets AS (
  SELECT
     CAST(REGEXP_EXTRACT(url, '/([0-9]+)/?$', 1) AS INTEGER) AS film_id,
    UNNEST(STRING_SPLIT(planets, ',')) AS planet_url
  FROM {{ source('raw', 'films') }}
)

SELECT
  fc.film_id,
   CAST(REGEXP_EXTRACT(fc.character_url, '/([0-9]+)/?$', 1) AS INTEGER) AS person_id,
   CAST(REGEXP_EXTRACT(fp.planet_url, '/([0-9]+)/?$', 1) AS INTEGER) AS planet_id  
FROM film_characters fc
JOIN film_planets fp ON fc.film_id = fp.film_id