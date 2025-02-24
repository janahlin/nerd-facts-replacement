{{
  config(
    materialized = 'table'
  )
}}

WITH source AS (
  SELECT 
    url, 
    title,
    episode_id,
    opening_crawl,
    director,
    producer,
    release_date,
    created,
    edited
  FROM {{ source('raw', 'films') }}
)

SELECT
  CAST(REGEXP_MATCHES(url, '/([0-9]+)/?$')[1] AS INTEGER) AS film_id,
  title AS film_title,
  episode_id,
  opening_crawl,
  director,
  producer,
  CAST(release_date AS DATE) AS release_date,
  CAST(created AS TIMESTAMP) AS created_at,
  CAST(edited AS TIMESTAMP) AS updated_at
FROM source