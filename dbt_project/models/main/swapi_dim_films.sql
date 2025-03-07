{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(uid AS INTEGER) AS film_id,
        title,
        CAST(episode_id AS INTEGER) AS episode_id,
        director,
        COALESCE(producer, 'Unknown') AS producer,
        CAST(NULLIF(release_date, '') AS DATE) AS release_date,
        opening_crawl
    FROM {{ source('raw', 'films') }}
)

SELECT * FROM source
