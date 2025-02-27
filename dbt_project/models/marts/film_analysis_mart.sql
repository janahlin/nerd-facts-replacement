{{ config(
    materialized = 'table',
    alias = 'film_analysis_mart'
) }}

SELECT 
    f.film_id,
    f.film_title,
    f.episode_id,
    f.director,
    f.producer,
    f.release_date,
    fc_facts.num_characters
FROM {{ ref('films') }} f
LEFT JOIN {{ ref('film_characters_facts') }} fc_facts ON f.film_id = fc_facts.film_id
