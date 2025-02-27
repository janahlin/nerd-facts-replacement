{{ config(
    materialized = 'table',
    alias = 'film_characters_facts'
) }}

SELECT 
    fc.film_id,
    f.film_title,
    COUNT(fc.person_id) AS num_characters  -- Hur många karaktärer som förekommer i varje film
FROM {{ ref('film_characters') }} fc
LEFT JOIN {{ ref('films') }} f ON fc.film_id = f.film_id
GROUP BY fc.film_id, f.film_title
