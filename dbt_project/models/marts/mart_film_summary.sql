{{ config(materialized='table') }}

WITH film_details AS (
    SELECT 
        f.film_id,
        f.title,
        f.episode_id,
        f.director,
        f.producer,
        f.release_date,
        COUNT(DISTINCT fc.character_id) AS num_characters,
        COUNT(DISTINCT fp.planet_id) AS num_planets,
        COUNT(DISTINCT fs.species_id) AS num_species,
        COUNT(DISTINCT fss.starship_id) AS num_starships,
        COUNT(DISTINCT fv.vehicle_id) AS num_vehicles
    FROM {{ ref('dim_films') }} f
    LEFT JOIN {{ ref('fact_film_characters') }} fc ON f.film_id = fc.film_id
    LEFT JOIN {{ ref('fact_film_planets') }} fp ON f.film_id = fp.film_id
    LEFT JOIN {{ ref('fact_film_species') }} fs ON f.film_id = fs.film_id
    LEFT JOIN {{ ref('fact_film_starships') }} fss ON f.film_id = fss.film_id
    LEFT JOIN {{ ref('fact_film_vehicles') }} fv ON f.film_id = fv.film_id
    GROUP BY f.film_id, f.title, f.episode_id, f.director, f.producer, f.release_date
)

SELECT * FROM film_details
