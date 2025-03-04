{{ config(materialized='table') }}

WITH planet_details AS (
    SELECT 
        p.planet_id,
        p.name,
        p.diameter_km,
        p.climate,
        p.terrain,
        p.population,
        p.gravity,
        COUNT(DISTINCT fp.film_id) AS films_featured_in,
        COUNT(DISTINCT sp.species_id) AS species_on_planet
    FROM {{ ref('dim_planets') }} p
    LEFT JOIN {{ ref('fact_film_planets') }} fp ON p.planet_id = fp.planet_id
    LEFT JOIN {{ ref('species_planets') }} sp ON p.planet_id = sp.planet_id
    GROUP BY p.planet_id, p.name, p.diameter_km, p.climate, p.terrain, p.population, p.gravity
)

SELECT * FROM planet_details
