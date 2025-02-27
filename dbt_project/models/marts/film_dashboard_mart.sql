{{ config(
    materialized = 'table',
    alias = 'film_dashboard_mart'
) }}

SELECT 
    f.film_id,
    f.film_title,
    f.episode_id,
    f.director,
    f.producer,
    f.release_date,
    
    -- Fakta: Antal entiteter kopplade till filmen
    fc_facts.num_characters,
    fp_facts.num_planets,
    fs_facts.num_species,
    fst_facts.num_starships,
    fv_facts.num_vehicles

FROM {{ ref('films') }} f

-- Fakta-tabeller
LEFT JOIN (
    SELECT film_id, COUNT(person_id) AS num_characters 
    FROM {{ ref('film_characters') }} 
    GROUP BY film_id
) fc_facts ON f.film_id = fc_facts.film_id

LEFT JOIN (
    SELECT film_id, COUNT(planet_id) AS num_planets 
    FROM {{ ref('film_planets') }} 
    GROUP BY film_id
) fp_facts ON f.film_id = fp_facts.film_id

LEFT JOIN (
    SELECT film_id, COUNT(species_id) AS num_species 
    FROM {{ ref('film_species') }} 
    GROUP BY film_id
) fs_facts ON f.film_id = fs_facts.film_id

LEFT JOIN (
    SELECT film_id, COUNT(starship_id) AS num_starships 
    FROM {{ ref('film_starships') }} 
    GROUP BY film_id
) fst_facts ON f.film_id = fst_facts.film_id

LEFT JOIN (
    SELECT film_id, COUNT(vehicle_id) AS num_vehicles 
    FROM {{ ref('film_vehicles') }} 
    GROUP BY film_id
) fv_facts ON f.film_id = fv_facts.film_id
