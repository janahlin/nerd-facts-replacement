{{ config(materialized='table') }}

WITH character_details AS (
    SELECT 
        c.character_id,
        c.name,
        c.gender,
        c.skin_color,
        c.hair_color,
        c.eye_color,
        c.height_cm,
        c.weight_kg,
        p.name AS homeworld,
        COUNT(DISTINCT fc.film_id) AS films_appeared_in,
        COUNT(DISTINCT sp.species_id) AS species_count,
        COUNT(DISTINCT sv.starship_id) AS starships_piloted,
        COUNT(DISTINCT vv.vehicle_id) AS vehicles_piloted
    FROM {{ ref('dim_people') }} c
    LEFT JOIN {{ ref('dim_planets') }} p ON c.homeworld_id = p.planet_id
    LEFT JOIN {{ ref('fact_film_characters') }} fc ON c.character_id = fc.character_id
    LEFT JOIN {{ ref('species_people') }} sp ON c.character_id = sp.character_id
    LEFT JOIN {{ ref('starships_people') }} sv ON c.character_id = sv.character_id
    LEFT JOIN {{ ref('vehicles_people') }} vv ON c.character_id = vv.character_id
    GROUP BY c.character_id, c.name, c.gender, c.skin_color, c.hair_color, c.eye_color, 
             c.height_cm, c.weight_kg, p.name
)

SELECT * FROM character_details
