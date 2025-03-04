{{ config(materialized='table') }}

WITH starship_details AS (
    SELECT 
        s.starship_id,
        s.name,
        s.model,
        s.manufacturer,
        s.length_m,
        s.crew_size,
        s.passenger_capacity,
        s.starship_class,
        s.hyperdrive_rating,
        COUNT(DISTINCT fs.film_id) AS films_featured_in,
        COUNT(DISTINCT sp.character_id) AS pilots
    FROM {{ ref('dim_starships') }} s
    LEFT JOIN {{ ref('fact_film_starships') }} fs ON s.starship_id = fs.starship_id
    LEFT JOIN {{ ref('starships_people') }} sp ON s.starship_id = sp.starship_id
    GROUP BY s.starship_id, s.name, s.model, s.manufacturer, s.length_m, s.crew_size, 
             s.passenger_capacity, s.starship_class, s.hyperdrive_rating
)

SELECT * FROM starship_details
