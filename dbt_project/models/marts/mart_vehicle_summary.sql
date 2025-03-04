{{ config(materialized='table') }}

WITH vehicle_details AS (
    SELECT 
        v.vehicle_id,
        v.name,
        v.model,
        v.manufacturer,
        v.length_m,
        v.crew_size,
        v.passenger_capacity,
        v.vehicle_class,
        COUNT(DISTINCT fv.film_id) AS films_featured_in,
        COUNT(DISTINCT vp.character_id) AS pilots
    FROM {{ ref('dim_vehicles') }} v
    LEFT JOIN {{ ref('fact_film_vehicles') }} fv ON v.vehicle_id = fv.vehicle_id
    LEFT JOIN {{ ref('vehicles_people') }} vp ON v.vehicle_id = vp.vehicle_id
    GROUP BY v.vehicle_id, v.name, v.model, v.manufacturer, v.length_m, v.crew_size, 
             v.passenger_capacity, v.vehicle_class
)

SELECT * FROM vehicle_details
