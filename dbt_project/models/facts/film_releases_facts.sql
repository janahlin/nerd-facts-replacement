{{ config(
    materialized = 'table',
    alias = 'film_releases_facts'
) }}

SELECT 
    f.film_id,
    f.film_title,
    f.release_date,
    COUNT(fc.character_id) AS num_characters,  -- Antal karaktärer i filmen
    COUNT(fp.planet_id) AS num_planets,  -- Antal planeter i filmen
    COUNT(fs.species_id) AS num_species,  -- Antal arter i filmen
    COUNT(fst.starship_id) AS num_starships,  -- Antal rymdskepp i filmen
    COUNT(fv.vehicle_id) AS num_vehicles  -- Antal fordon i filmen
FROM {{ ref('films') }} f
LEFT JOIN {{ ref('film_characters') }} fc ON f.film_id = fc.film_id
LEFT JOIN {{ ref('film_planets') }} fp ON f.film_id = fp.film_id
LEFT JOIN {{ ref('film_species') }} fs ON f.film_id = fs.film_id
LEFT JOIN {{ ref('film_starships') }} fst ON f.film_id = fst.film_id
LEFT JOIN {{ ref('film_vehicles') }} fv ON f.film_id = fv.film_id
GROUP BY f.film_id, f.film_title, f.release_date
