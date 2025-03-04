{{ config(materialized='table') }}

WITH cleaned_data AS (
    SELECT
        CAST(uid AS INTEGER) AS species_id,
        
        -- Step 1: Strip out non-numeric homeworld values BEFORE casting
        CASE 
            WHEN TRIM(LOWER(homeworld)) IN ('null', '') OR homeworld IS NULL THEN NULL
            ELSE REGEXP_REPLACE(homeworld, '.*/planets/', '')
        END AS planet_id_raw
        
    FROM {{ source('raw', 'species') }}
),

final_cast AS (
    SELECT
        species_id,
        
        -- Step 2: Apply CAST only on fully validated numeric strings
        CASE 
            WHEN planet_id_raw ~ '^[0-9]+$' THEN CAST(planet_id_raw AS INTEGER)
            ELSE NULL
        END AS planet_id
        
    FROM cleaned_data
)

SELECT * FROM final_cast
