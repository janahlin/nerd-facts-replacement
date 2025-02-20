WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_swapi_people') }}
)

SELECT 
    name,
    CAST(height AS INTEGER) AS height,
    CAST(mass AS INTEGER) AS mass,
    hair_color,
    CASE 
        WHEN height > 170 THEN 'Tall'
        WHEN height BETWEEN 150 AND 170 THEN 'Medium'
        ELSE 'Short'
    END AS height_category
FROM source
