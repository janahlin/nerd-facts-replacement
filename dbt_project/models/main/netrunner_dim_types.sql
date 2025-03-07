WITH raw_types AS (
    SELECT * FROM {{ source('raw', 'netrunner_types') }}
)

SELECT
    code AS type_id,
    name,
    position,
    is_subtype,
    side_code    
FROM raw_types
