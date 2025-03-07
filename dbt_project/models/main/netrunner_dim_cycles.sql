WITH raw_cycles AS (
    SELECT * FROM {{ source('raw', 'netrunner_cycles') }}
)

SELECT
    code AS cycle_code,
    name AS cycle_name,
    position AS cycle_position,
    size AS cycle_size,
    rotated AS is_rotated
FROM raw_cycles
