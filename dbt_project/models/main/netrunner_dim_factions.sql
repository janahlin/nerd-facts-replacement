WITH raw_factions AS (
    SELECT * FROM {{ source('raw', 'netrunner_factions') }}
)

SELECT
    code AS faction_code,
    name AS faction_name,
    color AS faction_color,
    is_mini,
    is_neutral,
    side_code
FROM raw_factions
