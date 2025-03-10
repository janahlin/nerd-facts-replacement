{{ config(materialized='table') }}

WITH source AS (
    SELECT
        id AS move_id,
        name AS move_name,
        power,
        accuracy,
        pp,
        priority,
        "damage_class.name" as damage_class,
        type.name as type
    FROM {{ source('raw', 'pokemon_move') }}
)

SELECT * FROM source
