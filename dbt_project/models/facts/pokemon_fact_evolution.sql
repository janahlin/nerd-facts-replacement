{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(evolution_chain_id AS INTEGER) AS evolution_chain_id,
        CAST(pokemon_id AS INTEGER) AS pokemon_id,
        evolves_from_species_id,
        evolves_to_species_id,
        evolution_trigger,
        min_level,
        min_happiness,
        min_beauty,
        held_item
    FROM {{ source('raw', 'pokemon_evolution_chain') }}
)

SELECT * FROM source
