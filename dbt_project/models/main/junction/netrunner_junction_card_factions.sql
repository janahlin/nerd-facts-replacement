{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(code AS VARCHAR) AS card_id,
        faction_code AS faction_code  -- Fix: Using correct column
    FROM {{ source('raw', 'netrunner_cards') }}
)

SELECT * FROM source
