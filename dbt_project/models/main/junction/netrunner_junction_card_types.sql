{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(code AS VARCHAR) AS card_id,
        type_code AS type_code  -- Fix: Using the correct column name
    FROM {{ source('raw', 'netrunner_cards') }}
)

SELECT * FROM source
