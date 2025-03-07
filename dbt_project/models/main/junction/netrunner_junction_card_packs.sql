{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(code AS VARCHAR) AS card_code,
        CAST(pack_code AS VARCHAR) AS pack_code
    FROM {{ source('raw', 'netrunner_cards') }}  -- ✅ Ensure source table exists
)

SELECT * FROM source
