{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(code AS VARCHAR) AS pack_code,  -- Ensure correct reference
        CAST(cycle_code AS VARCHAR) AS cycle_code
    FROM {{ source('raw', 'netrunner_packs') }}  -- ✅ Ensure correct source table
)

SELECT * FROM source
