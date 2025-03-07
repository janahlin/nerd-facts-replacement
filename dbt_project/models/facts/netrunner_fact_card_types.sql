{{ config(materialized='table') }}

WITH card_type_links AS (
    SELECT 
        CAST(card_id AS VARCHAR) AS card_code,
        CAST(type_code AS VARCHAR) AS type_code
    FROM {{ ref('netrunner_junction_card_types') }}
)

SELECT 
    ct.card_code,
    c.card_name AS card_title,
    ct.type_code,
    t.name AS type_name
FROM card_type_links ct
LEFT JOIN {{ ref('netrunner_dim_cards') }} c ON ct.card_code = c.card_id
LEFT JOIN {{ ref('netrunner_dim_types') }} t ON ct.type_code = t.type_id
