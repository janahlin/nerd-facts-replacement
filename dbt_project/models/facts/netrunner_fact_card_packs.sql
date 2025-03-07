{{ config(materialized='table') }}

WITH card_pack_links AS (
    SELECT 
        CAST(card_code AS VARCHAR) AS card_code,
        CAST(pack_code AS VARCHAR) AS pack_code
    FROM {{ ref('netrunner_junction_card_packs') }}
)

SELECT 
    cp.card_code,
    c.card_name AS card_title,
    cp.pack_code,
    p.pack_name,
    p.release_date
FROM card_pack_links cp
LEFT JOIN {{ ref('netrunner_dim_cards') }} c ON cp.card_code = c.card_id
LEFT JOIN {{ ref('netrunner_dim_packs') }} p ON cp.pack_code = p.pack_code
