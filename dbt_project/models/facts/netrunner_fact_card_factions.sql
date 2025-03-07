{{ config(materialized='table') }}

WITH card_faction_links AS (
    SELECT 
        CAST(card_id AS VARCHAR) AS card_code,
        CAST(faction_code AS VARCHAR) AS faction_code
    FROM {{ ref('netrunner_junction_card_factions') }}
)

SELECT 
    cf.card_code,
    c.card_name AS card_title,
    cf.faction_code,
    f.faction_name,
    f.faction_color
FROM card_faction_links cf
LEFT JOIN {{ ref('netrunner_dim_cards') }} c ON cf.card_code = c.card_id
LEFT JOIN {{ ref('netrunner_dim_factions') }} f ON cf.faction_code = f.faction_code
