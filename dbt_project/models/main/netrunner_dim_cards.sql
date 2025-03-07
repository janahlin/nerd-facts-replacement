WITH raw_cards AS (
    SELECT * FROM {{ source('raw', 'netrunner_cards') }}
)

SELECT
    code AS card_id,                          
    title AS card_name,
    stripped_title,
    text,
    stripped_text,    
    type_code AS card_type,
    faction_code AS faction,
    faction_cost,
    pack_code,
    side_code,
    illustrator,
    influence_limit,
    keywords,
    deck_limit,
    minimum_deck_size,
    position,
    quantity,
    base_link,
    cost,
    memory_cost,
    strength,
    advancement_cost,
    agenda_points,
    trash_cost,
    flavor,    
    text AS card_text,
    uniqueness AS is_unique
FROM raw_cards
