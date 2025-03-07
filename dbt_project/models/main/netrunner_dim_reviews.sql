WITH raw_reviews AS (
    SELECT * FROM {{ source('raw', 'netrunner_reviews') }}
)

SELECT
    id AS review_id,
    title,
    user,
    ruling,
    votes,
    comments,
    date_create,
    date_update
FROM raw_reviews
