WITH raw_packs AS (
    SELECT * FROM {{ source('raw', 'netrunner_packs') }}
)

SELECT
    code AS pack_code,
    name AS pack_name,
    cycle_code,
    date_release AS release_date,
    size AS pack_size
FROM raw_packs
