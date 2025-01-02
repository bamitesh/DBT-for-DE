WITH l AS (
    SELECT
        *
    FROM
        {{ ref('dim_listing_cleansed') }}
),
r AS (
    SELECT
        *
    FROM
        {{ ref('fct_reviews') }}
)
SELECT
    l.*
FROM
    l
    LEFT JOIN r
    ON l.listing_id = r.listing_id
WHERE
    r.review_date <= l.created_at
LIMIT
    10
