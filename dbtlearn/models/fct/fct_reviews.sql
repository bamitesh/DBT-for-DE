{{ config(
    materialized = 'incremental',
    on_schema_change = 'fail'
) }}

WITH src_reviewes AS (

    SELECT
        *
    FROM
        {{ ref("src_reviews") }}
)
SELECT
    listing_id,
    review_date,
    reviewer_name,
    review_text,
    review_sentiment
FROM
    src_reviewes
WHERE
    review_text IS NOT NULL

{% if is_incremental() %}
AND review_date >= COALESCE(
    (
        SELECT
            MAX(review_date)
        FROM
            {{ this }}
    ),
    '1900-01-01'
)
{% endif %}

