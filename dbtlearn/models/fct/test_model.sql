with source_reviews as (
    select * from {{ref("src_reviews")}}
)

select * from source_reviews