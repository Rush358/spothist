{{
    config(
        unique_key='artist_id'
    )
}}

SELECT
     artist_id
    ,artist_name
    ,genres
    ,TIMEZONE('UTC', NOW()) AS time_updated
FROM {{ ref('artist') }}
