{{
    config(
        unique_key='album_id'
    )
}}
SELECT DISTINCT
     album_id
    ,album_name
    ,release_date
    ,TIMEZONE('UTC', NOW()) AS time_updated
FROM {{ ref('listening_history') }}
