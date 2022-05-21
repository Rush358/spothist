{{
    config(
        unique_key='id'
    )
}}

SELECT
     track_id || played_at_unix::int AS id
    ,track_id AS fk_track_id
    ,artist_id AS fk_artist_id
    ,album_id AS fk_album_id
    ,played_at_unix
    ,played_at
    ,TIMEZONE('UTC', NOW()) AS time_updated
FROM {{ ref('listening_history') }}