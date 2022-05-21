SELECT
     track_id
    ,artist_id
    ,album_id
    ,artist_name
    ,album_name
    ,track_name
    ,(track_duration_ms::int)/1000 AS track_duration_sec
    ,LEFT(release_date, 4)::int AS release_date
    ,DATE_TRUNC('second', played_at::timestamp) AS played_at
    ,EXTRACT(EPOCH FROM DATE_TRUNC('second', played_at::timestamp)) AS played_at_unix
    ,track_number::int AS track_number
FROM spothist.staging.listening_history