{{
    config(
        unique_key='track_id'
    )
}}
SELECT DISTINCT
     track_id
    ,track_name
    ,track_duration_sec
    ,NULL AS track_duration_str
    ,track_number
    ,TIMEZONE('UTC', NOW()) AS time_updated
FROM {{ ref('listening_history') }}