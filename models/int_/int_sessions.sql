{{ config(materialized='incremental', unique_key='session_id') }}

WITH sessionized AS (
    SELECT
        user_id,
        user_pseudo_id,
        traffic_source,
        traffic_medium,
        campaign,
        event_name,
        -- Convert microseconds to actual TIMESTAMP
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        LEAD(event_timestamp) OVER (
            PARTITION BY COALESCE(user_id,user_pseudo_id) 
            ORDER BY event_timestamp
        ) AS next_event_raw,
        TIMESTAMP_DIFF(
            TIMESTAMP_MICROS(LEAD(event_timestamp) OVER (
                PARTITION BY COALESCE(user_id,user_pseudo_id) 
                ORDER BY event_timestamp
            )),
            TIMESTAMP_MICROS(event_timestamp),
            MINUTE
        ) AS diff_minutes,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(user_id,user_pseudo_id) 
            ORDER BY event_timestamp
        ) AS rn,
        CONCAT(
            COALESCE(user_id,user_pseudo_id),'_',CAST(event_timestamp AS STRING)
        ) AS session_id
    FROM {{ ref('stream_events') }}
)

SELECT *
FROM sessionized

{% if is_incremental() %}
WHERE event_ts > (SELECT MAX(event_ts) FROM {{ this }})
{% endif %}
