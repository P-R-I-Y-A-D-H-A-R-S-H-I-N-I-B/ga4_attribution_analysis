SELECT *
FROM {{ ref('stream_events') }}
WHERE event_id IS NULL
