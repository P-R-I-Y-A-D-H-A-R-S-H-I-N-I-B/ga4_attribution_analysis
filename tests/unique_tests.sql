SELECT event_id, COUNT(*) 
FROM {{ ref('stream_events') }}
GROUP BY event_id
HAVING COUNT(*) > 1
