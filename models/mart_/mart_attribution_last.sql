-- overview:

-- This model identifies the last-click attribution for each user.  
-- It selects the latest event (like page_view or purchase) per user to capture the final traffic source and campaign before conversion.

{{ config(materialized='table') }}

WITH attribution AS (
    SELECT
        COALESCE(user_id, user_pseudo_id) AS user_key,
        event_name,
        traffic_source,
        traffic_medium,
        campaign,
        event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(user_id, user_pseudo_id)
            ORDER BY event_ts DESC
        ) AS last_click_rank
    FROM {{ ref('int_sessions') }}
    WHERE event_name IN ('page_view', 'purchase')
)

SELECT
    user_key,
    event_name,
    traffic_source AS last_click_source,
    traffic_medium AS last_click_medium,
    campaign AS last_click_campaign,
    event_ts AS last_click_ts
FROM attribution
WHERE last_click_rank = 1
