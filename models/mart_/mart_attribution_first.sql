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
            ORDER BY event_ts ASC
        ) AS first_click_rank
    FROM {{ ref('int_sessions') }}
    WHERE event_name IN ('page_view', 'purchase')
)

SELECT
    user_key,
    event_name,
    traffic_source AS first_click_source,
    traffic_medium AS first_click_medium,
    campaign AS first_click_campaign,
    event_ts AS first_click_ts
FROM attribution
WHERE first_click_rank = 1
