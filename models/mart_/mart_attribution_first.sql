-- Overview:

-- This model computes FIRST-CLICK attribution for conversions (purchases).
-- For each user who made a purchase, we identify their first touchpoint
-- (source/medium/campaign) from their historical session data.

{{ config(materialized='table') }}


WITH purchasers AS (
    -- Step 1: Get all unique users who completed a purchase
    SELECT DISTINCT
        COALESCE(user_id, user_pseudo_id) AS user_key
    FROM {{ ref('int_sessions') }}
    WHERE event_name = 'purchase'
),

user_journey AS (
    -- Step 2: Get all touchpoints for these purchasers (e.g. page views, purchases)
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
        ) AS click_rank
    FROM {{ ref('int_sessions') }}
    WHERE event_name IN ('page_view', 'purchase')
      AND COALESCE(user_id, user_pseudo_id) IN (SELECT user_key FROM purchasers)
)

-- Step 3: Select the FIRST touchpoint for each purchaser
SELECT
    user_key,
    event_name,
    traffic_source AS first_click_source,
    traffic_medium AS first_click_medium,
    campaign AS first_click_campaign,
    event_ts AS first_click_ts
FROM user_journey
WHERE click_rank = 1
