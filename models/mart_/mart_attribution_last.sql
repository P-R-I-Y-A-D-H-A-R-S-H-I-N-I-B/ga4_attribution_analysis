-- Overview:

-- This model computes LAST-CLICK attribution for conversions (purchases).
-- For each user who made a purchase, we identify the last touchpoint
-- (source/medium/campaign) immediately before conversion.

{{ config(materialized='table') }}

WITH purchasers AS (
    -- Step 1: Identify all users who made at least one purchase
    SELECT DISTINCT
        COALESCE(user_id, user_pseudo_id) AS user_key
    FROM {{ ref('int_sessions') }}
    WHERE event_name = 'purchase'
),

user_journey AS (
    -- Step 2: Get all touchpoints for those purchasers
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
        ) AS click_rank
    FROM {{ ref('int_sessions') }}
    WHERE event_name IN ('page_view', 'purchase')
      AND COALESCE(user_id, user_pseudo_id) IN (SELECT user_key FROM purchasers)
)

-- Step 3: Select the LAST touchpoint for each purchaser
SELECT
    user_key,
    event_name,
    traffic_source AS last_click_source,
    traffic_medium AS last_click_medium,
    campaign AS last_click_campaign,
    event_ts AS last_click_ts
FROM user_journey
WHERE click_rank = 1
