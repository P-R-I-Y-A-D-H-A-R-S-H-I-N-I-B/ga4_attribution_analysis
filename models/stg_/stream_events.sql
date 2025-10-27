-- overview: 

-- This model extracts and stages raw GA4 event data from BigQuery sample tables.  
-- It incrementally loads new events based on the latest event_timestamp, ensuring no duplicates using event_id.

{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH raw_events AS (

    SELECT
        event_date,
        event_timestamp,
        user_id,
        user_pseudo_id,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source') AS traffic_source,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium') AS traffic_medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS campaign,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key='value') AS event_value,
        event_bundle_sequence_id,
        event_previous_timestamp,
        CONCAT(
        user_pseudo_id, '_',
        CAST(event_timestamp AS STRING), '_',
        event_name
        ) AS event_id


    FROM
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
)

SELECT *
FROM raw_events

{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
