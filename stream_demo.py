"""Small demo that simulates GA4-like events and streams them to BigQuery.

This file is a demo helper for local development and testing. It
streams a small number of synthetic events to a staging table.
"""

import time
import random
import uuid
from datetime import datetime
from google.cloud import bigquery

# Configuration
PROJECT_ID = "ga4-attribution-analytics"
DATASET_ID = "ga4_attribution"
TABLE_ID = "stream_events"

NUM_EVENTS = 20
SLEEP_SECONDS = 1

# Sample values for synthetic events
TRAFFIC_SOURCES = ["google", "facebook", "instagram", "email", "direct"]
TRAFFIC_MEDIUMS = ["cpc", "social", "email", "none"]
CAMPAIGNS = ["spring_sale", "black_friday", "promo_10", "new_user"]
EVENT_NAMES = ["page_view", "purchase"]

# Initialize BigQuery client and table reference
client = bigquery.Client(project=PROJECT_ID)
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)


def generate_event():
    """Create one synthetic GA4-like event (dict matching BQ schema).

    The event id combines a UUID and timestamp to make inserts idempotent.
    """
    event_ts = datetime.utcnow()
    user_id = str(random.randint(1000, 1100))
    user_pseudo_id = str(uuid.uuid4())
    event_name = random.choice(EVENT_NAMES)
    event_value = random.randint(1, 100) if event_name == "purchase" else None

    event_id = f"{user_pseudo_id}_{int(event_ts.timestamp() * 1000)}"

    return {
        "event_date": event_ts.strftime("%Y%m%d"),
        "event_timestamp": int(event_ts.timestamp() * 1_000_000),  # microseconds
        "user_id": user_id,
        "user_pseudo_id": user_pseudo_id,
        "event_name": event_name,
        "traffic_source": random.choice(TRAFFIC_SOURCES),
        "traffic_medium": random.choice(TRAFFIC_MEDIUMS),
        "campaign": random.choice(CAMPAIGNS),
        "event_value": event_value,
        "event_id": event_id,
    }


def stream_event_to_bq(row):
    """Insert a single row into BigQuery; print status for demo visibility."""
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"[ERROR] Failed to insert row: {errors}")
    else:
        print(f"[INFO] Inserted event {row['event_id']} ({row['event_name']})")


def main():
    print(f"[INFO] Starting GA4 streaming demo: {NUM_EVENTS} events")
    for _ in range(NUM_EVENTS):
        event = generate_event()
        stream_event_to_bq(event)
        time.sleep(SLEEP_SECONDS)
    print("[INFO] Streaming demo completed!")


if __name__ == "__main__":
    main()