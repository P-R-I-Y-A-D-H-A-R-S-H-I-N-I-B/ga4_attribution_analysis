"""Streamlit dashboard for GA4 Click Attribution.

Displays first/last attribution totals, a time series, top channels, and
recent streamed events. Queries BigQuery for materialized mart tables and
a small streaming staging table.
"""

import time
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from streamlit_autorefresh import st_autorefresh

# Configuration
PROJECT_ID = "ga4-attribution-analytics"
DATASET = "ga4_attribution"
MART_FIRST = f"{PROJECT_ID}.{DATASET}.mart_attribution_first"
MART_LAST = f"{PROJECT_ID}.{DATASET}.mart_attribution_last"
STREAM_TABLE = f"{PROJECT_ID}.{DATASET}.stream_events"
MAX_LIVE_ROWS = 50
DEFAULT_REFRESH_SECONDS = 5


@st.cache_resource
def get_bq_client():
    """Return a cached BigQuery client for the app session."""
    return bigquery.Client(project=PROJECT_ID)


client = get_bq_client()


@st.cache_data(ttl=10)
def fetch_first_last_totals(days=14):
    """Fetch daily totals for first- and last-click attribution.

    The query uses the materialized mart tables and returns a continuous
    date range with zero-filled days for missing data.
    """
    # start_date = (datetime.utcnow().date() - timedelta(days=days - 1)).isoformat()
    # default demo start date (can be changed if needed)
    start_date = '2021-01-27'

    q = f"""
      WITH first_attr AS (
        SELECT DATE(first_click_ts) AS day, COUNT(*) AS first_count
        FROM `{MART_FIRST}`
        WHERE DATE(first_click_ts) >= '{start_date}'
        GROUP BY day
      ),
      last_attr AS (
        SELECT DATE(last_click_ts) AS day, COUNT(*) AS last_count
        FROM `{MART_LAST}`
        WHERE DATE(last_click_ts) >= '{start_date}'
        GROUP BY day
      )
      SELECT
        COALESCE(f.day, l.day) AS day,
        COALESCE(f.first_count, 0) AS first_count,
        COALESCE(l.last_count, 0) AS last_count
      FROM first_attr f
      FULL OUTER JOIN last_attr l USING(day)
      ORDER BY day ASC
    """

    df = client.query(q).to_dataframe()
    df['day'] = pd.to_datetime(df['day'].astype(str))

    # Ensure continuous date range and convert to plain date for display
    days_idx = pd.date_range(start=start_date, periods=days)
    df = (
        df.set_index('day')
          .reindex(days_idx, fill_value=0)
          .rename_axis('day')
          .reset_index()
    )
    df['day'] = df['day'].dt.date
    return df[['day', 'first_count', 'last_count']]


@st.cache_data(ttl=10)
def fetch_channel_breakdown(days=14, top_n=20):
    """Return top channels by first and last attribution in window."""
    # start_date = (datetime.utcnow().date() - timedelta(days=days - 1)).isoformat()
    start_date = '2021-01-27'
    q = f"""
    WITH first_top AS (
      SELECT 
        COALESCE(first_click_source, '(unknown)') AS source,
        COALESCE(first_click_medium, '') AS medium,
        COUNT(*) AS first_count
      FROM `{MART_FIRST}`
      WHERE DATE(first_click_ts) >= '{start_date}'
      GROUP BY source, medium
    ),
    last_top AS (
      SELECT 
        COALESCE(last_click_source, '(unknown)') AS source,
        COALESCE(last_click_medium, '') AS medium,
        COUNT(*) AS last_count
      FROM `{MART_LAST}`
      WHERE DATE(last_click_ts) >= '{start_date}'
      GROUP BY source, medium
    )
    SELECT
      COALESCE(f.source, l.source) AS source,
      COALESCE(f.medium, l.medium) AS medium,
      COALESCE(f.first_count, 0) AS first_count,
      COALESCE(l.last_count, 0) AS last_count
    FROM first_top f
    FULL OUTER JOIN last_top l USING(source, medium)
    ORDER BY (COALESCE(f.first_count, 0) + COALESCE(l.last_count, 0)) DESC
    LIMIT {top_n}
    """

    return client.query(q).to_dataframe()


@st.cache_data(ttl=5)
def fetch_live_events(limit=MAX_LIVE_ROWS):
    """Fetch recent streamed events from the staging table for live panel."""
    q = f"""
    SELECT
      TIMESTAMP_MICROS(event_timestamp) AS event_ts,
      user_pseudo_id,
      user_id,
      event_name,
      traffic_source,
      traffic_medium,
      campaign,
      event_id
    FROM `{STREAM_TABLE}`
    ORDER BY event_ts DESC
    LIMIT {limit}
    """
    df = client.query(q).to_dataframe()
    if not df.empty:
        df['event_ts'] = pd.to_datetime(df['event_ts'])
    return df


# -----------------
# UI
# -----------------
st.set_page_config(layout="wide", page_title="GA4 Attribution — RealTime")
st.title("GA4 Real-Time Attribution Dashboard")

# Controls
col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([1, 1, 2])
with col_ctrl1:
    days = st.selectbox("Window (days)", options=[7, 14, 30], index=1, help="Lookback window for aggregates")
with col_ctrl2:
    refresh_interval = st.number_input("Auto-refresh interval (s)", min_value=1, max_value=60, value=DEFAULT_REFRESH_SECONDS, step=1)
with col_ctrl3:
    auto_refresh = st.checkbox("Auto-refresh", value=False)
    if st.button("Manual refresh"):
        # Clear caches and rerun to fetch fresh data
        fetch_first_last_totals.clear()
        fetch_channel_breakdown.clear()
        fetch_live_events.clear()
        st.experimental_rerun()


# Layout panels
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("First vs Last — Time Series")
    df_totals = fetch_first_last_totals(days=days)
    total_first = int(df_totals['first_count'].sum())
    total_last = int(df_totals['last_count'].sum())

    totals_col1, totals_col2, _ = st.columns([1, 1, 2])
    totals_col1.metric("First-Click conversions", f"{total_first}")
    totals_col2.metric("Last-Click conversions", f"{total_last}")

    st.line_chart(df_totals.set_index('day')[['first_count', 'last_count']], height=320)

    st.markdown("### Channel breakdown (top channels)")
    df_chan = fetch_channel_breakdown(days=days, top_n=25)
    if df_chan.empty:
        st.write("No channel data available.")
    else:
        st.dataframe(df_chan.rename(columns={
            'source': 'Source', 'medium': 'Medium', 'first_count': 'First', 'last_count': 'Last'
        }))

with right_col:
    st.subheader("Live Streamed Events")
    st.markdown("Shows the most recent streamed events (manual or auto-refresh).")
    live_limit = st.number_input("Rows to show", min_value=5, max_value=200, value=MAX_LIVE_ROWS, step=5)
    df_live = fetch_live_events(limit=live_limit)
    if df_live.empty:
        st.write("No live events found.")
    else:
        st.dataframe(df_live)

    st.markdown("**Stream stats**")
    st.write(f"Last event: {df_live['event_ts'].max() if not df_live.empty else 'N/A'}")
    st.write(f"Unique users in panel: {df_live['user_pseudo_id'].nunique() if not df_live.empty else 0}")


# Auto-refresh handling – clear caches then trigger refresh
if auto_refresh:
    fetch_first_last_totals.clear()
    fetch_channel_breakdown.clear()
    fetch_live_events.clear()
    time.sleep(refresh_interval)
    st_autorefresh(interval=refresh_interval * 1000, key="data_refresh")

st.markdown("---")
st.caption("Dashboard queries BigQuery; use materialized marts to lower cost and improve render times.")