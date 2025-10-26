# GA4 Click Attribution Project Worklog

| Date       | Task Description                                                                                  |
|------------|--------------------------------------------------------------------------------------------------|
| 2025-10-24 | Project kickoff: reviewed GA4 public dataset, defined goal, and prepared architecture diagram.  |
| 2025-10-24 | Created Git repository, initialized DBT project folder structure (`stg_`, `int_`, `mart_`).     |
| 2025-10-25 | Developed staging model `stg_ga4_events.sql` to extract GA4 events from BigQuery sample dataset.|
| 2025-10-25 | Built intermediate model `int_sessions.sql` for sessionization and incremental processing.      |
| 2025-10-26 | Implemented attribution models: `mart_attribution_first.sql` and `mart_attribution_last.sql`. Added DBT tests. |
| 2025-10-26 | Created streaming demo script `stream_demo.py` to push sample events into BigQuery streaming table. |
| 2025-10-27 | Developed Streamlit dashboard: First vs Last click totals, 14-day time series, channel breakdown, live event panel. |
| 2025-10-27 | Polished README.md, requirements.txt, verified DBT tests, and tested end-to-end pipeline with streaming events. |
