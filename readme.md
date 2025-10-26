# GA4 Click Attribution Pipeline

## Project Overview

This project builds a **near-real-time attribution pipeline** on GA4 public e-commerce data using **BigQuery + dbt**, computes **First-Click and Last-Click attribution**, supports **streamed events**, and visualizes results on a **dashboard**.

**Dataset:** `bigquery-public-data.ga4_obfuscated_sample_ecommerce`  
**Tools:** BigQuery, dbt, Python, Streamlit / Looker Studio  

**Attribution Logic:**
- **First-Click:** earliest touch per user within a 30-day lookback window  
- **Last-Click:** latest touch per user within the same window  
- **Identity Resolution:** prefer `user_id`, fallback to `user_pseudo_id`  
- **Tie-breaker:** use earliest / latest timestamp; deterministic if tied  

---


Installation Instructions

Create a virtual environment (recommended):

    python -m venv venv
    source venv/bin/activate   # Linux / Mac
    venv\Scripts\activate      # Windows


Install Python packages:

    pip install -r requirements.txt


Install dbt BigQuery adapter globally or in the same environment:

    pip install dbt-bigquery==1.6.1


## Setup & Run

### 1. Configure dbt
- Edit `profiles.yml` with your **GCP project**, **dataset**, and **service account key**.
# GA4 Click Attribution Pipeline

A compact demonstration repo implementing a near-real-time GA4 attribution pipeline
using BigQuery and dbt. It computes First-Click and Last-Click attribution, supports
streamed events for demonstrations, and includes a small Streamlit dashboard.

Key technologies: BigQuery, dbt, Python, Streamlit

Dataset used for examples: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`

## Table of contents
- Project overview
- Repo structure
- Requirements
- Installation
- Configuration
- Run (dbt, demo, dashboard)
- Notes & troubleshooting


## Project overview

- Attribution methods: First-Click and Last-Click within a configurable lookback window
- Identity resolution: prefer `user_id`, otherwise `user_pseudo_id`
- Intended for code demo, hence it is intentionally compact and readable


## Repository structure

Top-level files and folders:

```
dbt_project.yml         # dbt configuration
profiles.yml            # local dbt profile (BigQuery credentials and project/dataset)
readme.md               # this file
stream_demo.py          # small script that writes synthetic events to the staging table
dashboard/              # Streamlit demo app
models/                 # dbt models: stg_, int_, mart_
macros/                 # dbt macros and helpers
tests/                  # dbt SQL tests used during development
docs/                   # supporting documentation (field definitions, notes)
target/                 # dbt compiled artifacts (ignore for review)
```

Model layout (dbt):
- `models/stg_/`: staging models (ingest / normalize GA4 events)
- `models/int_/`: intermediate models (sessionization, identity resolution)
- `models/mart_/`: mart models implementing First-Click and Last-Click attribution


## Requirements

- Python 3.10+ (tested with 3.11)
- `pip` and a virtual environment tool (venv)
- Google Cloud service account with BigQuery access if you run against your own project
- dbt-core and `dbt-bigquery` adapter


## Installation

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv venv
source venv/bin/activate   # macOS / Linux
pip install -r requirements.txt
pip install dbt-bigquery==1.6.1
```


## Configuration

1. Set Google Application Credentials to a service account JSON (if using your own GCP):

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

2. Update `profiles.yml` with your GCP project, dataset, and location. A minimal `profiles.yml` entry:

```yaml
your_profile_name:
    target: dev
    outputs:
        dev:
            type: bigquery
            method: service-account
            project: your-gcp-project-id
            dataset: your_dbt_dataset
            keyfile: /path/to/service-account.json
            location: US
```

Replace `your_profile_name`, `your-gcp-project-id`, and `your_dbt_dataset` accordingly.


## Run

Run dbt models in stages to materialize the pipeline:

```bash
# install dependencies declared in packages.yml (if present)
dbt deps

# run staging models
dbt run --models stg_*

# run intermediate models
dbt run --models int_*

# run mart models (attribution)
dbt run --models mart_*

# run tests (optional)
dbt test
```

Optional demo and dashboard:

- Stream synthetic events into the staging table (local demo):

```bash
python stream_demo.py
```

- Start the Streamlit dashboard (queries BigQuery directly):

```bash
streamlit run dashboard/streamlit_app.py
```


## Notes & troubleshooting

- The Streamlit app makes live queries to BigQuery; use materialized marts in production to
    reduce costs and latency.
- `target/` contains compiled dbt artifacts — ignore when reviewing code.
- If `stream_demo.py` raises errors, confirm your `GOOGLE_APPLICATION_CREDENTIALS` and `profiles.yml`.


## Author

Priyadharshini B — October 2025
