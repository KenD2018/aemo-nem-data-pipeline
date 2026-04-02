# AEMO NEM Electricity Data Pipeline

End-to-end data engineering pipeline for Australian electricity market data.

## Overview

Automated pipeline that ingests 5 years of electricity price and demand data from AEMO (Australian Energy Market Operator), covering all 5 states in the National Electricity Market (NEM).

**Key findings from the data:**
- 2022 energy crisis caused prices to triple across all states
- SA1 had negative electricity prices on 75% of days (1,368/1,825) due to renewable oversupply
- May is unexpectedly the most expensive month in NSW — supply shocks matter more than seasonal demand
- QLD1 is the most price-volatile state, not SA1 as commonly assumed

## Architecture
```
AEMO Website (CSV files)
    ↓ [Orchestration: Apache Airflow]
Google Cloud Storage (data lake)
    ↓ [Loading]
BigQuery raw table → BigQuery partitioned + clustered table
    ↓ [Transformation: dbt]
Staging view → Daily aggregation table
    ↓ [Visualisation]
Power BI Dashboard
```

## Dataset

| Property | Detail |
|----------|--------|
| Source | [AEMO NEM Aggregated Data](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data) |
| States | NSW1, VIC1, QLD1, SA1, TAS1 |
| Period | January 2020 – December 2024 |
| Files | 300 CSV files (5 years × 5 states × 12 months) |
| Rows | ~3 million (5-minute intervals) |
| Fields | settlement_date, region_id, total_demand_mw, price_aud_per_mwh, period_type |

## Technology Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow (Docker) | Pipeline orchestration |
| Google Cloud Storage | Data lake (raw CSV storage) |
| BigQuery | Data warehouse |
| dbt | Data transformation |
| Power BI | Dashboard and visualisation |

## Project Structure
```
aemo-nem-data-pipeline/
├── dags/
│   └── aemo_pipeline_dag.py     # Airflow DAG (4 tasks)
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   └── stg_nem_data.sql
│       └── core/
│           └── fact_daily_electricity.sql
└── README.md
```

## Pipeline Details

### Airflow DAG — 4 Tasks

1. `download_and_upload_to_gcs` — Downloads CSVs from AEMO, uploads to GCS
2. `load_gcs_to_bigquery` — Loads GCS files into BigQuery raw table
3. `create_partitioned_table` — Creates partitioned + clustered table
4. `verify_data` — Validates row counts

### BigQuery Tables

| Table | Type | Description |
|-------|------|-------------|
| `raw_nem_data` | Table | Raw data loaded from GCS |
| `nem_partitioned` | Table | Partitioned by date, clustered by region_id |
| `stg_nem_data` | View | dbt staging — adds year, month, hour fields |
| `fact_daily_electricity` | Table | dbt mart — daily aggregates per state |

**Partitioning:** `PARTITION BY DATE(settlement_date)` — reduces query cost by scanning only relevant dates

**Clustering:** `CLUSTER BY region_id` — further reduces scan when filtering by state

### dbt Models

**Staging (`stg_nem_data`):** Materialised as VIEW. Adds derived fields: `settlement_date_day`, `year`, `month`, `hour`. Filters nulls and zero-demand rows.

**Core (`fact_daily_electricity`):** Materialised as TABLE. Daily aggregates: avg/max/min price, avg/max demand per state. Result: 9,140 rows from ~3M raw rows.

## How to Reproduce

### Prerequisites
- Docker Desktop
- GCP account (BigQuery + GCS enabled)
- Service account key with BigQuery Admin + Storage Admin roles

### Steps

**1. Clone the repo**
```bash
git clone https://github.com/KenD2018/aemo-nem-data-pipeline.git
cd aemo-nem-data-pipeline
```

**2. Set up GCP credentials**
```bash
mkdir -p ~/.google/credentials
cp /path/to/your-service-account-key.json ~/.google/credentials/google_credentials.json
```

**3. Start Airflow**
```bash
# Use docker-compose.yaml from DE Zoomcamp week2
docker-compose up -d
```
Open http://localhost:8080 (user: airflow / pass: airflow)

Add connection: Admin → Connections → `google_cloud_default`
- Type: Google Cloud
- Keyfile Path: `/.google/credentials/google_credentials.json`

**4. Configure and run the DAG**

Copy `dags/aemo_pipeline_dag.py` to your Airflow dags folder. Update these values:
```python
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_BUCKET = "your-gcs-bucket-name"
BIGQUERY_DATASET = "aemo_electricity"
```

Trigger `aemo_nem_data_pipeline` in Airflow UI. Wait ~20 minutes for 300 files.

**5. Run dbt transformations**
```bash
pip install dbt-bigquery
cd dbt
dbt run
```

**6. Connect Power BI**

Power BI Desktop → Get Data → Google BigQuery → `aemo_electricity.fact_daily_electricity`

## Dashboard

Power BI dashboard with 2 visualisations:

1. **Price trend by state (2020–2024)** — 2022 crisis clearly visible
2. **Demand by state** — NSW1 highest, TAS1 lowest
