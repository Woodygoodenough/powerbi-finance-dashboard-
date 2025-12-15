# Finance Market Data ETL Pipeline

A Python-based ETL pipeline that fetches public market data from Alpha Vantage, performs feature engineering and aggregation, and publishes **analytics-ready CSV datasets**. The pipeline is designed to serve **clean, versioned financial data assets** that can be consumed by downstream tools such as Business Intelligence platforms, dashboards, or analytical applications.

## Purpose
This project focuses strictly on the **data engineering layer**:
- Extract: pull equity, FX, and crypto time-series data
- Transform: normalize schemas, enrich with derived features, and generate analytical tables
- Load: publish a **latest public snapshot** as static data assets

Visualization and application layers (e.g., Power BI, Streamlit, custom dashboards) are treated as **downstream consumers** and are intentionally decoupled from this repository.

## Prerequisites
- Python 3.11+
- Alpha Vantage API key (`ALPHAVANTAGE_API_KEY`)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
````

## Configuration

* Set `ALPHAVANTAGE_API_KEY` in your shell or `.env`
* Edit `config/tickers.yaml` to control:

  * asset classes (Equity / FX / Crypto)
  * symbols and currency pairs
  * logical groupings and display names

## Run locally

```bash
python -m src.pipeline
```

### Outputs

* **Versioned runs (local / archival)**: `data/<run_id>/`
* **Latest snapshot (deploy-ready)**: `docs/data/`

The `docs/data/` directory is overwritten on each run and always represents the most recent successful ETL output.

## Published datasets (CSV)

The pipeline produces normalized, analytics-ready tables:

* `dim_date.csv`
* `dim_ticker.csv`
* `fact_prices.csv`
* `fact_features_daily.csv`
* `fact_latest_snapshot.csv`
* `etl_metadata.csv`

Refer to `data_dictionary.md` for column definitions, keys, and table grain.

## Public data access (GitHub Pages)

The latest snapshot is published as static assets via GitHub Pages and can be accessed programmatically:

```
https://<user>.github.io/<repo>/data/<file>.csv
```

Example:

```
https://<user>.github.io/<repo>/data/fact_latest_snapshot.csv
```

These endpoints are intended for downstream ETL, analytics, and visualization workflows.

## CI/CD

A scheduled GitHub Actions workflow:

1. fetches fresh market data
2. runs the ETL pipeline
3. generates `docs/data/*.csv`
4. deploys the latest snapshot to GitHub Pages

No datasets are committed back to the repository. You can run locally to get the data directory.

## Testing

Core logic is organized into small, testable functions (data ingestion, feature computation, snapshot generation) under `src/pipeline/`, enabling unit-level validation of each ETL stage.