# Shopify Analytics

This project pulls ShopifyQL analytics data, shapes it into pandas DataFrames, and writes results to either BigQuery or CSV files.

## What it does
- Connects to Shopify and runs ShopifyQL queries for sales, inventory, and channel performance.
- Builds consolidated datasets for reporting.
- Writes outputs to BigQuery tables or CSV files based on a CLI flag.

## Requirements
- Python environment with dependencies used in the codebase (shopify, pandas, google-cloud-bigquery, dotenv, requests).
- A Shopify access token and valid environment configuration.
- BigQuery credentials (if writing to BigQuery).

## Configuration
The code expects environment variables loaded from a `.env` file. If you keep it under `credentials/.env`, make sure the code loads it explicitly.

Required variables:
- `SHOP_URL`
- `API_KEY`
- `API_SECRET`
- `API_VERSION`
- `project_id`
- `dataset_id`

## Run
Write to BigQuery (default):
```bash
python fith_bigquery.py
```

Write to CSV:
```bash
python fith_bigquery.py --output csv --csv-dir output
```

## Logs
Logs are written to `logs/run.log` (overwritten each run) and also printed to console.
