import os
import sys

from google.cloud import bigquery

# Add src to path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.ingestion.core import config


def create_tables():
    client = bigquery.Client(project=config.PROJECT_ID)
    dataset_ref = client.dataset(config.BIGQUERY_DATASET)

    # 1. Options Chain History
    table_ref = dataset_ref.table(config.OPTIONS_CHAIN_HISTORY_TABLE)
    schema = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("contract_symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("option_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("expiration_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("strike", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("last_price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("bid", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ask", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("volume", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("open_interest", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("implied_volatility", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("delta", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("theta", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("vega", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("gamma", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("underlying_price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("fetch_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("dte", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField(
            "snapshot_date", "DATE", mode="REQUIRED"
        ),  # Partitioning column
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="snapshot_date",
    )
    table.clustering_fields = ["ticker", "contract_symbol"]

    try:
        client.create_table(table)
        print(f"Created table {table.full_table_id}")
    except Exception as e:
        print(f"Table {table.full_table_id} already exists or error: {e}")

    # 2. Technicals History
    table_ref_tech = dataset_ref.table(config.TECHNICALS_HISTORY_TABLE)
    schema_tech = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),  # Partitioning column
        bigquery.SchemaField("latest_rsi", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("latest_macd", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("latest_sma50", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("latest_sma200", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("close_30d_delta_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("rsi_30d_delta", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("macd_30d_delta", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("close_90d_delta_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("rsi_90d_delta", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("macd_90d_delta", "FLOAT", mode="NULLABLE"),
    ]
    table_tech = bigquery.Table(table_ref_tech, schema=schema_tech)
    table_tech.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    table_tech.clustering_fields = ["ticker"]

    try:
        client.create_table(table_tech)
        print(f"Created table {table_tech.full_table_id}")
    except Exception as e:
        print(f"Table {table_tech.full_table_id} already exists or error: {e}")


if __name__ == "__main__":
    create_tables()
