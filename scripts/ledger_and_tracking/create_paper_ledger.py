from google.cloud import bigquery

PROJECT_ID = "profitscout-fida8"
DATASET_ID = "profit_scout"
TABLE_ID = "forward_paper_ledger"

client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

schema = [
    # Identity
    bigquery.SchemaField("scan_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("recommended_contract", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("direction", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_premium_signal", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("premium_score", "INTEGER", mode="NULLABLE"),
    
    # Policy State
    bigquery.SchemaField("is_skipped", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("skip_reason", "STRING", mode="NULLABLE"),
    
    # Market / Regime Context
    bigquery.SchemaField("VIX_at_entry", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("SPY_trend_state", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("recommended_dte", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("recommended_volume", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("recommended_oi", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("recommended_spread_pct", "FLOAT", mode="REQUIRED"),
    
    # Execution
    bigquery.SchemaField("entry_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("entry_price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("target_price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("stop_price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("exit_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("exit_reason", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("realized_return_pct", "FLOAT", mode="NULLABLE"),
]

table = bigquery.Table(table_ref, schema=schema)

try:
    table = client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
except Exception as e:
    print(f"Error creating table: {e}")

