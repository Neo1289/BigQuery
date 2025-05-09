import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import logging
from google.oauth2 import service_account

destination_table = "bitcoin.transactions"

def fetch_transactions(credentials) -> pd.DataFrame:
   
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
      SELECT 
        DATE(timestamp) AS date_,
        SUM(transaction_count) AS total_transactions
     FROM `bigquery-public-data.crypto_bitcoin.blocks`
     WHERE DATE(timestamp_month) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
     GROUP BY 1
    """
    query_job = client.query(query)
    results = query_job.result()
    df_transactions_count = results.to_dataframe()
    df_transactions_count['date_'] = df_transactions_count['date_'].astype(str)

    fetch_transactions.__doc__ = f"Bytes processed: {query_job.total_bytes_processed}"
    return df_transactions_count

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date_', 'type': 'STRING', 'description': 'The date of the transaction'},
        {'name': 'total_transactions', 'type': 'INT64', 'description': 'Total number of daily bitcoin transactions'}
    ]
    return table_schema

def run_etl(credentials) -> None:
    table = fetch_transactions(credentials)
    table_schema = schema()

    pandas_gbq.to_gbq(
        dataframe=table,
        destination_table=destination_table,
        project_id="connection-123",
        table_schema=table_schema,
        credentials=credentials,
        if_exists="replace" 
    )

    run_etl.__doc__ = f"fetching data from {fetch_transactions.__name__}, added {len(table)} rows."

if __name__ == '__main__':
    main()
