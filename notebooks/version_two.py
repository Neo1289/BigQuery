import pandas as pd
import datetime
from datetime import timedelta
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import os
import datetime
from datetime import timedelta 
import schedule
import time
import logging
from google.oauth2 import service_account

def logging_info(func):
    def wrapper(*args, **kwargs):
        try:
            print("executing:", func.__name__)
            return func(*args, **kwargs)
        except Exception as e:
            print(func.__name__, "caused an error during execution:", e)
    return wrapper 

credentials = service_account.Credentials.from_service_account_file("connection-123-892e002c2def.json")
destination_table = "bitcoin.prices"
logging.basicConfig(
    filename='logfile.txt',  
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@logging_info
def fetch_transactions() -> pd.DataFrame:
    """
    Fetch transaction data from BigQuery.
    """
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

    logger.info(f"Bytes processed: {query_job.total_bytes_processed}")
    print(f"Bytes processed: {query_job.total_bytes_processed}")
    return df_transactions_count

@logging_info
def eth_transactions() -> pd.DataFrame:
    """
    Fetch transaction eth data from BigQuery
    """
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    query = """
    SELECT 
      DATE(block_timestamp) AS eth_date,
      COUNT(transaction_index) AS today_transaction
    FROM bigquery-public-data.crypto_ethereum.transactions
    WHERE DATE(block_timestamp) = CURRENT_DATE()
    GROUP BY 1
    """
    query_job = client.query(query)
    results = query_job.result()
    df_transactions_count = results.to_dataframe()
    df_transactions_count['eth_date'] = df_transactions_count['eth_date'].astype(str)

    logger.info(f"Bytes processed: {query_job.total_bytes_processed}")
    print(f"Bytes processed: {query_job.total_bytes_processed}")
    return df_transactions_count

@logging_info
def joining_tables() -> pd.DataFrame:
    """
    Join transaction data and Bitcoin price data into a single DataFrame.
    """
    df_transactions = fetch_transactions()
    eth_data = eth_transactions()

    joined_df = pd.merge(df_transactions, eth_data, how='left', left_on='date_', right_on='eth_date')
    return joined_df

@logging_info
def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date_', 'type': 'STRING', 'description': 'The date of the transaction'},
        {'name': 'total_transactions', 'type': 'INT64', 'description': 'Total number of transactions'},
        {'name': 'today_transaction', 'type': 'INT64', 'description': 'today eth transactions'}
    ]
    return table_schema

@logging_info
def run_etl() -> None:
    table = joining_tables()
    table_schema = schema()

    pandas_gbq.to_gbq(
        dataframe=table,
        destination_table=destination_table,
        project_id="connection-123",
        table_schema=table_schema,
        credentials=credentials,
        if_exists="replace" 
    )

    logger.info(f"uploaded {len(table)} row(s) into bigquery")

if __name__ == '__main__':
    main()
















