import pandas as pd
import datetime
from datetime import timedelta
import requests
from google.oauth2 import service_account
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import os
import datetime
from datetime import timedelta 
import schedule
import time
import yfinance as yf
import logging

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

@logging_info
def fetch_bitcoin_price() -> pd.DataFrame:
    """
    Fetch Bitcoin price data using the CoinGecko API.
    """
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart'
    params = {
        'vs_currency': 'usd',
        'days': '365',  
        'interval': 'daily'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  

    data = response.json()
    prices = data.get('prices', [])
    df = pd.DataFrame(prices, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date.astype(str)

    return df
    
@logging_info
def fetch_transactions() -> pd.DataFrame:
    """
    Fetch transaction data from BigQuery.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler("bigquery_usage.log")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
     SELECT 
        DATE(timestamp) AS date_,
        SUM(transaction_count) AS total_transactions
    FROM `bigquery-public-data.crypto_bitcoin.blocks`
    WHERE DATE(timestamp_month) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH) 
    AND timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY) AND CURRENT_TIMESTAMP()
    GROUP BY 1
    ORDER BY 1
    """
    query_job = client.query(query)
    results = query_job.result()
    df_transactions_count = results.to_dataframe()
    df_transactions_count['date_'] = df_transactions_count['date_'].astype(str)
    logger.info(f"Bytes processed: {query_job.total_bytes_processed}")
    print(f"This query will process {query_job.total_bytes_processed} bytes.")
    return df_transactions_count

@logging_info
def joining_tables() -> pd.DataFrame:
    """
    Join transaction data and Bitcoin price data into a single DataFrame.
    """
    df_transactions = fetch_transactions()
    df_price = fetch_bitcoin_price()

    joined_df = pd.merge(df_transactions,df_price, how='inner', left_on='date_', right_on='timestamp')

    return joined_df
    
@logging_info
def schema() -> list[dict]:
     """
     create the schema for the bq table
     """
     table_schema = [
        {'name': 'date_', 'type': 'STRING', 'description': 'The date of the transaction'},
        {'name': 'total_transactions', 'type': 'INT64', 'description': 'Total number of transactions'},
        {'name': 'timestamp', 'type': 'STRING', 'description': 'Timestamp of the price'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'Closing price'}
     ]
    
     return table_schema

@logging_info
def main()-> None:
    table = joining_tables()
    table_schema = schema()

    pandas_gbq.to_gbq(
        dataframe = table,
        destination_table = destination_table, 
        project_id = "connection-123",
        table_schema = table_schema,
        credentials = credentials,
        if_exists="replace" 
    )


if __name__ == '__main__':
    main()

















