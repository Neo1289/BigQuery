import pandas as pd
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import os
import logging
from google.oauth2 import service_account

destination_table = "bitcoin.price"

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
    df = df.drop_duplicates(subset='timestamp', keep='first')

    return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'STRING', 'description': 'The date of the price'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'opening price'}
    ]
    return table_schema

def run_etl(credentials) -> None:
   
    table = fetch_bitcoin_price()
    table_schema = schema()

    pandas_gbq.to_gbq(
        dataframe=table,
        destination_table=destination_table,
        project_id="connection-123",
        table_schema=table_schema,
        credentials=credentials,
        if_exists="replace" 
    )

    run_etl.__doc__ = f"fetching the last 365 days bitcoin prices, added {len(table)} rows."

if __name__ == '__main__':
    main()
