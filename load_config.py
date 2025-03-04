from google.cloud import bigquery
import pandas as pd
import requests
from typing import Any
import os
import datetime
from datetime import timedelta 
import schedule
import time
import yfinance as yf

def fetch_transactions() -> pd.DataFrame:
    """
    Fetch transaction data from BigQuery.
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\nrade\Desktop\connection-123-892e002c2def.json"

    client = bigquery.Client()
    query = """
    SELECT 
        DATE(timestamp, DAY) AS date_,
        SUM(transaction_count) AS total_transactions
    FROM `bigquery-public-data.crypto_bitcoin.blocks`
    WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY) AND CURRENT_TIMESTAMP()
    GROUP BY 1
    ORDER BY 1
    """
    query_job = client.query(query)
    results = query_job.result()
    df_transactions_count = results.to_dataframe()
    return df_transactions_count


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
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date

    return df

def import_sp_500() -> pd.DataFrame():
    ticker = "^GSPC"

    end_date = datetime.date.today()
    start_date = end_date - timedelta(days=365)
    
    sp500_data = yf.download(ticker, start=start_date, end=end_date)
    sp500_data = sp500_data.reset_index()

    return sp500_data

def joining_tables() -> pd.DataFrame:
    """
    Join transaction data and Bitcoin price data into a single DataFrame.
    """
    df_transactions = fetch_transactions()
    df_price = fetch_bitcoin_price()
    sp500_transactions = import_sp_500()

    joined_df = pd.merge(df_transactions,df_price, how='inner', left_on='date_', right_on='timestamp')
    joined_df = pd.merge(joined_df,sp500_transactions,how='left',left_on='date_',right_on='Date')

    joined_df = joined_df.drop(columns=['timestamp','Date'])

    return joined_df


def loading_data(data: pd.DataFrame) -> None:
    """
    Load the combined data into BigQuery.
    """
    client = bigquery.Client()
    project_id = 'connection-123'
    dataset_id = 'bitcoin'
    table_id = 'combined_transactions_bitcoin'

    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    try:
        load_job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
        load_job.result()  # Waits for the job to complete
        print("Transaction data loaded successfully.")
    except Exception as e:
        print(f"An error occurred while loading transaction data: {e}")


def main() -> None:
    """
    Main function to fetch, process, and load data.
    """
    data = joining_tables()
    loading_data(data)

def scheduler():
    main()
    schedule.every().day.at("09:00").do(my_daily_task)
    
    while True:
        schedule.run_pending()
        time.sleep(18)
        print("checking the execution of the script")

if __name__ == '__main__':
    main()
