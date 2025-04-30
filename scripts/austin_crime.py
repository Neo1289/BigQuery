import pandas as pd
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import os
import logging
from google.oauth2 import service_account

destination_table = "austin_crimes"


def get_crimes(credentials) -> pd.DataFrame:
   
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
      SELECT 
          TIMESTAMP_TRUNC(clearance_date, MONTH) AS month,
          COUNT(unique_key AS total_crimes
      FROM bigquery-public-data.austin_crime.crime
        GROUP BY 1
        ORDER BY 1
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
        {'name': 'month', 'type': 'DATE', 'description': 'The month of the collective crimes committed'},
        {'name': 'total_crimes', 'type': 'INT64', 'description': 'the total amount of crimes commited'}
    ]
    return table_schema

def run_etl(credentials) -> None:
   
    table = get_crimes(credentials)
    table_schema = schema()

    table = pandas_gbq.to_gbq(
        dataframe=table,
        destination_table=destination_table,
        project_id="connection-123",
        table_schema=table_schema,
        credentials=credentials,
        if_exists="replace" 
    )

    table.time_partitioning = bigquery.TimePartitioning(
        type = bigquery.TimePartitioningType.DAY,
        field = "month"
    )

    client.create_table(table=table)

    run_etl.__doc__ = f"fetching from {get_crimes.__name__}, added {len(table)} rows."

if __name__ == '__main__':
    main()


