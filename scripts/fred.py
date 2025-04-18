import os
import requests
import pandas as pd
from datetime import datetime
import numpy as np
from typing import List, Dict, TypedDict, Optional, Any
import logging
from google.oauth2 import service_account
import pandas_gbq
from pathlib import Path


current_dir = Path(__file__).parent
api_key_path = current_dir.parent / "fred_api_key.txt"
api_key_path_str = str(api_key_path.resolve())

destination_table = "bitcoin.fred_inflation_data"

with open(api_key_path_str, 'r') as f:
    api_key = f.read().strip()

base_url = "https://api.stlouisfed.org/fred/"

def fred_request(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if params is None:
        params = {}
    
    params['api_key'] = api_key
    params['file_type'] = 'json'
    
   
    url = base_url + endpoint
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
        
def get_inflation_data(series_id: str = "CPIAUCSL") -> Optional[pd.DataFrame]:  # Consumer Price Index for All Urban Consumers
    endpoint = "series/observations"
    params = {
        "series_id": series_id,
        "observation_start": "2023-01-01",  
        "frequency": "m",  
        "units": "pc1"     # Percent change from a year ago
    }
    result = fred_request(endpoint, params)
    
    if result and 'observations' in result:
        print(f"Retrieved {len(result['observations'])} observations for {series_id}")
        
        # Convert to DataFrame
        df = pd.DataFrame(result['observations'])
        df = df[['date', 'value']]
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date', 'type': 'STRING', 'description': 'The date of the measurement'},
        {'name': 'value', 'type': 'FLOAT64', 'description': 'the value of the CPI measured'}
    ]
    return table_schema
    
def run_etl(credentials) -> None:
    table = get_inflation_data()
    table_schema = schema()

    pandas_gbq.to_gbq(
        dataframe=table,
        destination_table=destination_table,
        project_id="connection-123",
        table_schema=table_schema,
        credentials=credentials,
        if_exists="replace" 
    )

    run_etl.__doc__ = f"fetching data from {get_inflation_data.__name__}, added {len(table)} rows."

if __name__ == "__main__":
    main()
        