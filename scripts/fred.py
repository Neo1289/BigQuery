import os
import requests
import pandas as pd
from datetime import datetime

# Read API key
with open('../fred_api_key.txt', 'r') as f:
    api_key = f.read().strip()

# Base URL for FRED API
base_url = "https://api.stlouisfed.org/fred/"

# Function to make API requests
def fred_request(endpoint, params=None):
    if params is None:
        params = {}
    
    # Add API key to parameters
    params['api_key'] = api_key
    params['file_type'] = 'json'
    
    # Make request
    url = base_url + endpoint
    response = requests.get(url, params=params)
    
    # Check for successful response
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

# Get root categories
def get_categories(category_id=0):
    endpoint = "category/children"
    params = {"category_id": category_id}
    result = fred_request(endpoint, params)

    for obj in result['categories']:
        for key in obj.items():
            print(key[0])
        
    
# Run the function to see root categories
get_categories()