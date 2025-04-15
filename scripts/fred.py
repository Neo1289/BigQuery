import os
import requests
import pandas as pd
from datetime import datetime
import numpy as np
from typing import List, Dict, TypedDict

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

# Get specific category info (33509: Labor Market Conditions)
def get_specific_category(category_id=32240):
    endpoint = "category"
    params = {"category_id": category_id}
    result = fred_request(endpoint, params)
    
    if result and 'categories' in result:
        category = result['categories'][0]
        print(f"Category ID: {category['id']}")
        print(f"Name: '{category['name']}'")
        print(f"Parent ID: {category['parent_id']}")
        return category
    else:
        print("Failed to retrieve category information")
        return None

# Get children of specific category
def get_specific_category_children(category_id=32240):
    endpoint = "category/children"
    params = {"category_id": category_id}
    result = fred_request(endpoint, params)
    
    if result and 'categories' in result:
        print(f"Children of category ID {category_id}:")
        for category_dict in result['categories']:
            category_id = category_dict['id']
            category_name = category_dict['name']
            parent_id = category_dict['parent_id']
            print(f"  ID: {category_id}, Name: '{category_name}', Parent ID: {parent_id}")
        return result['categories']
    else:
        print("No children found or error retrieving children")
        return None

# Get series for the specific category
def get_specific_category_series(category_id=33509):
    endpoint = "category/series"
    params = {"category_id": category_id}
    result = fred_request(endpoint, params)
    
    if result and 'seriess' in result:
        print(f"Series in category ID {category_id}:")
        for series in result['seriess']:
            series_id = series['id']
            series_title = series['title']
            print(f"  ID: {series_id}, Title: '{series_title}'")
        return result['seriess']
    else:
        print("No series found or error retrieving series")
        return None

# Run functions to explore category ID 33509
if __name__ == "__main__":
    print("Getting information for Labor Market Conditions (ID 33509):")
    get_specific_category()
    print("\n")
    get_specific_category_children()
    print("\n")
    get_specific_category_series()