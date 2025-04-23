from datacommons_client.client import DataCommonsClient
from pathlib import Path
import requests

current_dir = Path(__file__).parent
api_key_path = current_dir.parent / "datacoms_apy_key.txt"
api_key_path_str = str(api_key_path.resolve())

with open(api_key_path_str, 'r') as f:
    api_key = f.read().strip()

client = DataCommonsClient(api_key=api_key)

url = "https://api.datacommons.org/v1/observations/series"

# Set parameters
params = {
    "entity": "country/USA",
    "variable": "Amount_EconomicActivity_GrossDomesticProduction_Nominal",
    "key": api_key
}

# Send request
response = requests.get(url, params=params)

# Parse response
data = response.json()

# Print the time series
if "observations" in data:
    for obs in data["observations"]:
        print(f"Date: {obs['date']}, GDP: ${obs['value']}")
else:
    print("No observations found or invalid response.")