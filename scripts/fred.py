import os 

api_key = "fred_api_key.txt"

with open (api_key,'r') as f:
    content = f.read()
    print(content)