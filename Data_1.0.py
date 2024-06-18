#python3 Data_1.0.py then get [historical_funding_rates.json] on [ArbFunding_Data]
#Data_1.0.py
import requests
import pandas as pd
import os
import json
from datetime import datetime, timezone

# Set Binance API Key as an environment variable
api_key = os.getenv('BINANCE_API_KEY')

# Define the number of days of data needed
days = 70

# Define the directory path to save the raw data
#directory_path = '/home/jason.wu/0MAX1/ArbFunding_Data'
directory_path = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/ArbFunding_Data')
os.makedirs(directory_path, exist_ok=True)  # Create the directory if it doesn't exist

# Error fetching symbols: 451 means ip is restricted please open VPN

def get_futures_symbols():
    url = 'https://dapi.binance.com/dapi/v1/exchangeInfo'
    headers = {'X-MBX-APIKEY': api_key}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return [item['symbol'] for item in data['symbols'] if item['contractType'] == 'PERPETUAL']
    else:
        print(f"Error fetching symbols: {response.status_code}")
        return []

def get_historical_funding_rates(symbol, limit):
    url = 'https://dapi.binance.com/dapi/v1/fundingRate'
    headers = {'X-MBX-APIKEY': api_key}
    params = {'symbol': symbol, 'limit': limit}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching historical rates for {symbol}: {response.status_code}")
        return []

def save_data_to_json(data, filename):
    full_path = os.path.join(directory_path, filename)
    with open(full_path, 'w') as file:
        json.dump(data, file,indent=4)
    print(f"Data saved to '{full_path}'")

symbols = get_futures_symbols()

all_data_frames = []
# Dictionary to hold all data, initially empty
Funding_rates_historical_data = {}

# Fetch and save data for all symbols
for symbol in symbols:
    historical_rates = get_historical_funding_rates(symbol, limit=days*3)
    if historical_rates:
        df = pd.DataFrame(historical_rates)
        if not df.empty:
            df['symbol'] = symbol
            all_data_frames.append(df)

# Save Funding_rates_historical_data once outside the loop
Funding_rates_historical_data = {symbol: get_historical_funding_rates(symbol, days*3) for symbol in symbols}

# Adding the list of symbols to the dictionary under the key 'symbols'
Funding_rates_historical_data['symbols'] = symbols

save_data_to_json(Funding_rates_historical_data, 'historical_funding_rates.json')
