#GET (1)[latest_fundingrates_UTC_20240603_213852] and [Monitor_APR_Score_20240603_213852] in [ArbFunding_Data]
#get (2)[monitor_latest_portfolio_calculation] IN [Portfolio]
#get (3)[monitor_latest] in [nginx_results]
import requests
import pandas as pd
import os
from datetime import datetime, timezone
import websocket
import json
import ssl
import threading
from threading import Lock
api_key = os.getenv('BINANCE_API_KEY')

# Define the directory path to get the raw data
directory_path = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/ArbFunding_Data')
directory_portfolio = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/Portfolio')
directory_path_ngix_server = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/nginx_results')
#directory_path = '/home/jason.wu/0MAX1/ArbFunding_Data'
#directory_portfolio= '/home/jason.wu/0MAX1/Portfolio'
#directory_path_ngix_server = '/var/www/html/results'

os.makedirs(directory_path, exist_ok=True)  # Ensure the directory exists
# Ensure the directories exist
os.makedirs(directory_portfolio, exist_ok=True)
os.makedirs(directory_path_ngix_server, exist_ok=True)
json_file_path = os.path.join(directory_path, 'historical_funding_rates.json')##我想需要改的就是这一步啊 需要改成  改成rds讀取 

# Generate a UTC timestamp
utc_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Constructing the filenames with the specified directory
csv_filename = f'{directory_path}/latest_fundingrates_UTC_{utc_timestamp}.csv'

# Constructing the filenames for the database of Monitor_latest
csv_monitor_latest_ngix = f'{directory_path_ngix_server}/monitor_latest.csv'

# Constructing the filenames for the database of Monitor_latest
csv_monitor_latest_portfolio = f'{directory_portfolio}/monitor_latest_portfolio_calculation.csv'



# Define the weights
W_next = 0.25
W_prev = 0.15
W3 = 0.3
W7 = 0.2
W30 = 0.1



# Define the allocations
A1 = 0.4
A2 = 0.35
A3 = 0.25
A4 = 0
A5 = 0

#Define the number of days of data needed
days = 70

# Global weights for APR calculation
WEIGHTS = [W_next,W_prev,W3, W7, W30]

# Allocation percentages for the top 5
ALLOCATIONS = [A1, A2, A3, A4, A5]

## Step 1 to get data from a json file and store it into a dataframe

def get_symbols_and_rates_from_file(file_path):
    """Retrieve symbols and their corresponding funding rates from a JSON file."""
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        symbols = data.get("symbols", [])
        if not all(isinstance(symbol, str) for symbol in symbols):  # Validate all symbols are strings
            raise ValueError("All symbols must be strings.")
        fundingRate = {sym: data.get(sym, []) for sym in symbols}  # Extract funding rate data for each symbol
        return symbols, fundingRate
    except FileNotFoundError:
        print("The specified file was not found.")
        return [], {}
    except json.JSONDecodeError:
        print("Error decoding JSON from the file.")
        return [], {}
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], {}

def combine_data(fundingRates):
    """Combine historical funding rate data into a single DataFrame."""
    history_df = []
    for symbol, rates in fundingRates.items():
        if rates:  # Ensure there is data
            df = pd.DataFrame(rates)
            df['symbol'] = symbol  # Add symbol column
            # Convert 'fundingRate' to numeric, coerce errors to NaN
            df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
            # Optionally handle or remove NaN values if necessary
            df.dropna(subset=['fundingRate'], inplace=True)
            history_df.append(df)
    return pd.concat(history_df, ignore_index=True) if history_df else pd.DataFrame()

# Fetch symbols and funding rates from JSON file
symbols, fundingRates = get_symbols_and_rates_from_file(json_file_path)

# Combine data into a single DataFrame for historical analysis
historical_funding_rate = combine_data(fundingRates)

print ("Step 1 completed: data retrieved from a json file and save it into historical_funding_rate data frame")

## Step 2 get the realtime latest funding rates data
latest_funding_rate_df = pd.DataFrame(columns=['Symbol', 'Latest Funding Rate'])

def get_symbols_and_rates_from_file(json_file_path):
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        symbols = data.get("symbols", [])
        if not all(isinstance(symbol, str) for symbol in symbols):  # Validation step
            raise ValueError("All symbols should be strings.")
        fundingRate = data.get("fundingRate", {})
        return symbols, fundingRate
    except FileNotFoundError:
        print("The specified file was not found.")
        return [], {}
    except json.JSONDecodeError:
        print("Error decoding JSON from the file.")
        return [], {}

latest_funding_rates = {}

# Initialize a lock
df_lock = Lock()

def on_message(ws, message):
    global latest_funding_rate_df
    data = json.loads(message)
    try:
        if 'data' in data and 's' in data['data'] and 'r' in data['data']:
            symbol = data['data']['s'].upper()
            rate = float(data['data']['r'])
            with df_lock:
                if symbol in latest_funding_rate_df['Symbol'].values:
                    # Update the existing row for the symbol
                    latest_funding_rate_df.loc[latest_funding_rate_df['Symbol'] == symbol, 'Latest Funding Rate'] = rate
                else:
                    # Append new data if symbol not found
                    new_data = pd.DataFrame([[symbol, rate]], columns=['Symbol', 'Latest Funding Rate'])
                    latest_funding_rate_df = pd.concat([latest_funding_rate_df, new_data], ignore_index=True)
    except ValueError as e:
        print(f"Error converting rate to float for symbol {symbol}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, code, reason):
    global latest_funding_rate_df
    print(f"WebSocket closed with code {code}, reason {reason}")
    if not latest_funding_rate_df.empty:
        try:
            latest_funding_rate_df.to_csv(csv_filename, index=False)
            print(f"Latest funding rates data saved to {csv_filename}")
        except Exception as e:
            print(f"Failed to save data: {e}")
    else:
        print("No data to save: DataFrame is empty.")


def on_open(ws):
    global streams
    symbols, _ = get_symbols_and_rates_from_file(json_file_path)  # Ensure it's unpacking properly
    if not symbols:  # Checking if symbols list is empty or not
        print("No symbols found to subscribe.")
        ws.close()  # Close the websocket if no symbols to subscribe
        return
    streams = [f"{symbol.lower()}@markPrice" for symbol in symbols if isinstance(symbol, str)]
    if streams:
        subscribe_message = json.dumps({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        })
        ws.send(subscribe_message)
    else:
        print("No valid streams to subscribe.")
        ws.close()


def connect_websocket():
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp("wss://dstream.binance.com/stream",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    thread = threading.Thread(target=lambda: ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}))
    thread.start()
    return ws, thread

if __name__ == "__main__":
    
    # Connect to the WebSocket
    ws, thread = connect_websocket()
    try:
        thread.join(timeout=30)
    finally:
        ws.close()

print ("Step 2 completed: realtime latest funding rates retrieved from Binance websocket")

## Step 3 calculate monitored results 

def calculate_apr_score(df, symbol, next_rate):
    aprs = [
        next_rate * 3 * 360,
        df.iloc[0]['fundingRate'] * 3 * 360 if len(df) > 1 else 0,
        df.head(9)['fundingRate'].mean() * 3 * 360,
        df.head(21)['fundingRate'].mean() * 3 * 360,
        df.head(90)['fundingRate'].mean() * 3 * 360,
    ]
    apr_score = sum(apr * weight for apr, weight in zip(aprs, WEIGHTS))
    return apr_score

# Assume 'historical_funding_rate_df' is loaded and contains the necessary data
grouped = historical_funding_rate.groupby('symbol')

final_data_list = []

print("Checking if DataFrame exists:", 'latest_funding_rate_df' in globals())
if 'latest_funding_rate_df' in globals():
    print(latest_funding_rate_df.head())

for symbol, data in grouped:
    next_rate_row = latest_funding_rate_df[latest_funding_rate_df['Symbol'] == symbol.upper()]
    if not next_rate_row.empty:
        next_rate = next_rate_row.iloc[0]['Latest Funding Rate']
        data_sorted = data.sort_values('fundingTime', ascending=False)
        latest_data = data_sorted.iloc[0].to_dict()

        # Calculate and store the funding rates and Implied APR
        latest_data.update({
            'Latest Funding Rate': next_rate * 3 * 360,
            'Previous Funding Rate': data_sorted.iloc[0]['fundingRate'] * 3 * 360 if len(data_sorted) > 1 else "N/A",
            '3 Day Cum Funding APR': data_sorted.head(9)['fundingRate'].mean() * 3 * 360 if len(data_sorted) >= 9 else "N/A",
            '7 Day Cum Funding APR': data_sorted.head(21)['fundingRate'].mean() * 3 * 360 if len(data_sorted) >= 21 else "N/A",
            '30 Day Cum Funding APR': data_sorted.head(90)['fundingRate'].mean() * 3 * 360 if len(data_sorted) >= 90 else "N/A",
            'Implied APR': calculate_apr_score(data_sorted, symbol, next_rate)  # Calculate Implied APR
        })

        final_data_list.append(latest_data)
    else:
        print(f"No latest rate available for {symbol}")


# Create final DataFrame and sort by Implied APR
final_data = pd.DataFrame(final_data_list)

# Sort the DataFrame by Implied APR as a float
final_data.sort_values('Implied APR', ascending=False, inplace=True)

# Use None instead of an empty string for better data handling
final_data['Target_Allocation%'] = [None] * len(final_data)

# Apply Target_Allocation% to the top 5 entries
top_five_allocation = [alloc for alloc in ALLOCATIONS[:5]]  # Keep as numbers without converting to string
final_data.iloc[0:5, final_data.columns.get_loc('Target_Allocation%')] = top_five_allocation

# Select only the desired columns to display and save
final_data = final_data[['symbol', 'Implied APR', 'Latest Funding Rate', 'Previous Funding Rate', '3 Day Cum Funding APR', '7 Day Cum Funding APR', '30 Day Cum Funding APR', 'Target_Allocation%']]

print("Step 3 completed: implied APR and allocation calcuated")

# Save the DataFrame to a CSV file with UTC timestamp for record
final_data.to_csv(f"{directory_path}/Monitor_APR_Score_{utc_timestamp}.csv", index=False)
print(f"Data saved to 'Monitor_APR_Score_{utc_timestamp}.csv'")

# Update the DataFrame to the 0max1 FArb Monitor database
#final_data.to_csv(directory_portfolio/csv_monitor_latest_portfolio, index=False)#這個蓋的對嗎
# Update the DataFrame to the Portfolio database
final_data.to_csv(csv_monitor_latest_portfolio, index=False)
print(f"Data saved to {csv_monitor_latest_portfolio}")


# Create a new DataFrame by copying the original
final_percentage_data = final_data.copy()

# Convert selected columns to percentages and round to 2 decimal places
for column in ['Implied APR', 'Latest Funding Rate', 'Previous Funding Rate', '3 Day Cum Funding APR', '7 Day Cum Funding APR', '30 Day Cum Funding APR', 'Target_Allocation%']:  
    final_percentage_data[column] = (final_percentage_data[column] * 100).round(2).astype(str) + '%'

# Add the UTC timestamp as a new column to the DataFrame
final_percentage_data['UTC Timestamp'] = utc_timestamp

# Update the DataFrame to the 0max1 FArb Monitor database
final_percentage_data.to_csv(csv_monitor_latest_ngix, index=False)
print(f"Data saved to {csv_monitor_latest_ngix}")