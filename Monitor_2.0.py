
#task2:把data生成的[historical_funding_rates.json]从本地读取改成从google sql读取--已完成
#task3：[nginx_results]里的[monitor_latest.csv]同时存到google cloud--已完成
#把monitor_latest.csv存储到了
#instance_connection_name = 'jz0328:us-central1:data-instance'
#db_user = 'root'
#db_pass = 'KKbhast0088!'
#db_name = 'Monitor_APR_Attribute'
#需要在本地下载service-account-file.json和这个monitor2.0py后才可以进行
import requests
import pandas as pd
import os
from datetime import datetime, timezone
import websocket
import json
import ssl
import threading
from threading import Lock
from google.cloud.sql.connector import Connector
import pymysql

# Set Binance API Key as an environment variable
api_key = os.getenv('BINANCE_API_KEY')

# Define the directory path to get the raw data
#这个需要根据本地路径改变
directory_path = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/ArbFunding_Data')
directory_portfolio = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/Portfolio')
directory_path_ngix_server = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/nginx_results')

os.makedirs(directory_path, exist_ok=True)  # Ensure the directory exists
os.makedirs(directory_portfolio, exist_ok=True)
os.makedirs(directory_path_ngix_server, exist_ok=True)

# Generate a UTC timestamp
utc_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Constructing the filenames with the specified directory
csv_filename = f'{directory_path}/latest_fundingrates_UTC_{utc_timestamp}.csv'
csv_monitor_latest_ngix = f'{directory_path_ngix_server}/monitor_latest.csv'
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

# Define the number of days of data needed
days = 70

# Global weights for APR calculation
WEIGHTS = [W_next, W_prev, W3, W7, W30]

# Allocation percentages for the top 5
ALLOCATIONS = [A1, A2, A3, A4, A5]

# 配置Google Cloud SQL连接
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/zhangjingyi/Desktop/intern/Codebase/service-account-file.json"

instance_connection_name = 'jz0328:us-central1:data-instance'
db_user = 'root'
db_pass = 'KKbhast0088!'

# 创建连接器
connector = Connector()

def getconn(db_name):
    conn = connector.connect(
        instance_connection_name,
        "pymysql",
        user=db_user,
        password=db_pass,
        db=db_name
    )
    return conn

# 创建表结构
def create_table():
    #这里json改成Monitor_APR_Attribute
    #connection = getconn('json')
    connection = getconn('Monitor_APR_Attribute')
    cursor = connection.cursor()
    
    # 检查表是否存在，如果存在则删除
    cursor.execute("DROP TABLE IF EXISTS monitor_results")
    
    create_table_query = '''
    CREATE TABLE monitor_results (
        apr_id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(50) NOT NULL,
        time TIMESTAMP NOT NULL,
        previous_funding_rate DECIMAL(10, 5) NOT NULL,
        next_funding_rate DECIMAL(10, 5) NOT NULL,
        _3_day_cum_funding DECIMAL(10, 5) NOT NULL,
        _7_day_cum_funding DECIMAL(10, 5) NOT NULL,
        _30_day_cum_funding DECIMAL(10, 5) NOT NULL,
        apr_score DECIMAL(10, 5) NOT NULL,
        allocation_percentage DECIMAL(10, 5) NOT NULL,
        simulator_id INT NOT NULL
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()

# 获取交易对符号和资金费率数据从 RDS
def get_symbols_and_rates_from_rds():
    connection = getconn('raw_data')
    query = "SELECT * FROM funding_rates"
    df = pd.read_sql(query, connection)
    connection.close()
    
    symbols = df['symbol'].unique().tolist()
    fundingRates = {symbol: df[df['symbol'] == symbol].to_dict(orient='records') for symbol in symbols}
    return symbols, fundingRates

# Combine data into a single DataFrame for historical analysis
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

## Step 1 to get data from RDS and store it into a dataframe
symbols, fundingRates = get_symbols_and_rates_from_rds()
historical_funding_rate = combine_data(fundingRates)

print("Step 1 completed: data retrieved from RDS and saved into historical_funding_rate data frame")

## Step 2 get the realtime latest funding rates data
latest_funding_rate_df = pd.DataFrame(columns=['Symbol', 'Latest Funding Rate'])

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
    symbols, _ = get_symbols_and_rates_from_rds()  # Ensure it's unpacking properly
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

def convert_timestamp(utc_timestamp):
    return datetime.strptime(utc_timestamp, '%Y%m%d_%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

def process_dataframe(df):
    # Convert UTC Timestamp to standard format
    df['UTC Timestamp'] = df['UTC Timestamp'].apply(convert_timestamp)
    
    # Handle NaN values and remove percentage symbols
    df['Target_Allocation%'] = df['Target_Allocation%'].replace('nan%', '0').fillna('0').str.replace('%', '')
    df['Previous Funding Rate'] = df['Previous Funding Rate'].str.replace('%', '')
    df['Latest Funding Rate'] = df['Latest Funding Rate'].str.replace('%', '')
    df['3 Day Cum Funding APR'] = df['3 Day Cum Funding APR'].str.replace('%', '')
    df['7 Day Cum Funding APR'] = df['7 Day Cum Funding APR'].str.replace('%', '')
    df['30 Day Cum Funding APR'] = df['30 Day Cum Funding APR'].str.replace('%', '')
    df['Implied APR'] = df['Implied APR'].str.replace('%', '')
    return df

if __name__ == "__main__":
    
    # Step 2: Connect to the WebSocket
    ws, thread = connect_websocket()
    try:
        thread.join(timeout=30)
    finally:
        ws.close()

    print("Step 2 completed: realtime latest funding rates retrieved from Binance websocket")

    # Step 3: Calculate monitored results 
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

    print("Step 3 completed: implied APR and allocation calculated")

    # Save the DataFrame to a CSV file with UTC timestamp for record
    final_data.to_csv(f"{directory_path}/Monitor_APR_Score_{utc_timestamp}.csv", index=False)
    print(f"Data saved to 'Monitor_APR_Score_{utc_timestamp}.csv'")

    # Update the DataFrame to the 0max1 FArb Monitor database
    final_data.to_csv(csv_monitor_latest_portfolio, index=False)
    print(f"Data saved to {csv_monitor_latest_portfolio}")

    # Create a new DataFrame by copying the original
    final_percentage_data = final_data.copy()

    # Convert selected columns to percentages and round to 2 decimal places
    for column in ['Implied APR', 'Latest Funding Rate', 'Previous Funding Rate', '3 Day Cum Funding APR', '7 Day Cum Funding APR', '30 Day Cum Funding APR', 'Target_Allocation%']:  
        final_percentage_data[column] = (final_percentage_data[column].astype(float) * 100).round(2).astype(str) + '%'

    # Add the UTC timestamp as a new column to the DataFrame
    final_percentage_data['UTC Timestamp'] = utc_timestamp

    # Process the final DataFrame
    final_percentage_data = process_dataframe(final_percentage_data)

    # Update the DataFrame to the 0max1 FArb Monitor database
    final_percentage_data.to_csv(csv_monitor_latest_ngix, index=False)
    print(f"Data saved to {csv_monitor_latest_ngix}")

    # 创建表并插入数据
    create_table()
    final_percentage_data = final_percentage_data.fillna('0')

    def insert_data_to_gcsql(df):
        #这里json改成Monitor_APR_Attribute
        #connection = getconn('json')
        connection = getconn('Monitor_APR_Attribute')
        cursor = connection.cursor()

        insert_query = '''
        INSERT INTO monitor_results (
            symbol, time, previous_funding_rate, next_funding_rate,
            _3_day_cum_funding, _7_day_cum_funding, _30_day_cum_funding,
            apr_score, allocation_percentage, simulator_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['symbol'], row['UTC Timestamp'], row['Previous Funding Rate'], row['Latest Funding Rate'],
                row['3 Day Cum Funding APR'], row['7 Day Cum Funding APR'], row['30 Day Cum Funding APR'],
                row['Implied APR'], row['Target_Allocation%'], 1  # 假设 simulator_id 为 1
            ))

        connection.commit()
        cursor.close()
        connection.close()

    # 插入数据到 Google Cloud SQL
    insert_data_to_gcsql(final_percentage_data)
    print("step4: monitor APR attribute (monitor latest) upload to google cloud")
    # 查询并输出前几行数据
    def fetch_data_from_gcsql(limit=5):
        #这里json改成Monitor_APR_Attribute
        #connection = getconn('json')
        connection = getconn('Monitor_APR_Attribute')
        query = f"SELECT * FROM monitor_results LIMIT {limit}"
        df = pd.read_sql(query, connection)
        connection.close()
        return df

    result_df = fetch_data_from_gcsql()
    print("Fetched data from Google Cloud SQL:")
    print(result_df)