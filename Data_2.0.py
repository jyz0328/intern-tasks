#python3 Data_1.0.py then get [historical_funding_rates.json] on [ArbFunding_Data]
#Data_1.0.py
#总结：task1--讓這個文件產生的json file[historical_funding_rates.json]自动傳到google cloud sql 已完成
#[historical_funding_rates.json]上传到了
##instance_connection_name = 'jz0328:us-central1:data-instance'
#db_user = 'root'
#db_pass = 'KKbhast0088!'
#db_name = 'raw_data'
#需要在本地下载service-account-file.json和这个data2.0py后才可以进行
import requests
import pandas as pd
import os
import json
from datetime import datetime
from google.cloud.sql.connector import Connector
import pymysql

# Set Binance API Key as an environment variable
api_key = os.getenv('BINANCE_API_KEY')

# Define the number of days of data needed
days = 70

# Define the directory path to save the raw data
#这个需要根据本地路径改变
directory_path = os.path.expanduser('/Users/zhangjingyi/Desktop/intern/ArbFunding_Data')
os.makedirs(directory_path, exist_ok=True)  # Create the directory if it doesn't exist

# Error fetching symbols: 451 means IP is restricted please open VPN
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
        json.dump(data, file, indent=4)
    print(f"Data saved to '{full_path}'")

# 配置Google Cloud SQL连接
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/zhangjingyi/Desktop/intern/Codebase/service-account-file.json"
#存储到了这样的信息的sql
instance_connection_name = 'jz0328:us-central1:data-instance'
db_user = 'root'
db_pass = 'KKbhast0088!'
db_name = 'raw_data'

# 创建连接器
connector = Connector()

def getconn():
    conn = connector.connect(
        instance_connection_name,
        "pymysql",
        user=db_user,
        password=db_pass,
        db=db_name
    )
    return conn

# 删除现有表并重新创建表
def recreate_table():
    connection = getconn()
    cursor = connection.cursor()
    
    drop_table_query = "DROP TABLE IF EXISTS funding_rates;"
    cursor.execute(drop_table_query)
    
    create_table_query = """
    CREATE TABLE funding_rates (
        funding_id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(50) NOT NULL,
        fundingTime BIGINT NOT NULL,
        fundingRate DECIMAL(10, 8) NOT NULL,
        markPrice DECIMAL(15, 8) NOT NULL,
        Time TIMESTAMP NOT NULL
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    
    cursor.close()
    connection.close()

# 读取 JSON 文件
def read_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# 转换 fundingTime 并准备数据
def prepare_records(data):
    records = []
    for symbol, records_list in data.items():
        if symbol == "symbols":
            continue  # 跳过不需要处理的键
        print(f"Processing symbol: {symbol}")  # 调试输出
        for record in records_list:
            try:# 确保 fundingTime 是整数类型
                print(f"Record: {record}")  # 调试输出
                fundingTime = int(record['fundingTime'])  # 保留原始毫秒时间戳
                fundingRate = float(record['fundingRate'])
                markPrice = float(record['markPrice'])
                # 将时间格式化为人类可读形式
                Time = datetime.utcfromtimestamp(fundingTime / 1000).strftime('%Y-%m-%d %H:%M:%S')
                records.append((record['symbol'], fundingTime, fundingRate, markPrice, Time))
            except KeyError as e:
                print(f"Missing key in record: {record}, error: {e}")  # 调试输出
            except ValueError as e:
                print(f"Value error in record: {record}, error: {e}")  # 调试输出
            except TypeError as e:
                print(f"Type error in record: {record}, error: {e}")  # 调试输出
    return records

# 批量插入数据
def insert_data_in_batches(records, batch_size=1000):
    connection = getconn()
    cursor = connection.cursor()
    
    insert_query = ("INSERT INTO funding_rates (symbol, fundingTime, fundingRate, markPrice, Time) "
                    "VALUES (%s, %s, %s, %s, %s)")
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_query, batch)
        connection.commit()
        print(f"Inserted batch {i // batch_size + 1} of {len(records) // batch_size + 1}")
    
    cursor.close()
    connection.close()

def query_data():
    connection = getconn()
    cursor = connection.cursor()
    
    query = "SELECT * FROM funding_rates LIMIT 10"
    cursor.execute(query)
    
    results = cursor.fetchall()
    
    for row in results:
        print(row)
    
    cursor.close()
    connection.close()

def main():
    # 获取交易对符号
    symbols = get_futures_symbols()

    all_data_frames = []
    # 初始化保存所有数据的字典
    Funding_rates_historical_data = {}

    # 获取并保存所有符号的数据
    for symbol in symbols:
        historical_rates = get_historical_funding_rates(symbol, limit=days * 3)
        if historical_rates:
            df = pd.DataFrame(historical_rates)
            if not df.empty:
                df['symbol'] = symbol
                all_data_frames.append(df)

    # 保存 Funding_rates_historical_data 一次
    Funding_rates_historical_data = {symbol: get_historical_funding_rates(symbol, days * 3) for symbol in symbols}

    # 添加符号列表到数据字典
    Funding_rates_historical_data['symbols'] = symbols

    # 保存数据到 JSON 文件
    save_data_to_json(Funding_rates_historical_data, 'historical_funding_rates.json')

    # 创建数据库表
    print("Recreating table...")
    recreate_table()

    # 准备数据并插入到数据库
    print("Preparing records...")
    records = prepare_records(Funding_rates_historical_data)

    print("Reading JSON file...")
    #这个需要根据本地路径改变
    file_path = '/Users/zhangjingyi/Desktop/intern/ArbFunding_Data/historical_funding_rates.json'
    data = read_json(file_path)
    
    print("Inserting data...")
    insert_data_in_batches(records)
    
    print("Data inserted successfully.")

    # 查询并打印数据
    print("Querying data...")
    query_data()

if __name__ == "__main__":
    main()
