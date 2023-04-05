import os
import pandas as pd
from datetime import datetime

liquidity_pool_data_directory = 'liquidity_pool_data'

def get_pools_with_data_till_today():
    files_with_data_till_date = []
    for root, dirs, files in os.walk(liquidity_pool_data_directory):
        for filename in files:
            data = pd.read_csv(os.path.join(root, filename))
            contract_address = data.iloc[0]['id'].split('-')[0]
            start_date = datetime.fromtimestamp(data.iloc[0]['periodStartUnix'])
            end_date = datetime.fromtimestamp(data.iloc[-1]['periodStartUnix'])
            if end_date.date() == datetime.now().date():
                files_with_data_till_date.append(os.path.join(root, filename))
                # print(f"{contract_address}: Start - {start_date} End - {end_date}")

    print(f"Total files with till date: {len(files_with_data_till_date)}")
    return files_with_data_till_date