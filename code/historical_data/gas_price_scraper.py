from graphql_client import GraphqlClient
import json
import logging
import pandas as pd
from tqdm import tqdm
from database_interactions import table_to_df, drop_table, create_table, insert_rows
import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
from constants import VOLUME_LOWER_LIMIT


gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
    headers={}
)

logging.basicConfig(filename='historical_data/data_scraper.log',
                    filemode='a',
                    format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger('urbanGUI')

def get_block_data(table_name, prev_max_time=0):
    rows_set = None
    try:
        rows_set = {}
        data_length = 1000
        while data_length >= 1000:
            print(prev_max_time)
            result = gq_client.execute(
                query=f"""
                        query ($prev_max_time: Int!) {{
                            transactions(where: {{timestamp_gt: $prev_max_time}}, first:1000, orderBy: timestamp, orderDirection: asc) {{
                                id
                                timestamp
                                gasPrice
                            }}
                        }}
                        """,
                operation_name='foo',
                variables={"prev_max_time": int(prev_max_time)})

            transactionsData = json.loads(result)
            if 'data' in transactionsData:
                transactionsData = transactionsData['data']['transactions']
            else:
                raise Exception(f'Error fetching transaction data: {str(transactionsData)}')

            if len(transactionsData) != 0:
                rows_set.update({txn['id']: (txn['id'], txn['timestamp'], txn['gasPrice']) for txn in transactionsData})
                data_length = len(transactionsData)
                prev_max_time = transactionsData[-1]['timestamp'] if data_length > 0 else prev_max_time
                insert_rows(table_name, [(txn['id'], txn['timestamp'], txn['gasPrice']) for txn in transactionsData])
            else:
                return []

    except Exception as e:
        print(f'ERROR: {table_name} {e}')

    finally:
        return list(rows_set.values()) if rows_set is not None else []


def reinitialise_all_liquidity_pool_data():
    table_name = 'gas_prices'
    
    rows = get_block_data(table_name)
    if len(rows) > 0:
        drop_table(table_name)
        create_table(table_name, [('id', 'VARCHAR(255)'), ('timestamp', 'BIGINT'), ('gas_price_wei', 'NUMERIC')])
        insert_rows(table_name, rows)


def refresh_database():
    table_name = 'gas_prices'

    df = table_to_df(
        command=f'SELECT max(timestamp) as max_period_start_unix FROM {table_name};')
    
    rows = get_block_data(table_name, df['max_period_start_unix'].iloc[0] if df['max_period_start_unix'].iloc[0] is not None else 0)

    if len(rows) > 0:
        insert_rows(table_name, rows)

if __name__ == "__main__":
    # reinitialise_all_liquidity_pool_data()
    refresh_database()
