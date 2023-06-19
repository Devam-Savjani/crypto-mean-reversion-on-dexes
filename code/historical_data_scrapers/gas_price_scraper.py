import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
import json
import logging
import pandas as pd
from tqdm import tqdm
import time
from utils.graphql_client import GraphqlClient
from utils.database_interactions import table_to_df, drop_table, create_table, insert_rows
from utils.constants import LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY


gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
    headers={}
)

def get_block_data(table_name, min_time, max_time):
    rows_set = {}
    try:
        for timestamp in tqdm(range(min_time, max_time+(60*60), 60*60)):
            found_result = False
            window_size_in_minutes = 5

            while not found_result:
                result = gq_client.execute(
                    query=f"""
                            query ($min_time: Int!, $max_time: Int!) {{
                                transactions(where: {{timestamp_gt: $min_time, timestamp_lt: $max_time}}, first:1000, orderBy: timestamp, orderDirection: asc) {{
                                    id
                                    timestamp
                                    gasPrice
                                }}
                            }}
                            """,
                    operation_name='foo',
                    variables={"min_time": timestamp - (60 * window_size_in_minutes), "max_time": timestamp + (60 * window_size_in_minutes)})
            
                transaction_data = json.loads(result)
                if 'data' in transaction_data:
                    transaction_data = transaction_data['data']['transactions']

                    if len(transaction_data) != 0:
                        transaction_data_sorted = sorted(transaction_data, key=lambda x:abs(timestamp - int(x['timestamp'])))
                        rows_set.update({timestamp: (timestamp, transaction_data_sorted[0]['gasPrice'])})
                        found_result = True
                    else:
                        window_size_in_minutes += 5
                else:
                    raise Exception(f'Error fetching transaction data: {str(transaction_data)}')
            
            insert_rows(table_name, [rows_set[timestamp]])

    except Exception as e:
        print(f'ERROR: {table_name} {e}')

    finally:
        return list(rows_set.values()) if rows_set is not None else []


def reinitialise_gas_price_data():
    table_name = 'gas_prices'

    df = table_to_df(
        command=f"""
            CREATE OR REPLACE FUNCTION get_min_timestamp()
                RETURNS TABLE (table_name TEXT, min_timestamp BIGINT)
                LANGUAGE plpgsql AS $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN
                ({LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY})
            LOOP
                EXECUTE FORMAT ('SELECT MIN(period_start_unix) FROM %I', r.table_name) INTO min_timestamp;
                table_name := r.table_name;
                RETURN next;
            END LOOP;
            END $$;

            SELECT min_timestamp FROM get_min_timestamp();
        """, path_to_config='utils/database.ini')
    
    drop_table(table_name)
    create_table(table_name, [('timestamp', 'BIGINT'), ('gas_price_wei', 'NUMERIC')])
    rows = get_block_data(table_name, min(df['min_timestamp']), int(time.time() - (time.time() % (60 * 60))))

def refresh_gas_price_data():
    table_name = 'gas_prices'

    df = table_to_df(
        command=f'SELECT max(timestamp) as max_period_start_unix FROM {table_name};', path_to_config='utils/database.ini')
    
    rows = get_block_data(table_name, df['max_period_start_unix'].iloc[0] if df['max_period_start_unix'].iloc[0] is not None else 0, int(time.time() - (time.time() % (60 * 60))))

if __name__ == "__main__":
    # reinitialise_all_liquidity_pool_data()
    refresh_gas_price_data()
