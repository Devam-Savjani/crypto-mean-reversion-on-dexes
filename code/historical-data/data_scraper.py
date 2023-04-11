import csv
from graphql_client import GraphqlClient
import json
import os
import pandas as pd
import logging
from tqdm import tqdm
from database_interactions import table_to_df, drop_table, create_table, insert_rows, drop_all_tables_except_table

header = ['id', 'periodStartUnix', 'token0Price',
          'token1Price', 'liquidity', 'feesUSD']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
    headers={}
)

logging.basicConfig(filename='data_scraper.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('urbanGUI')

# https://thegraph.com/docs/en/querying/graphql-api/
# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3


def get_block_data(pool_address, table_name, prev_max_time=0):
    rows_set = None
    try:
        rows_set = {}
        data_length = 1000
        while data_length >= 1000:
            result = gq_client.execute(
                query="""
                    query ($id: ID!, $prev_max_time: Int!) {
                        pool(id: $id) {
                            poolHourData(
                                where: {periodStartUnix_gt: $prev_max_time}, orderBy: periodStartUnix, first: 1000
                                ){
                                    id
                                    token0Price
                                    token1Price
                                    periodStartUnix
                                    liquidity
                                    feesUSD
                                }
                        }
                    }
                """,
                operation_name='foo',
                variables={"id": pool_address, "prev_max_time": int(prev_max_time)})

            hourlyData = json.loads(result)
            if 'data' in hourlyData:
                hourlyData = hourlyData['data']['pool']['poolHourData']
            else:
                raise Exception(hourlyData)
            

            rows_set.update({hourData['id']: tuple([hourData[key] for key in header] + [float(hourData['token0Price']) / float(
                hourData['token1Price']) if float(hourData["token1Price"]) > 0 else 0]) for hourData in hourlyData})
            
            data_length = len(hourlyData)
            prev_max_time = hourlyData[-1]['periodStartUnix'] if data_length > 0 else prev_max_time

    except Exception as e:
        print(f'ERROR: {table_name} {e}')
        logger.error('Failed to fetch some data for address ' +
                     pool_address + ' : ' + table_name)

    finally:
        return list(rows_set.values()) if rows_set is not None else []

def reinitialise_all_liquidity_pool_data():
    drop_all_tables_except_table('liquidity_pools')
    df = table_to_df(
        command="SELECT pool_address, token0, token1 FROM liquidity_pools WHERE volume_usd >= 10000000000;")
    logger.info('Begining to fetch data on pools')

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        table_name = '"' + \
            f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'
        rows = get_block_data(row['pool_address'], table_name)
        if len(rows) > 0:
            drop_table(table_name)
            create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), (
                'token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC'), ('fees_USD', 'NUMERIC'), ('token0_Price_Per_token1', 'NUMERIC')])
            insert_rows(table_name, rows)

def refresh_database():
    df = table_to_df(command="SELECT pool_address, token0, token1 FROM liquidity_pools WHERE volume_usd >= 10000000000;")
    df_liquidity_pool_tables = list(table_to_df(command=f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'liquidity_pools'")['tablename'])
    df_liquidity_pool_tables = ['"' + tn + '"' for tn in df_liquidity_pool_tables]
    logger.info('Refreshing liquidity pool data tables')

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        table_name = '"' + f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'
        
        if table_name in df_liquidity_pool_tables:
            df = table_to_df(command=f'SELECT max(id) as max_id, max(period_start_unix) as max_period_start_unix FROM {table_name};')
            rows = get_block_data(row['pool_address'], table_name, df['max_period_start_unix'].iloc[0])

            if len(rows) > 0:
                insert_rows(table_name, rows)
        else:
            rows = get_block_data(row['pool_address'], table_name)
            
            if len(rows) > 0:
                create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), (
                    'token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC'), ('fees_USD', 'NUMERIC'), ('token0_Price_Per_token1', 'NUMERIC')])
                insert_rows(table_name, rows)


# reinitialise_all_liquidity_pool_data()
refresh_database()