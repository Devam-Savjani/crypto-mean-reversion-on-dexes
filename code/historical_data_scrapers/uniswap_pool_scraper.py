import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(os.path.realpath(current))
sys.path.append(os.path.dirname(parent))
import json
import logging
import pandas as pd
from tqdm import tqdm
from utils.graphql_client import GraphqlClient
from utils.database_interactions import table_to_df, drop_table, create_table, insert_rows, drop_all_tables_given_condition
from utils.constants import LIQUIDITY_POOLS_OF_INTEREST_TABLE_QUERY

header = ['id', 'periodStartUnix', 'token0Price', 'token1Price', 'liquidity']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
    headers={}
)

logging.basicConfig(filename='historical_data/data_scraper.log',
                    filemode='a',
                    format='%(asctime)s, %(name)s %(levelname)s %(message)s',
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
                raise Exception(f'Error fetching pool data: {str(hourlyData)}')

            if len(hourlyData) != 0:
                rows_set.update({hourData['id']: tuple(
                    [hourData[key] for key in header]) for hourData in hourlyData})

                data_length = len(hourlyData)
                prev_max_time = hourlyData[-1]['periodStartUnix'] if data_length > 0 else prev_max_time

            else:
                return []

    except Exception as e:
        print(f'ERROR: {table_name} {e}')
        logger.error('Failed to fetch some data for address ' +
                     pool_address + ' : ' + table_name)

    finally:
        return list(rows_set.values()) if rows_set is not None else []


def reinitialise_all_liquidity_pool_data():
    drop_all_tables_given_condition("tablename LIKE '%_%_0x%'")

    df = table_to_df(command=LIQUIDITY_POOLS_OF_INTEREST_TABLE_QUERY, path_to_config='utils/database.ini')
    logger.info('Begining to reinitialise liquidity pool data tables')

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        table_name = '"' + \
            f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'
        rows = get_block_data(row['pool_address'], table_name)
        if len(rows) > 0:
            drop_table(table_name)
            create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), (
                'token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC')])
            insert_rows(table_name, rows)

    logger.info('Completed: Reinitialise liquidity pool data tables')


def refresh_liquidity_pool_data():

    df = table_to_df(command=LIQUIDITY_POOLS_OF_INTEREST_TABLE_QUERY, path_to_config='utils/database.ini')

    df_liquidity_pool_tables = list(table_to_df(
        command=f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%_%_0x%'", path_to_config='utils/database.ini')['tablename'])
    df_liquidity_pool_tables = ['"' + tn +
                                '"' for tn in df_liquidity_pool_tables]
    logger.info('Refreshing liquidity pool data tables')

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        table_name = '"' + \
            f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'

        if table_name in df_liquidity_pool_tables:
            df = table_to_df(
                command=f'SELECT max(id) as max_id, max(period_start_unix) as max_period_start_unix FROM {table_name};', path_to_config='utils/database.ini')
            rows = get_block_data(
                row['pool_address'], table_name, df['max_period_start_unix'].iloc[0])

            if len(rows) > 0:
                insert_rows(table_name, rows)
        else:
            rows = get_block_data(row['pool_address'], table_name)

            if len(rows) > 0:
                create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), (
                    'token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC')])
                insert_rows(table_name, rows)

    logger.info('Completed: Refresh liquidity pool data tables')


if __name__ == "__main__":
    # reinitialise_all_liquidity_pool_data()
    refresh_liquidity_pool_data()
