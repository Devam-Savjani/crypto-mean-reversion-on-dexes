import csv
from graphql_client import GraphqlClient
import json
import os
import pandas as pd
import logging
from tqdm import tqdm
from database_interactions import table_to_df, drop_table, create_table, insert_rows, drop_all_tables_except_table

header = ['id', 'periodStartUnix', 'token0Price', 'token1Price', 'liquidity', 'feesUSD']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', #https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql
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

def get_block_data(pool_address, table_name):
    rows_set = None
    try:
        result = gq_client.execute(
            query="""
                query ($id: ID!) {
                    pool(id: $id) {
                        poolHourData(orderBy: periodStartUnix, first: 1000) {
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
            variables={"id": pool_address})
        
        hourlyData = json.loads(result)['data']['pool']['poolHourData']

        rows_set = {}
        rows_set.update({hourData['id'] : tuple([hourData[key] for key in header] + [float(hourData['token1Price']) / float(hourData['token0Price']) if float(hourData["token0Price"]) > 0 else 0]) for hourData in hourlyData})
        
        while len(hourlyData) >= 1000:
            prev_max_time = hourlyData[-1]['periodStartUnix']

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
                variables={"id": pool_address, "prev_max_time": prev_max_time})

            hourlyData = json.loads(result)['data']['pool']['poolHourData']
            rows_set.update({hourData['id'] : tuple([hourData[key] for key in header] + [float(hourData['token1Price']) / float(hourData['token0Price']) if float(hourData["token0Price"]) > 0 else 0]) for hourData in hourlyData})

    except Exception as e:
        print(f'ERROR: {table_name} {e}')
        logger.error('Failed to fetch some data for address ' + pool_address + ' : ' +  table_name)
    finally:
        if rows_set is not None:
            drop_table(table_name)
            create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), ('token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC'), ('fees_USD', 'NUMERIC'), ('token1_Price_Ratio', 'NUMERIC')])
            insert_rows(table_name, list(rows_set.values()))

drop_all_tables_except_table('liquidity_pools')
df = table_to_df(command="SELECT pool_address, token0, token1 FROM liquidity_pools WHERE volume_usd >= 100000000;")
logger.info('Begining to fetch data on pools')

for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    table_name = '"' + f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'
    get_block_data(row['pool_address'], table_name)
