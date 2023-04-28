from graphql_client import GraphqlClient
import json
import logging
import pandas as pd
from tqdm import tqdm
from database_interactions import table_to_df, drop_table, create_table, insert_rows, drop_all_tables_except_table

header = ['id', 'periodStartUnix', 'token0Price',
          'token1Price', 'liquidity', 'gasPrice']

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
                swaps_data = gq_client.execute(
                    query="""
                            query pools($min_timestamp: Int!, $max_timestamp: Int!) {
                                pools(where: {volumeUSD_gte: 10000000000}) {
                                    id
                                    swaps(where: {timestamp_gte: $min_timestamp, timestamp_lt: $max_timestamp}) {
                                        id
                                        timestamp
                                        transaction {
                                            id
                                            gasPrice
                                        }
                                    }
                                }
                            }
                        """,
                    operation_name='foo',
                    variables={"min_timestamp": int(hourlyData[0]['periodStartUnix']) - (7*60*60), "max_timestamp": int(hourlyData[-1]['periodStartUnix']) + (7*60*60)})

                swaps_data = json.loads(swaps_data)

                if 'data' in swaps_data:
                    swaps = sum([pool_data['swaps'] for pool_data in swaps_data['data']['pools']], [])
                    df = pd.DataFrame(data={'timestamp': [int(swap['timestamp']) for swap in swaps], 'gasPrice': [
                                      int(swap['transaction']['gasPrice']) for swap in swaps]})
                    for i in range(len(hourlyData)):
                        hourlyData[i]['gasPrice'] = int(df.iloc[(
                            df['timestamp'] - hourlyData[i]['periodStartUnix']).abs().argsort()[:1]]['gasPrice'].values[0])

                else:
                    raise Exception(
                        f'Error fetching gas price data: {str(swaps_data)}')

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
    drop_all_tables_except_table('liquidity_pools')
    df = table_to_df(
        command="SELECT pool_address, token0, token1 FROM liquidity_pools WHERE volume_usd >= 10000000000;")
    logger.info('Begining to reinitialise liquidity pool data tables')

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        table_name = '"' + \
            f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'
        rows = get_block_data(row['pool_address'], table_name)
        if len(rows) > 0:
            drop_table(table_name)
            create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), (
                'token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC'), ('gas_price_wei', 'NUMERIC')])
            insert_rows(table_name, rows)

    logger.info('Completed: Reinitialise liquidity pool data tables')


def refresh_database():
    df = table_to_df(
        command="SELECT pool_address, token0, token1 FROM liquidity_pools WHERE volume_usd >= 10000000000;")
    df_liquidity_pool_tables = list(table_to_df(
        command=f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'liquidity_pools'")['tablename'])
    df_liquidity_pool_tables = ['"' + tn +
                                '"' for tn in df_liquidity_pool_tables]
    logger.info('Refreshing liquidity pool data tables')

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        table_name = '"' + \
            f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"'

        if table_name in df_liquidity_pool_tables:
            df = table_to_df(
                command=f'SELECT max(id) as max_id, max(period_start_unix) as max_period_start_unix FROM {table_name};')
            rows = get_block_data(
                row['pool_address'], table_name, df['max_period_start_unix'].iloc[0])

            if len(rows) > 0:
                insert_rows(table_name, rows)
        else:
            rows = get_block_data(row['pool_address'], table_name)

            if len(rows) > 0:
                create_table(table_name, [('id', 'VARCHAR(255)'), ('period_start_unix', 'BIGINT'), ('token0_Price', 'NUMERIC'), (
                    'token1_Price', 'NUMERIC'), ('liquidity', 'NUMERIC'), ('gas_price_wei', 'NUMERIC')])
                insert_rows(table_name, rows)

    logger.info('Completed: Refresh liquidity pool data tables')


if __name__ == "__main__":
    # reinitialise_all_liquidity_pool_data()
    refresh_database()
