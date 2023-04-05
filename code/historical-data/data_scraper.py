import csv
from graphql_client import GraphqlClient
import json
import os
import pandas as pd
import logging
from tqdm import tqdm


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

def get_block_data(pool_address, file_name):    
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
        
        # open the file in the write mode
        f = open(file_name, 'w')

        # create the csv writer
        writer = csv.writer(f)

        # write a row to the csv file
        writer.writerow(header + ['token1PriceRatio'])

        hourlyData = json.loads(result)['data']['pool']['poolHourData']

        rows = [[hourData[key] for key in header] + [float(hourData['token1Price']) / float(hourData['token0Price']) if float(hourData["token0Price"]) > 0 else 0] for hourData in hourlyData]
        writer.writerows(rows)
        
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
            rows = [[hourData[key] for key in header] + [float(hourData['token1Price']) / float(hourData['token0Price']) if float(hourData["token0Price"]) > 0 else 0] for hourData in hourlyData]
            writer.writerows(rows)

    except Exception as e:
        print('ERROR: ' +  file_name)
        print(e)
        logger.error('Failed to fetch some data for address ' + pool_address + ' : ' +  file_name)
    finally:
        # close the file
        f.close()

def clear_dir(dir):
    for f in os.listdir(dir):
        os.remove(os.path.join(dir, f))

clear_dir('liquidity_pool_data')
df = pd.read_csv('liquidity_pools.csv')
df = df.loc[df['volumeUSD'] >= 10**8 ].reset_index()

print(df.shape)

for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    path = f"liquidity_pool_data/{row['token0']}_{row['token1']}_{row['id']}.csv"
    # print(path)
    get_block_data(row['id'], path)
