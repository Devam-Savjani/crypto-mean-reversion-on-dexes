import csv
from graphql_client import GraphqlClient
import json
import os
import pandas as pd
import logging


header = ['id', 'periodStartUnix', 'token0Price',
          'token1Price', 'liquidity', 'feesUSD']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', #https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql
    headers={}
)

logging.basicConfig(filename='data_scraper.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Running Urban Planning")

logger = logging.getLogger('urbanGUI')

# https://thegraph.com/docs/en/querying/graphql-api/
# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3

def get_block_data(pool_address, file_name):
    # open the file in the write mode
    f = open(file_name, 'w')

    # create the csv writer
    writer = csv.writer(f)

    # write a row to the csv file
    writer.writerow(header)

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

    while len(hourlyData) > 0:
        rows = [[hourData[key] for key in header] for hourData in hourlyData]
        writer.writerows(rows)
        # print(hourlyData)

        prev_max_time = hourlyData[-1]['periodStartUnix']
        # print(prev_max_time)

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

        # print(result)
        if json.loads(result)['data']:
            hourlyData = json.loads(result)['data']['pool']['poolHourData']
        else:
            logging.error('Error fetching in ' + file_name)
            hourlyData = []

    # close the file
    f.close()

def generate_file_path(token0, token1):
    basepath = f"liquidity_pool_data/{token0}_{token1}_pricing_data"
    path = basepath
    if os.path.isfile(f"{path}.csv"):
        path += '1'
    
    count = 2
    while os.path.isfile(f"{path}.csv"):
        path = basepath + str(count)
        count += 1

    return f"{path}.csv"

def clear_dir(dir):
    for f in os.listdir(dir):
        os.remove(os.path.join(dir, f))



clear_dir('liquidity_pool_data')
df = pd.read_csv('liquidity_pools.csv')
df = df.loc[df['volumeUSD'] >= 10**6 ].reset_index()

print(df.shape)

for index, row in df.iterrows():
    path = generate_file_path(row['token0'], row['token1'])
    print(path)
    get_block_data(row['id'], path)

