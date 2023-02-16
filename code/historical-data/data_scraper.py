import csv
from graphql_client import GraphqlClient
import json
import os
import pandas as pd


header = ['id', 'periodStartUnix', 'token0Price',
          'token1Price', 'liquidity', 'feesUSD']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', #https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql
    headers={}
)

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
                pool(
                    id: $id,
                ) {
                    poolHourData {
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
                            where: {periodStartUnix_gt: $prev_max_time}
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
df = df.loc[df['volumeUSD'] >= 10**5 ].reset_index()

for index, row in df.iterrows():
    path = generate_file_path(row['token0'], row['token1'])
    print(path)
    get_block_data(row['id'], path)

