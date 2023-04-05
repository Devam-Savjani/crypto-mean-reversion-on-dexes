import csv
from graphql_client import GraphqlClient
import json

header = ['id', 'token0', 'token1', 'volumeUSD', 'createdAtTimestamp']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', #https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql
    headers={}
)

# https://thegraph.com/docs/en/querying/graphql-api/
# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3

def get_block_data(file_name):
    # open the file in the write mode
    f = open(file_name, 'w')

    # create the csv writer
    writer = csv.writer(f)

    # write a row to the csv file
    writer.writerow(header)

    max_start_time = 1630450800 # 2021-08-31 00:00:00

    result = gq_client.execute(
        query="""
            query pools {
                pools(orderBy: createdAtTimestamp, first: 1000, orderDirection: asc) {
                    id
                    token0 {
                        symbol
                        decimals
                    }
                    token1 {
                        symbol
                        decimals
                    }
                    volumeUSD
                    createdAtTimestamp
                }
            }
        """,
        operation_name='foo',
        variables={})
    
    pools = json.loads(result)['data']['pools']
    rows = [[pool['id'], pool['token0']['symbol'], pool['token1']['symbol'], pool['volumeUSD'], pool['createdAtTimestamp']] for pool in pools]
    writer.writerows(rows)

    while len(pools) >= 1000:
        prev_max_time = pools[-1]['createdAtTimestamp']

        result = gq_client.execute(
            query="""
                query pools($prev_max_time: BigInt!, $max_start_time: BigInt!) {
                    pools(orderBy: createdAtTimestamp, where: {createdAtTimestamp_gte: $prev_max_time, createdAtTimestamp_lt: $max_start_time}, first: 1000, orderDirection: asc) {
                        id
                        token0 {
                            symbol
                            decimals
                        }
                        token1 {
                            symbol
                            decimals
                        }
                        volumeUSD
                        createdAtTimestamp
                    }
                }
            """,
            operation_name='foo',
            variables={"prev_max_time": prev_max_time, "max_start_time": max_start_time})
        
        try:
            pools = json.loads(result)['data']['pools']
            rows = [[pool['id'], pool['token0']['symbol'], pool['token1']['symbol'], pool['volumeUSD'], pool['createdAtTimestamp']] for pool in pools]
            writer.writerows(rows)
        except Exception as e:
            print(e)
            print(result)
            break

    f.close()


get_block_data('liquidity_pools.csv')