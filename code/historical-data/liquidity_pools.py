import csv
from graphql_client import GraphqlClient
import json

header = ['id', 'token0', 'token1', 'volumeUSD', 'createdAtTimestamp']

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', #https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql
    headers={}
)

# query pools {
#                 pools(orderBy: volumeUSD, where: {id : "0x5777d92f208679db4b9778590fa3cab3ac9e2168"}) {
#                     id
#                     token0 {
#                         symbol
#                         decimals
#                     }
#                     token1 {
#                         symbol
#                         decimals
#                     }
#                     volumeUSD
#                     createdAtTimestamp
#                 }
#             }


# https://thegraph.com/docs/en/querying/graphql-api/
# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3

def get_block_data(file_name):
    # open the file in the write mode
    f = open(file_name, 'w')

    # create the csv writer
    writer = csv.writer(f)

    # write a row to the csv file
    writer.writerow(header)

    result = gq_client.execute(
        query="""
            query pools {
                pools(orderBy: createdAtTimestamp, first: 1000) {
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
    
    hourlyData = json.loads(result)['data']['pools']

    while len(hourlyData) >= 1000:
        rows = [[hourData['id'], hourData['token0']['symbol'], hourData['token1']['symbol'], hourData['volumeUSD'], hourData['createdAtTimestamp']] for hourData in hourlyData]
        writer.writerows(rows)

        prev_max_time = hourlyData[-1]['createdAtTimestamp']

        result = gq_client.execute(
            query="""
                query pools($prev_max_time: BigInt!) {
                    pools(orderBy: createdAtTimestamp, where: {createdAtTimestamp_gte: $prev_max_time}, first: 1000) {
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
            variables={"prev_max_time": prev_max_time})

        hourlyData = json.loads(result)['data']['pools']

    f.close()


get_block_data('liquidity_pools.csv')
