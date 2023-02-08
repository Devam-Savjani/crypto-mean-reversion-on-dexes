import json
from graphql_client import GraphqlClient

gq_client = GraphqlClient(
        endpoint= 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
        headers={}
    )

result = gq_client.execute(
    query="""
        query pools {
            pools {
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

print(json.dumps(json.loads(result), indent=4))

print(len(json.loads(result)['data']['pools']))

f = open("demofile2.txt", "w")
f.write(json.dumps(json.loads(result), indent=4))
f.close()



import csv
from graphql_client import GraphqlClient
import json

header = ['id', 'token0', 'token1',
          'volumeUSD', 'createdAtTimestamp']

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

    result = gq_client.execute(
        query="""
            query pools {
                pools {
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

    while len(hourlyData) > 0:
        rows = [[hourData['id'], hourData['token0']['symbol'], hourData['token1']['symbol'], hourData['volumeUSD'], hourData['createdAtTimestamp']] for hourData in hourlyData]
        writer.writerows(rows)

        prev_max_time = hourlyData[-1]['createdAtTimestamp']
        print(prev_max_time)

        result = gq_client.execute(
            query="""
                query pools($prev_max_time: BigInt!) {
                    pools(where: {createdAtTimestamp_gt: $prev_max_time}) {
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

    # close the file
    f.close()


get_block_data('foo1.csv')
