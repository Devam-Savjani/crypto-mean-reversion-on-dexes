import csv
from graphql_client import GraphqlClient
import json

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

        prev_max_time = hourlyData[-1]['periodStartUnix']
        print(prev_max_time)

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


get_block_data('0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'foo.csv')
