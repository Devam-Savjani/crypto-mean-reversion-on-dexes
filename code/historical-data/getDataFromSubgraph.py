import requests
import json
from graphql_client import GraphqlClient



# x = requests.get('https://api.coingecko.com/api/v3/exchanges/uniswap_v3')


# # print(json.dumps(x.json(), indent=4))



gq_client = GraphqlClient(
        endpoint= 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
        headers={}
    )

result = gq_client.execute(
    query="""
        query ($id: ID!) {
            pool (id : $id) {
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
    variables={"id" : "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"})

print(json.dumps(json.loads(result), indent=4))

print(len(json.loads(result)['data']['pool']['poolHourData']))

f = open("demofile2.txt", "w")
f.write(json.dumps(json.loads(result), indent=4))
f.close()


