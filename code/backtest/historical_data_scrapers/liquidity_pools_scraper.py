import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(os.path.realpath(current))
sys.path.append(os.path.dirname(parent))
import json
from utils.graphql_client import GraphqlClient
from utils.database_interactions import drop_table, create_table, insert_rows

gq_client = GraphqlClient(
    endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
    headers={}
)

def get_block_data():
    drop_table('liquidity_pools')
    create_table('liquidity_pools', [('pool_address', 'VARCHAR(255)'), ('token0', 'VARCHAR(255)'), ('token0_address', 'VARCHAR(255)'), ('token1', 'VARCHAR(255)'), ('token1_address', 'VARCHAR(255)'), ('volume_USD', 'NUMERIC(80,60)'), ('created_At_Timestamp', 'BIGINT'), ('fee_tier', 'BIGINT')])

    rows_set = {}

    result = gq_client.execute(
        query="""
            query pools {
                pools(orderBy: createdAtTimestamp, first: 1000, orderDirection: asc) {
                    id
                    token0 {
                        id
                        symbol
                        decimals
                    }
                    token1 {
                        id
                        symbol
                        decimals
                    }
                    volumeUSD
                    createdAtTimestamp
                    feeTier
                }
            }
        """,
        operation_name='foo',
        variables={})
    
    pools = json.loads(result)['data']['pools']
    rows_set.update({pool['id'] : (pool['id'], pool['token0']['symbol'], pool['token0']['id'], pool['token1']['symbol'], pool['token1']['id'], pool['volumeUSD'], pool['createdAtTimestamp'], pool['feeTier']) for pool in pools})

    while len(pools) >= 1000:
        prev_max_time = pools[-1]['createdAtTimestamp']

        result = gq_client.execute(
            query="""
                query pools($prev_max_time: BigInt!) {
                    pools(orderBy: createdAtTimestamp, where: {createdAtTimestamp_gte: $prev_max_time}, first: 1000, orderDirection: asc) {
                        id
                        token0 {
                            id
                            symbol
                            decimals
                        }
                        token1 {
                            id
                            symbol
                            decimals
                        }
                        volumeUSD
                        createdAtTimestamp
                        feeTier
                    }
                }
            """,
            operation_name='foo',
            variables={"prev_max_time": prev_max_time})
        
        try:
            pools = json.loads(result)['data']['pools']
            rows_set.update({pool['id'] : (pool['id'], pool['token0']['symbol'], pool['token0']['id'], pool['token1']['symbol'], pool['token1']['id'], pool['volumeUSD'], pool['createdAtTimestamp'], pool['feeTier']) for pool in pools})

        except Exception as e:
            print(e)
            print(result)
    
    insert_rows('liquidity_pools', list(rows_set.values()))
    print(f'Inserted {len(rows_set.values())} Rows')

if __name__ == "__main__":
    get_block_data()
