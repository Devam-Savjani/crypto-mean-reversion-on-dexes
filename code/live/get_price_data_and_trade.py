import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
import time
import json
import pickle
from live.live_trade_execution import execute_signal
from utils.database_interactions import insert_rows, table_to_df
from utils.graphql_client import GraphqlClient

TOKEN_ADDRESSES = {
    'WETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    'DAI': '0x6B175474E89094C44Da98b954EedeAC495271d0F',
    'USDC': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7'
}


def get_pools_to_get_data_for(pairs):
    return list(set(sum([[pair[0].split('_')[2], pair[1].split('_')[2]] for pair in pairs], [])))


def get_pool_prices(pool1, pool2):
    gq_client = GraphqlClient(
        endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
        headers={}
    )

    pool_prices = {}

    for pool_id in [pool1, pool2]:
        result = gq_client.execute(
            query="""
                query ($id: ID!) {
                    pool(id: $id) {
                        token0 {
                            symbol
                        }
                        token1 {
                            symbol
                        }
                        token0Price
                        token1Price
                        liquidity
                    }
                }
            """,
            operation_name='foo',
            variables={"id": pool_id})

        poolData = json.loads(result)

        if 'data' in poolData:
            poolData = poolData['data']['pool']
        else:
            raise Exception(f'Error fetching pool data: {str(poolData)}')

        pool_prices[f"{poolData['token0']['symbol']}_{poolData['token1']['symbol']}_{pool_id}"] = (
            time.time(), poolData['token0Price'], poolData['token1Price'], poolData['liquidity'])

    return tuple(pool_prices.values())


def update_pool_table(pool_prices):
    for table_name, data in pool_prices.items():
        rows = [(f'{table_name}-{data[0]}', int(data[0]),
                 data[1], data[2], data[3])]
        # insert_rows(f'"{table_name}"', rows)


def get_gas_price():
    gq_client = GraphqlClient(
        endpoint='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
        headers={}
    )

    result = gq_client.execute(
        query="""
            query {
                transactions(first:1, orderBy: timestamp, orderDirection:desc) {
                    gasPrice
                    timestamp
                }
            }
        """,
        operation_name='foo',
        variables={})

    transactionData = json.loads(result)

    if 'data' in transactionData:
        transactionData = transactionData['data']['transactions']
    else:
        raise Exception(f'Error fetching gas data: {str(transactionData)}')

    return (transactionData['timestamp'], transactionData['gasPrice'])


def update_gas_price_table(row):
    insert_rows(f'gas_prices', [row])


def get_uniswap_fees(pool1, pool2):
    pools = table_to_df(command=f"""
                        SELECT * FROM liquidity_pools where pool_address='{pool1}' or pool_address='{pool2}';
                        """, path_to_config='../utils/database.ini')
    return {
        'T1': pools[pools['pool_address'] == pool1]['fee_tier'].iloc[0] * 1e-6,
        'T2': pools[pools['pool_address'] == pool2]['fee_tier'].iloc[0] * 1e-6
    }

with open('state.pickle', 'rb') as f:
    state = pickle.load(f)
    f.close()

latest_pool1_data, latest_pool2_data = get_pool_prices(
    state['pair'][0].split('_')[2], state['pair'][1].split('_')[2])
update_pool_table({state['pair'][0]: latest_pool1_data,
                  state['pair'][1]: latest_pool2_data})

prices = {
    'P1': latest_pool1_data[1] if state['pair'][0].split('_')[0] == 'WETH' else latest_pool1_data[2],
    'P2': latest_pool2_data[1] if state['pair'][1].split('_')[0] == 'WETH' else latest_pool2_data[2],
}

timestamp, gas_price_in_wei = get_gas_price()

# Now generate signal and execute ....
signal = state['strategy'].generate_signal(
    {
        'open_positions': state['open_positions'],
        'account': state['account'],
        'gas_price_in_eth': int(gas_price_in_wei) * 1e-18,
        'timestamp': int(time.time()),
        'ltv_eth': 0.825,
        'liquidation_threshold': 0.86,
        'uniswap_fees': get_uniswap_fees()
    }, prices)


new_account, new_open_positions = state['account'], state['open_positions']

if len(signal) > 0:
    token0_addr = TOKEN_ADDRESSES[state['pair'][0].split("_")[0] if state['pair'][0].split(
        "_")[1] == 'WETH' else state['pair'][0].split("_")[1]]
    token1_addr = TOKEN_ADDRESSES[state['pair'][1].split("_")[0] if state['pair'][1].split(
        "_")[1] == 'WETH' else state['pair'][1].split("_")[1]]
    new_account, new_open_positions = execute_signal(
        state['pair'], token0_addr, token0_addr, signal, state['account'], state['open_positions'], 0.825)
    
with open('state.pickle', 'wb') as f:
    new_state = {
        'pair': state['pair'],
        'strategy': state['strategy'],
        'open_positions': new_open_positions,
        'account': new_account
    }
    pickle.dump(new_state, f)
f.close()
