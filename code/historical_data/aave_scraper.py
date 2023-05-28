from graphql_client import GraphqlClient
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
from database_interactions import table_to_df, drop_table, create_table, insert_rows, drop_all_tables_except_table


def get_block_data(symbol, gq_client, prev_max_time=0):
    rows_set = None
    try:
        rows_set = {}
        data_length = 1000
        while data_length >= 1000:
            result = gq_client.execute(
                query="""
                    query ($symbol: String!, $prev_max_time: Int!) {
                        reserves(where: {symbol: $symbol}) {
                            id
                            symbol
                            lifetimeBorrows
                            baseLTVasCollateral
                            reserveLiquidationThreshold
                            borrowHistory(
                                where: {timestamp_gt: $prev_max_time}, first: 1000, orderBy: timestamp, orderDirection: asc) {
                                id
                                timestamp
                                borrowRate
                            }
                        }
                    }
                """,
                operation_name='foo',
                variables={"symbol": symbol, "prev_max_time": int(prev_max_time)})

            reserveData = json.loads(result)
            if 'data' in reserveData:
                reserveData = reserveData['data']['reserves'][0]['borrowHistory']
            else:
                raise Exception(
                    f'Error fetching reserve data: {str(reserveData)}')

            rows_set.update({hourData['id']: tuple([hourData['id'], hourData['timestamp'], ((1 + ((int(hourData['borrowRate']) * 1e-27) / 31536000))
                            ** 31536000) - 1] + [int(json.loads(result)['data']['reserves'][0]['baseLTVasCollateral']) * 1e-4, int(json.loads(result)['data']['reserves'][0]['reserveLiquidationThreshold']) * 1e-4]) for hourData in reserveData})

            data_length = len(reserveData)
            prev_max_time = reserveData[-1]['timestamp'] if data_length > 0 else prev_max_time

    except Exception as e:
        print(f'ERROR: {symbol} {e}')

    finally:
        return list(rows_set.values()) if rows_set is not None else []


def reinitialise_borrowing_rates_data():
    tokens_supported_by_aave = ['DAI', 'EURS', 'USDC', 'USDT', 'AAVE', 'LINK', 'WBTC']

    filter_tokens = ' OR \n\t'.join([f"token0 = '{token}' OR token1 = '{token}'" for token in tokens_supported_by_aave])

    query = f"""
        SELECT pool_address, token0, token1
        FROM liquidity_pools WHERE
        (token0='WETH' or token1='WETH') AND
        \t({filter_tokens}) AND volume_usd >= 1000000
        ORDER BY volume_usd DESC;
    """

    df = table_to_df(command=query)
    
    tokens = np.unique(pd.concat([df['token0'], df['token1']]))

    gq_client_aave_v2 = GraphqlClient(
        endpoint='https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
        headers={}
    )

    gq_client_aave_v3 = GraphqlClient(
        endpoint='https://api.thegraph.com/subgraphs/name/aave/protocol-v3',
        headers={}
    )

    for token in tqdm(tokens):
        table_name = f'{token}_borrowing_rates'
        rows_v2 = get_block_data(token, gq_client=gq_client_aave_v2)
        rows_v3 = get_block_data(token, gq_client=gq_client_aave_v3)
        rows = rows_v2 + rows_v3

        if len(rows) > 0:
            drop_table(table_name)
            create_table(table_name, [('id', 'VARCHAR(255)'), ('timestamp',
                         'BIGINT'), ('borrow_rate', 'NUMERIC'), ('LTV', 'NUMERIC'), ('liquidation_threshold', 'NUMERIC')])
            insert_rows(table_name, rows)


def refresh_database():
    tokens_supported_by_aave = ['DAI', 'EURS', 'USDC', 'USDT', 'AAVE', 'LINK', 'WBTC']

    filter_tokens = ' OR \n\t'.join([f"token0 = '{token}' OR token1 = '{token}'" for token in tokens_supported_by_aave])

    query = f"""
        SELECT pool_address, token0, token1
        FROM liquidity_pools WHERE
        (token0='WETH' or token1='WETH') AND
        \t({filter_tokens}) AND volume_usd >= 1000000
        ORDER BY volume_usd DESC;
    """

    df = table_to_df(command=query)
    print(df)

    tokens = np.unique(pd.concat([df['token0'], df['token1']]))

    gq_client_aave_v3 = GraphqlClient(
        endpoint='https://api.thegraph.com/subgraphs/name/aave/protocol-v3',
        headers={}
    )

    tables = list(table_to_df(
        command=f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'liquidity_pools'")['tablename'])

    for token in tqdm(tokens):
        token_lower = token.lower()
        table_name = f'{token_lower}_borrowing_rates'

        if table_name in tables:
            df = table_to_df(
                command=f'SELECT max(id) as max_id, max(timestamp) as max_timestamp FROM {table_name};')
            rows = get_block_data(token, gq_client=gq_client_aave_v3,
                                  prev_max_time=df['max_timestamp'].iloc[0])
            if len(rows) > 0:
                insert_rows(table_name, rows)
        else:
            table_name = f'{token}_borrowing_rates'
            rows_v3 = get_block_data(token, gq_client=gq_client_aave_v3)

            if len(rows_v3) > 0:
                drop_table(table_name)
                create_table(table_name, [('id', 'VARCHAR(255)'), ('timestamp',
                                                                   'BIGINT'), ('borrow_rate', 'NUMERIC'), ('LTV', 'NUMERIC')])
                insert_rows(table_name, rows_v3)


if __name__ == "__main__":
    reinitialise_borrowing_rates_data()
    # refresh_database()
