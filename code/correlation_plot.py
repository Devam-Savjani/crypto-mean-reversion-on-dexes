import sys
sys.path.append('./historical_data')
from database_interactions import table_to_df
import seaborn as sn
import matplotlib.pyplot as plt

liquidity_pools = []

tokens_supported_by_aave = ['DAI', 'EURS', 'USDC', 'USDT', 'AAVE', 'LINK', 'WBTC']
filter_tokens = ' OR \n\t'.join([f"token0 = '{token}' OR token1 = '{token}'" for token in tokens_supported_by_aave])

query = f"""
    SELECT pool_address, token0, token1
    FROM liquidity_pools WHERE
    (token0='WETH' or token1='WETH') AND
    \t({filter_tokens}) AND volume_usd >= 10000000
    ORDER BY volume_usd DESC;
"""

df = table_to_df(command=query, path_to_config='historical_data/database.ini')

for index, row in df.iterrows():
    liquidity_pools.append('"' + f"{row['token0']}_{row['token1']}_{row['pool_address']}" + '"')

columns = ',\n\t'.join([f'p{i}.{"token1_price" if liquidity_pools[i][1:5] != "WETH" else "token0_price"} as {liquidity_pools[i][1:-1][:15]}' for i in range(1, len(liquidity_pools))])
joins = '\n\t'.join([f'INNER JOIN {liquidity_pools[i]} as p{i} ON p1.period_start_unix = p{i}.period_start_unix' for i in range(2, len(liquidity_pools))])
where = ' AND \n\t'.join([f'p{i}.token1_price <> 0' for i in range(1, len(liquidity_pools))])

query = f"""
    SELECT \n\t{columns}
    FROM {liquidity_pools[0]} as p1
    \t{joins}
    WHERE 
    \t{where}
    ORDER BY p1.period_start_unix;
"""

corr_matrix = table_to_df(command=query, path_to_config='historical_data/database.ini').corr()

sn.heatmap(corr_matrix, annot = True)
plt.show() 
