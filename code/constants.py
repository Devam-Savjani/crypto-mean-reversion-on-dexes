GAS_USED_BY_SWAP = 150000
GAS_USED_BY_LOAN = 100000
GAS_USED = (2 * GAS_USED_BY_SWAP) + GAS_USED_BY_LOAN
VOLUME_LOWER_LIMIT = 10000000

tokens_supported_by_aave = ['DAI', 'EURS', 'USDC', 'USDT', 'AAVE', 'LINK', 'WBTC']
filter_tokens = ' OR \n\t'.join([f"token0 = '{token}' OR token1 = '{token}'" for token in tokens_supported_by_aave])

liquidity_pools_table_of_interest = f"""
    SELECT pool_address, token0, token1
    FROM liquidity_pools WHERE
    (token0='WETH' or token1='WETH') AND
    \t({filter_tokens}) AND volume_usd >= {VOLUME_LOWER_LIMIT}
    ORDER BY volume_usd DESC
"""

LIQUIDITY_POOLS_OF_INTEREST_TABLE_QUERY = f"{liquidity_pools_table_of_interest};"

LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY = f"""
    Select CONCAT(UPPER(pool.token0), '_', UPPER(pool.token1), '_', pool.pool_address) as table_name from
    ({liquidity_pools_table_of_interest}) as pool
"""
