CORRELATION_LOWER_LIMIT = 0.995
CORRELATION_UPPER_LIMIT = 0.999

GAS_USED_BY_BUYING_ETH = 61482
GAS_USED_BY_SWAP = 126108
GAS_USED_BY_DEPOSITING_COLLATERAL = 214353
GAS_USED_BY_WITHDRAWING_COLLATERAL = 181733
GAS_USED_BY_BORROW = 423777
GAS_USED_BY_REPAY = 401056

GAS_USED_BY_OPEN_BUY_AND_SELL_POSITION = 526749
GAS_USED_BY_CLOSE_BUY_AND_SELL_POSITION = 502811

VOLUME_LOWER_LIMIT = 10000000

tokens_supported_by_aave = ['DAI', 'EURS', 'USDC', 'USDT', 'AAVE', 'LINK', 'WBTC']
filter_tokens = ' OR \n\t'.join([f"token0 = '{token}' OR token1 = '{token}'" for token in tokens_supported_by_aave])

liquidity_pools_table_of_interest = f"""
    SELECT * FROM liquidity_pools WHERE
    (token0='WETH' or token1='WETH') AND
    \t({filter_tokens}) AND volume_usd >= {VOLUME_LOWER_LIMIT}
    ORDER BY volume_usd DESC
"""

LIQUIDITY_POOLS_OF_INTEREST_TABLE_QUERY = f"{liquidity_pools_table_of_interest};"

LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY = f"""
    Select CONCAT(UPPER(pool.token0), '_', UPPER(pool.token1), '_', pool.pool_address) as table_name from
    ({liquidity_pools_table_of_interest}) as pool
"""
