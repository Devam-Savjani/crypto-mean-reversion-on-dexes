import pandas as pd
import numpy as np
from database_interactions import table_to_df
from check_liquidity_pool_data import get_pools_max_timestamp
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

def calculate_pairs_sum_of_squared_differences():
    liquidity_pool_pair_ssds = {}
    valid_liquidity_pools = get_pools_max_timestamp()['table_name']

    for i in tqdm(range(len(valid_liquidity_pools))):
        for j in range(i+1, len(valid_liquidity_pools)):
    
            merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price_per_token0 as p1_token1_price_per_token0, p2.id as p2_id, p2.token1_price_per_token0 as p2_token1_price_per_token0
                FROM "{valid_liquidity_pools[i]}" as p1 INNER JOIN "{valid_liquidity_pools[j]}" as p2
                ON p1.period_start_unix = p2.period_start_unix;
                """)

            ssd = np.sum((merged['p1_token1_price_per_token0'].to_numpy() - merged['p2_token1_price_per_token0'].to_numpy())**2)

            key = (valid_liquidity_pools[i], valid_liquidity_pools[j])
            liquidity_pool_pair_ssds[key] = ssd

    liquidity_pool_pair_ssds = sorted(liquidity_pool_pair_ssds.items(), key=lambda x:x[1])

    return save_pairs_sum_of_squared_differences(liquidity_pool_pair_ssds)

def get_is_cointegrated_and_hedge_ratio(p1, p2):

    merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price_per_token0 as p1_token1_price_per_token0, p2.id as p2_id, p2.token1_price_per_token0 as p2_token1_price_per_token0
                FROM "{p1}" as p1 INNER JOIN "{p2}" as p2
                ON p1.period_start_unix = p2.period_start_unix;
                """)
    
    df1 = merged['p1_token1_price_per_token0']
    df2 = merged['p2_token1_price_per_token0']

    # Step 1: Test for unit roots
    adf1 = adfuller(df1)
    adf2 = adfuller(df2)

    if adf1[0] < adf1[4]['5%'] and adf2[0] < adf2[4]['5%']:
        # Both variables are stationary
        return False, None
    else:
        # At least one variable is non-stationary, proceed to step 2

        # Step 2: Test for cointegration
        eg_test = sm.OLS(df1, sm.add_constant(df2)).fit()
        resid = eg_test.resid

        # Test the residuals for stationarity
        resid_adf = adfuller(resid)
        if resid_adf[0] < resid_adf[4]['5%']:
            # The variables are cointegrated
            hedge_ratio = eg_test.params[1]
            return True, hedge_ratio
        else:
            # The variables are not cointegrated
            return False, None

def get_top_n_cointegrated_pairs(ssds, n=-1):
    cointegrated_pairs = []
    n = n if n != -1 else len(ssds)
    for pair in tqdm(ssds):
        p1, p2 = pair[0]
        is_cointegrated, hedge_rato = get_is_cointegrated_and_hedge_ratio(p1, p2)
        if is_cointegrated:
            cointegrated_pairs.append((pair[0], hedge_rato))
            if len(cointegrated_pairs) == n:
                return cointegrated_pairs

    return save_cointegrated_pairs(cointegrated_pairs)

def save_pairs_sum_of_squared_differences(ssds):
    with open('sum_square_differences.pickle', 'wb') as f:
        pickle.dump(ssds, f)

    f.close()
    return ssds

def load_pairs_sum_of_squared_differences():
    with open('sum_square_differences.pickle', 'rb') as f:
        liquidity_pool_pair_ssds = pickle.load(f)
        f.close()

    return liquidity_pool_pair_ssds

def save_cointegrated_pairs(cointegrated_pairs):
    with open('cointegrated_pairs.pickle', 'wb') as f:
        pickle.dump(cointegrated_pairs, f)

    f.close()
    return cointegrated_pairs

def load_cointegrated_pairs():
    with open('cointegrated_pairs.pickle', 'rb') as f:
        cointegrated_pairs = pickle.load(f)
        f.close()

    return cointegrated_pairs

use_pickled_cointegrated_pairs = False

if use_pickled_cointegrated_pairs:
    cointegrated_pairs = load_cointegrated_pairs()
else:
    ssds = calculate_pairs_sum_of_squared_differences()
    cointegrated_pairs = get_top_n_cointegrated_pairs(ssds=ssds)

print(*cointegrated_pairs, sep="\n")
print(len(cointegrated_pairs))
