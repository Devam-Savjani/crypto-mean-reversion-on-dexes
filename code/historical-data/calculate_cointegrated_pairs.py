import pandas as pd
import numpy as np
from database_interactions import table_to_df
from check_liquidity_pool_data import get_pools_max_timestamp
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

liquidity_pool_data_directory = 'liquidity_pool_data'

def calculate_pairs_sum_of_squared_differences():
    liquidity_pool_pair_ssds = {}
    valid_liquidity_pools = get_pools_max_timestamp()['table_name']

    for i in tqdm(range(len(valid_liquidity_pools))):
        for j in range(i+1, len(valid_liquidity_pools)):
    
            merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price_ratio as p1_token1_price_ratio, p2.id as p2_id, p2.token1_price_ratio as p2_token1_price_ratio
                FROM "{valid_liquidity_pools[i]}" as p1 INNER JOIN "{valid_liquidity_pools[j]}" as p2
                ON p1.period_start_unix = p2.period_start_unix;
                """)

            ssd = np.sum((merged['p1_token1_price_ratio'].to_numpy() - merged['p2_token1_price_ratio'].to_numpy())**2)

            key = (valid_liquidity_pools[i], valid_liquidity_pools[j])
            liquidity_pool_pair_ssds[key] = ssd

    liquidity_pool_pair_ssds = sorted(liquidity_pool_pair_ssds.items(), key=lambda x:x[1])

    with open('cointegrated_pairs_ssd.pickle', 'wb') as f:
        pickle.dump(liquidity_pool_pair_ssds, f)

    f.close()

def load_pairs_sum_of_squared_differences():
    with open('cointegrated_pairs_ssd.pickle', 'rb') as f:
        liquidity_pool_pair_ssds = pickle.load(f)
        f.close()

    return liquidity_pool_pair_ssds

def is_cointegrated(p1, p2):

    merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price_ratio as p1_token1_price_ratio, p2.id as p2_id, p2.token1_price_ratio as p2_token1_price_ratio
                FROM "{p1}" as p1 INNER JOIN "{p2}" as p2
                ON p1.period_start_unix = p2.period_start_unix;
                """)
    
    df1 = merged['p1_token1_price_ratio']
    df2 = merged['p2_token1_price_ratio']

    # Step 1: Test for unit roots
    adf1 = adfuller(df1)
    adf2 = adfuller(df2)

    if adf1[0] < adf1[4]['5%'] and adf2[0] < adf2[4]['5%']:
        # Both variables are stationary
        return False
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
            return True
        else:
            # The variables are not cointegrated
            return False

def get_top_n_cointegrated_pairs(ssds, n):
    cointegrated_pairs = []
    for pair in tqdm(ssds):
        p1, p2 = pair[0]
        if is_cointegrated(p1, p2):
            cointegrated_pairs.append(pair)
            if len(cointegrated_pairs) == n:
                return cointegrated_pairs
            
    return cointegrated_pairs

# calculate_pairs_sum_of_squared_differences()
ssds = load_pairs_sum_of_squared_differences()
foo = get_top_n_cointegrated_pairs(ssds, 20)
print(*foo, sep="\n")

