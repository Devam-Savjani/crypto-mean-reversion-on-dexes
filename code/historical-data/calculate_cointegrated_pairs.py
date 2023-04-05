import pandas as pd
import numpy as np
from check_liquidity_pool_data import get_pools_with_data_till_today
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

liquidity_pool_data_directory = 'liquidity_pool_data'

def calculate_pairs_sum_of_squared_differences():
    liquidity_pool_pair_ssds = {}
    valid_liquidity_pools = get_pools_with_data_till_today()

    for i in tqdm(range(len(valid_liquidity_pools))):
        for j in range(i+1, len(valid_liquidity_pools)):

            datapair1 = pd.read_csv(valid_liquidity_pools[i])
            datapair1 = datapair1.drop(['liquidity', 'feesUSD'], axis=1)

            datapair2 = pd.read_csv(valid_liquidity_pools[j])
            datapair2 = datapair2.drop(['liquidity', 'feesUSD'], axis=1)

            merged = pd.merge(datapair1, datapair2, on='periodStartUnix')
            ssd = np.sum((merged['token1PriceRatio_x'].to_numpy() - merged['token1PriceRatio_y'].to_numpy())**2)

            key = valid_liquidity_pools[i].split('/')[1][:-4] + '/' + valid_liquidity_pools[j].split('/')[1][:-4]
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

def is_cointegrated(liquidity_pair):
    pair = liquidity_pair.split('/')
    pair0 = pair[0]
    pair1 = pair[1]

    pair0_df = pd.read_csv(f"{liquidity_pool_data_directory}/{pair0}.csv")
    pair1_df = pd.read_csv(f"{liquidity_pool_data_directory}/{pair1}.csv")
    
    merged = pd.merge(pair0_df, pair1_df, on='periodStartUnix')

    df1 = merged['token1PriceRatio_x']
    df2 = merged['token1PriceRatio_y']

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
            return True
        else:
            # The variables are not cointegrated
            return False

def get_top_n_cointegrated_pairs(ssds, n):
    cointegrated_pairs = []
    for pair in tqdm(ssds):
        liquidity_pair_str, _ = pair
        if is_cointegrated(liquidity_pair_str):
            print('Found One')
            cointegrated_pairs.append(pair)
            if len(cointegrated_pairs) == n:
                return cointegrated_pairs
            
    return cointegrated_pairs

# calculate_pairs_sum_of_squared_differences()
ssds = load_pairs_sum_of_squared_differences()
foo = get_top_n_cointegrated_pairs(ssds, 2)
print(*foo, sep="\n")

