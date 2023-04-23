import numpy as np
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from database_interactions import table_to_df
from check_liquidity_pool_data import get_pools_max_timestamp

def calculate_pairs_sum_of_squared_differences(should_save=True):
    liquidity_pool_pair_ssds = {}
    valid_liquidity_pools = get_pools_max_timestamp()['table_name']

    for i in tqdm(range(len(valid_liquidity_pools))):
        for j in range(i+1, len(valid_liquidity_pools)):
    
            merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price as p1_token1_price, p2.id as p2_id, p2.token1_price as p2_token1_price
                FROM "{valid_liquidity_pools[i]}" as p1 INNER JOIN "{valid_liquidity_pools[j]}" as p2
                ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price <> 0 AND p2.token1_price <> 0;
                """)

            ssd_1 = np.sum((merged['p1_token1_price'].to_numpy() - merged['p2_token1_price'].to_numpy())**2)
            ssd_2 = np.sum((merged['p1_token1_price'].to_numpy() - (1 / merged['p2_token1_price']).to_numpy())**2)
            
            if ssd_1 < ssd_2:
                key = (valid_liquidity_pools[i], valid_liquidity_pools[j])
            else:
                key = (valid_liquidity_pools[i], f'{valid_liquidity_pools[j]}_swapped')

            liquidity_pool_pair_ssds[key] = min(ssd_1, ssd_2)

    liquidity_pool_pair_ssds = sorted(liquidity_pool_pair_ssds.items(), key=lambda x:x[1])

    if should_save:
        return save_pairs_sum_of_squared_differences(liquidity_pool_pair_ssds)
    
    return liquidity_pool_pair_ssds

def is_cointegrated(p1, p2):
    swapped = False
    p2_split = p2.split('_')
    if len(p2_split) == 4:
        swapped = True
        p2 = p2_split[0] + '_' + p2_split[1] + '_' + p2_split[2]

    merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price as p1_token1_price, p2.id as p2_id, p2.token1_price as p2_token1_price
                FROM "{p1}" as p1 INNER JOIN "{p2}" as p2
                ON p1.period_start_unix = p2.period_start_unix {'WHERE p2.token1_price <> 0' if swapped else ''};
                """)
    
    df1 = merged['p1_token1_price']
    df2 = merged['p2_token1_price']

    if swapped:
        df2 = 1 / merged['p2_token1_price']

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

def get_top_n_cointegrated_pairs(ssds, n=-1, should_save=False):
    cointegrated_pairs = []
    n = n if n != -1 else len(ssds)
    for pair in tqdm(ssds):
        p1, p2 = pair[0]
        is_cointegrated = is_cointegrated(p1, p2)
        if is_cointegrated:
            cointegrated_pairs.append(pair[0])
            if len(cointegrated_pairs) == n:
                if should_save:
                    return save_cointegrated_pairs(cointegrated_pairs)
                else:
                    return cointegrated_pairs

    if should_save:
        return save_cointegrated_pairs(cointegrated_pairs)
    else:
        return cointegrated_pairs

def save_pairs_sum_of_squared_differences(ssds):
    with open('historical_data/sum_square_differences.pickle', 'wb') as f:
        pickle.dump(ssds, f)

    f.close()
    return ssds

def load_pairs_sum_of_squared_differences():
    with open('sum_square_differences.pickle', 'rb') as f:
        liquidity_pool_pair_ssds = pickle.load(f)
        f.close()

    return liquidity_pool_pair_ssds

def save_cointegrated_pairs(cointegrated_pairs):
    with open('historical_data/cointegrated_pairs.pickle', 'wb') as f:
        pickle.dump(cointegrated_pairs, f)

    f.close()
    return cointegrated_pairs

def load_cointegrated_pairs(path='historical_data/cointegrated_pairs.pickle'):
    with open(path, 'rb') as f:
        cointegrated_pairs = pickle.load(f)
        f.close()

    return cointegrated_pairs

if __name__ == "__main__":
    use_pickled_cointegrated_pairs = False
    should_save = True

    if use_pickled_cointegrated_pairs:
        cointegrated_pairs = load_cointegrated_pairs()
    else:
        ssds = calculate_pairs_sum_of_squared_differences(should_save=should_save)
        cointegrated_pairs = get_top_n_cointegrated_pairs(ssds=ssds, should_save=should_save)

    print(*cointegrated_pairs, sep="\n")
    print(len(cointegrated_pairs))
