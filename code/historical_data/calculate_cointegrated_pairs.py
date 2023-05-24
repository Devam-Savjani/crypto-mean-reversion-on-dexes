import numpy as np
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from database_interactions import table_to_df
from check_liquidity_pool_data import get_pools_max_timestamp

def calculate_pairs_sum_of_squared_differences(should_save=True):
    liquidity_pool_pair_ssds = {}
    valid_pools_that_include_weth = [pool for pool in get_pools_max_timestamp()['table_name'] if 'WETH' in pool]

    for i in tqdm(range(len(valid_pools_that_include_weth))):
        for j in range(i+1, len(valid_pools_that_include_weth)):
    
            merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price as p1_token1_price, p2.id as p2_id, p2.token1_price as p2_token1_price
                FROM "{valid_pools_that_include_weth[i]}" as p1 INNER JOIN "{valid_pools_that_include_weth[j]}" as p2
                ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price <> 0 AND p2.token1_price <> 0;
                """)

            pool1_data = merged['p1_token1_price'].to_numpy() if valid_pools_that_include_weth[i].split('_')[1] == 'WETH' else (1 / merged['p1_token1_price'].to_numpy())
            pool2_data = merged['p2_token1_price'].to_numpy() if valid_pools_that_include_weth[j].split('_')[1] == 'WETH' else (1 / merged['p2_token1_price'].to_numpy())

            ssd = np.sum((pool1_data - pool2_data)**2)
            liquidity_pool_pair_ssds[(valid_pools_that_include_weth[i], valid_pools_that_include_weth[j])] = ssd

    liquidity_pool_pair_ssds = sorted(liquidity_pool_pair_ssds.items(), key=lambda x:x[1])

    if should_save:
        return save_pairs_sum_of_squared_differences(liquidity_pool_pair_ssds)
    
    return liquidity_pool_pair_ssds

def is_cointegrated(p1, p2):
    swapped_p1 = p1.split('_')[0] == 'WETH'
    swapped_p2 = p2.split('_')[0] == 'WETH'

    merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price as p1_token1_price, p2.id as p2_id, p2.token1_price as p2_token1_price
                FROM "{p1}" as p1 INNER JOIN "{p2}" as p2
                ON p1.period_start_unix = p2.period_start_unix {'WHERE p1.token1_price <> 0' if swapped_p1 else ''} {'AND p2.token1_price <> 0' if swapped_p1 and swapped_p2 else ('WHERE p2.token1_price <> 0' if swapped_p2 else '')};
                """)
    
    df1 = merged['p1_token1_price']
    df2 = merged['p2_token1_price']

    if swapped_p1:
        df1 = 1 / merged['p1_token1_price']

    if swapped_p2:
        df2 = 1 / merged['p2_token1_price']

    result = sm.tsa.stattools.coint(df1, df2)
    return result[0] < result[2][0]

def get_top_n_cointegrated_pairs(ssds, n=-1, should_save=False):
    cointegrated_pairs = []
    n = n if n != -1 else len(ssds)
    for pair in tqdm(ssds):
        p1, p2 = pair[0]
        is_pair_cointegrated = is_cointegrated(p1, p2)
        if is_pair_cointegrated:
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
