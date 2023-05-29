import numpy as np
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from database_interactions import table_to_df
from check_liquidity_pool_data import get_pools_max_timestamp

def get_correlation_matrix():
    liquidity_pools = get_pools_max_timestamp()['table_name'].to_list()

    columns = ',\n\t'.join([f'p{i}.{"token1_price" if liquidity_pools[i][:4] != "WETH" else "token0_price"} as "{liquidity_pools[i]}"' for i in range(0, len(liquidity_pools))])
    joins = '\n\t'.join([f'INNER JOIN "{liquidity_pools[i]}" as p{i} ON p0.period_start_unix = p{i}.period_start_unix' for i in range(1, len(liquidity_pools))])
    where = ' AND \n\t'.join([f'p{i}.token1_price <> 0' for i in range(len(liquidity_pools))])

    query = f"""
        SELECT \n\t{columns}
        FROM "{liquidity_pools[0]}" as p0
        \t{joins}
        WHERE 
        \t{where}
        ORDER BY p1.period_start_unix;
    """

    # corr_matrix = table_to_df(command=query).corr()
    # sn.heatmap(corr_matrix, annot = True)
    # plt.show() 

    return table_to_df(command=query).corr()

def get_correlated_pairs(should_save=True):
    corr_matrix = get_correlation_matrix()
    filteredDf = corr_matrix[((0.9 < corr_matrix)) & (corr_matrix < np.float64(0.997))]

    if should_save:
        return save_correlated_pairs(list(filteredDf.unstack().dropna().index))
    
    return list(filteredDf.unstack().dropna().index)

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

def get_top_n_cointegrated_pairs(correlated_pairs, n=-1, should_save=False):
    cointegrated_pairs = []
    n = n if n != -1 else len(correlated_pairs)
    for pair in tqdm(correlated_pairs):
        p1, p2 = pair
        is_pair_cointegrated = is_cointegrated(p1, p2)
        if is_pair_cointegrated:
            cointegrated_pairs.append(pair)
            if len(cointegrated_pairs) == n:
                if should_save:
                    return save_cointegrated_pairs(cointegrated_pairs)
                else:
                    return cointegrated_pairs

    if should_save:
        return save_cointegrated_pairs(cointegrated_pairs)
    else:
        return cointegrated_pairs

def save_correlated_pairs(correlated_pairs):
    with open('historical_data/correlated_pairs.pickle', 'wb') as f:
        pickle.dump(correlated_pairs, f)

    f.close()
    return correlated_pairs

def load_correlated_pairs():
    with open('correlated_pairs.pickle', 'rb') as f:
        correlated_pairs = pickle.load(f)
        f.close()

    return correlated_pairs

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
    should_save = False

    if use_pickled_cointegrated_pairs:
        cointegrated_pairs = load_cointegrated_pairs()
    else:
        correlated_pairs = get_correlated_pairs(should_save=should_save)
        cointegrated_pairs = get_top_n_cointegrated_pairs(correlated_pairs=correlated_pairs, should_save=should_save)

    print(*cointegrated_pairs, sep="\n")
    print(len(cointegrated_pairs))
