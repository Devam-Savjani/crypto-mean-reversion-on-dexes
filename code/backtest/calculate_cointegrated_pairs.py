import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
import numpy as np
from tqdm import tqdm
import pickle
import statsmodels.api as sm
from utils.database_interactions import table_to_df
from utils.check_liquidity_pool_data import get_pools_max_timestamp
from utils.constants import CORRELATION_LOWER_LIMIT, CORRELATION_UPPER_LIMIT
import seaborn as sn
import matplotlib.pyplot as plt

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

    return table_to_df(command=query, path_to_config='../utils/database.ini').corr()

def get_correlated_pairs():
    corr_matrix = get_correlation_matrix()
    # sn.heatmap(corr_matrix, annot = True, fmt='.4f')
    # plt.xticks(fontsize = 8) 
    # plt.show()
    filteredDf = corr_matrix[((CORRELATION_LOWER_LIMIT < corr_matrix)) & (corr_matrix < CORRELATION_UPPER_LIMIT)]

    return list(filteredDf.unstack().dropna().drop_duplicates().index)

def is_cointegrated(p1, p2):
    merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.{'token1_price' if p1.split('_')[0] != 'WETH' else 'token0_price'} as p1_token_price, p2.id as p2_id, p2.{'token1_price' if p2.split('_')[0] != 'WETH' else 'token0_price'} as p2_token_price
                FROM "{p1}" as p1 INNER JOIN "{p2}" as p2
                ON p1.period_start_unix = p2.period_start_unix;
                """, path_to_config='../utils/database.ini')

    result = sm.tsa.stattools.coint(merged['p1_token_price'], merged['p2_token_price'])

    # corr_matrix = get_correlation_matrix()
    # p1_name = p1.replace("_", "\_")
    # p2_name = p2.replace("_", "\_")
    # rounding_num = 6
    # print(f'\\truncate{{12em}}{{{p1_name}}} & \\truncate{{12em}}{{{p2_name}}} & {round(result[0], rounding_num)} & {round(result[2][0], rounding_num)} & {round(result[2][1], rounding_num)} & {round(result[2][2], rounding_num)} & {round(corr_matrix[p1][p2], rounding_num)}\\\\\\hline')
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
                return cointegrated_pairs

    return cointegrated_pairs

def save_cointegrated_pairs(cointegrated_pairs):
    with open('utils/cointegrated_pairs.pickle', 'wb') as f:
        pickle.dump(cointegrated_pairs, f)

    f.close()
    return cointegrated_pairs

def load_cointegrated_pairs(path='utils/cointegrated_pairs.pickle'):
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
        correlated_pairs = get_correlated_pairs()
        cointegrated_pairs = get_top_n_cointegrated_pairs(correlated_pairs=correlated_pairs, should_save=should_save)

    print(*cointegrated_pairs, sep="\n")
    print(len(cointegrated_pairs))
