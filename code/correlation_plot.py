import sys
sys.path.append('./historical_data')
from database_interactions import table_to_df
import seaborn as sn
import matplotlib.pyplot as plt
import numpy as np
from check_liquidity_pool_data import get_pools_max_timestamp
from constants import CORRELATION_LOWER_LIMIT, CORRELATION_UPPER_LIMIT, LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY

def get_correlation_matrix():
    liquidity_pools = table_to_df(command=LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY, path_to_config='historical_data/database.ini')['table_name'].to_list()

    columns = ',\n\t'.join([f'p{i}.{"token1_price" if liquidity_pools[i][:4] != "WETH" else "token0_price"} as {liquidity_pools[i]}' for i in range(0, len(liquidity_pools))])
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

    return table_to_df(command=query, path_to_config='historical_data/database.ini').corr().abs()

def plot_heatmap():
    corr_matrix = get_correlation_matrix()
    sn.heatmap(corr_matrix, annot = True)
    plt.show() 

if __name__ == "__main__":
    corr_matrix = get_correlation_matrix()

    filteredDf = corr_matrix[((CORRELATION_LOWER_LIMIT < corr_matrix)) & (corr_matrix < CORRELATION_UPPER_LIMIT)]
    print(filteredDf)

    print(filteredDf.unstack().dropna())
    print(list(filteredDf.unstack().dropna().index))

    plot_heatmap()
