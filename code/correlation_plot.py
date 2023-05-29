import sys
sys.path.append('./historical_data')
from database_interactions import table_to_df
import seaborn as sn
import matplotlib.pyplot as plt
from constants import liquidity_pools_query

def get_correlation_matrix():
    liquidity_pools = table_to_df(command=liquidity_pools_query, path_to_config='historical_data/database.ini')['table_name'].to_list()

    columns = ',\n\t'.join([f'p{i}.{"token1_price" if liquidity_pools[i][:4] != "WETH" else "token0_price"} as {liquidity_pools[i][:15]}' for i in range(0, len(liquidity_pools))])
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

    return table_to_df(command=query, path_to_config='historical_data/database.ini').corr()

def plot_heatmap():
    corr_matrix = get_correlation_matrix()
    sn.heatmap(corr_matrix, annot = True)
    plt.show() 

if __name__ == "__main__":
    plot_heatmap()
