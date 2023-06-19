from utils.database_interactions import table_to_df
import pandas as pd

def days_to_seconds(days): return int(days * 24 * 60 * 60)

def calculate_betas(backtest_constant_hr, backtest_sliding_window, backtest_lagged, backtest_gc, backtest_kalman_filter):
    market_price_label = 'token0_price'
    market_return = table_to_df(command=f'SELECT period_start_unix, {market_price_label} FROM "USDC_WETH_0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640";', path_to_config='../utils/database.ini')

    merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_constant_hr.times, 'value': backtest_constant_hr.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    returns = merged.pct_change().dropna()
    beta_constant = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_sliding_window.times, 'value': backtest_sliding_window.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    returns = merged.pct_change().dropna()
    beta_sw = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_lagged.times, 'value': backtest_lagged.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    returns = merged.pct_change().dropna()
    beta_lagged = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_gc.times, 'value': backtest_gc.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    returns = merged.pct_change().dropna()
    beta_GC = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_kalman_filter.times, 'value': backtest_kalman_filter.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    returns = merged.pct_change().dropna()
    beta_KF = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    return [beta_constant, beta_sw, beta_lagged, beta_GC, beta_KF]