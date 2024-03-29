import statsmodels.api as sm
import numpy as np
import statsmodels.tsa.stattools
import pandas as pd
import sys
sys.path.append('./strategies')
from strategies.abstract_strategy import Abstract_Strategy

class Granger_Causality_Strategy(Abstract_Strategy):
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment, should_batch_trade):
        super().__init__(number_of_sds_from_mean, window_size_in_seconds, percent_to_invest,
                         'Granger Causality', gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment, should_batch_trade)

    def initialise_historical_data(self, history_p1, history_p2):
        super().initialise_historical_data(history_p1, history_p2)
        self.initialise_kalman_filter_and_thresholds()

    def initialise_kalman_filter_and_thresholds(self):
        p1, p2 = self.history_p1, self.history_p2

        data = pd.DataFrame({'Asset1': p1, 'Asset2': p2})
        granger_results = statsmodels.tsa.stattools.grangercausalitytests(data, maxlag=[1], verbose=False)
        self.hedge_ratio = granger_results[1][1][1].params[0]

        spread = self.history_p1[-self.window_size_in_hours:] - \
            self.hedge_ratio * self.history_p2[-self.window_size_in_hours:]
        spread_mean = spread.mean()
        spread_std = spread.std()

        self.upper_threshold = spread_mean + self.number_of_sds_from_mean * spread_std
        self.lower_threshold = spread_mean - self.number_of_sds_from_mean * spread_std
        self.upper_thresholds.append(
            spread_mean + self.number_of_sds_from_mean * spread_std)
        self.lower_thresholds.append(
            spread_mean - self.number_of_sds_from_mean * spread_std)

    def recalculate_thresholds(self, has_trade=False):
        if not has_trade:
            self.update_hedge_ratio()
            spread = self.history_p1[-self.window_size_in_hours:] - \
                self.hedge_ratio * self.history_p2[-self.window_size_in_hours:]
            spread_mean = spread.mean()
            spread_std = spread.std()

            self.upper_threshold = spread_mean + self.number_of_sds_from_mean * spread_std
            self.lower_threshold = spread_mean - self.number_of_sds_from_mean * spread_std
            self.upper_thresholds.append(
                spread_mean + self.number_of_sds_from_mean * spread_std)
            self.lower_thresholds.append(
                spread_mean - self.number_of_sds_from_mean * spread_std)

    def update_hedge_ratio(self):
        p1, p2 = self.history_p1, self.history_p2
        data = pd.DataFrame({'Asset1': p1, 'Asset2': p2})
        granger_results = statsmodels.tsa.stattools.grangercausalitytests(data, maxlag=[1], verbose=False)
        self.hedge_ratio = granger_results[1][1][1].params[0]
        self.intercept_history.append(granger_results[1][1][1].params[2])
