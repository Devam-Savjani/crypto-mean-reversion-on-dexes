import statsmodels.api as sm
import numpy as np
import sys
sys.path.append('./strategies')
from strategies.abstract_strategy import Abstract_Strategy

class Lagged_Strategy(Abstract_Strategy):
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment, should_batch_trade, lag):
        super().__init__(number_of_sds_from_mean, window_size_in_seconds, percent_to_invest,
                         'Kalman', gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment, should_batch_trade)
        self.hedge_ratio_history = []
        self.lag = lag

    def initialise_historical_data(self, history_p1, history_p2):
        super().initialise_historical_data(history_p1, history_p2)
        self.initialise_thresholds()

    def initialise_thresholds(self):
        maxlag = min(self.lag, min(self.window_size_in_hours, len(self.history_p1))-1)
        p1, p2 = self.history_p1[-(self.window_size_in_hours+maxlag):], self.history_p2[-self.window_size_in_hours:]
        lagged_p1 = sm.tsa.lagmat(p1, maxlag=maxlag)[-self.window_size_in_hours:]
        
        model = sm.OLS(p2, sm.add_constant(lagged_p1))
        results = model.fit()

        self.hedge_ratio = results.params[1]

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
        maxlag = min(self.lag, min(self.window_size_in_hours, len(self.history_p1))-1)
        p1, p2 = self.history_p1[-(self.window_size_in_hours+maxlag):], self.history_p2[-self.window_size_in_hours:]
        lagged_p1 = sm.tsa.lagmat(p1, maxlag=maxlag)[-self.window_size_in_hours:]

        model = sm.OLS(p2, sm.add_constant(lagged_p1))
        results = model.fit()

        self.hedge_ratio = results.params[1]
        self.hedge_ratio_history.append(self.hedge_ratio)
