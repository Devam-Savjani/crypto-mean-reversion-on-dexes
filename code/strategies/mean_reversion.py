import statsmodels.api as sm
import sys
sys.path.append('./strategies')
import abstract_strategy

GAS_USED_BY_SWAP = 150000
GAS_USED_BY_LOAN = 100000
GAS_USED = (2 * GAS_USED_BY_SWAP) + GAS_USED_BY_LOAN


class Mean_Reversion_Strategy(abstract_strategy.Abstract_Strategy):
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment):
        super().__init__(number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, 'Mean Reversion', gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment)

    def calculate_hedge_ratio(self):
        # Regress the spread on the two assets
        model = sm.OLS(self.history_p1, sm.add_constant(self.history_p2))
        results = model.fit()

        # Calculate the ratio of the coefficients
        # Gradient of the OLS i.e. X = results.params[0] + results.params[1] 'p2_token1_price'
        return results.params[1]

    def initialise_historical_data(self, history_p1, history_p2):
        super().initialise_historical_data(history_p1, history_p2)
        self.hedge_ratio = self.calculate_hedge_ratio()
        self.recalculate_thresholds()

    def recalculate_thresholds(self, has_trade=False):
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
