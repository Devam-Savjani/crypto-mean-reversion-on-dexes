import statsmodels.api as sm
import abstract_strategy
import numpy as np
from pykalman import KalmanFilter
import sys
sys.path.append('./strategies')


GAS_USED_BY_SWAP = 150000
GAS_USED_BY_LOAN = 100000
GAS_USED = (2 * GAS_USED_BY_SWAP) + GAS_USED_BY_LOAN


class Kalman_Filter_Strategy(abstract_strategy.Abstract_Strategy):
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment):
        super().__init__(number_of_sds_from_mean, window_size_in_seconds, percent_to_invest,
                         'Kalman', gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment)
        self.hedge_ratio_history = []

    def initialise_historical_data(self, history_p1, history_p2):
        super().initialise_historical_data(history_p1, history_p2)
        self.initialise_kalman_filter_and_thresholds()

    def initialise_kalman_filter_and_thresholds(self):
        p1, p2 = self.history_p1, self.history_p2

        obs_mat = np.transpose(
            np.vstack([p1, np.ones(len(p1))])).reshape(-1, 1, 2)

        model = sm.OLS(p2, sm.add_constant(p1))
        initial_state = model.fit().params[::-1]

        kf = KalmanFilter(
            n_dim_state=2,
            initial_state_mean=initial_state,
            transition_matrices=np.eye(2),
            observation_matrices=obs_mat,
            transition_covariance=1e-5 * np.eye(2)
        )

        state_means, state_covs = kf.filter(p2)

        self.means_trace = [state_means[-1]]
        self.covs_trace = [state_covs[-1]]

        self.hedge_ratio = state_means[-1, 0]
        self.kf = kf

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
        x = p2[-1]
        y = p1[-1]

        observation_matrix_stepwise = np.array([[y, 1]])
        observation_stepwise = x

        state_means_stepwise, state_covs_stepwise = self.kf.filter_update(
            filtered_state_mean=self.means_trace[-1],
            filtered_state_covariance=self.covs_trace[-1],
            observation=observation_stepwise,
            observation_matrix=observation_matrix_stepwise)

        self.means_trace.append(state_means_stepwise)
        self.covs_trace.append(state_covs_stepwise)
        self.hedge_ratio = state_means_stepwise[0]
        self.hedge_ratio_history.append(self.hedge_ratio)
