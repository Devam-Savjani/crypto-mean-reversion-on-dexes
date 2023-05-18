import numpy as np
from pykalman import KalmanFilter
from sklearn.preprocessing import StandardScaler
import sys
sys.path.append('./strategies')
import abstract_strategy

GAS_USED_BY_SWAP = 150000
GAS_USED_BY_LOAN = 100000
GAS_USED = (2 * GAS_USED_BY_SWAP) + GAS_USED_BY_LOAN

class Kalman_Filter_Strategy(abstract_strategy.Abstract_Strategy):
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment):
        super().__init__(number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, 'Kalman', gas_price_threshold, rebalance_threshold_as_percent_of_initial_investment)

    def initialise_historical_data(self, history_p1, history_p2):
        super().initialise_historical_data(history_p1, history_p2)
        self.initialise_kalman_filter_and_thresholds()
    
    def initialise_kalman_filter_and_thresholds(self):
        p1, p2 = self.history_p1, self.history_p2

        # create a StandardScaler object
        scalerX = StandardScaler()
        scalerY = StandardScaler()

        # transform the data using the scaler
        # extract the scaled x and y arrays
        scaled_x = scalerX.fit_transform(p2.reshape(-1,1)).flatten()
        scaled_y = scalerY.fit_transform(p1.reshape(-1,1)).flatten()
        self.stdX = np.std(p1)
        self.stdY = np.std(p2)

        state_cov_multiplier = np.power(0.01, 2)       # 0.1: spread_std=2.2, cov=16  ==> 0.01: 0.22, 0.16
        observation_cov = 0.001

        # observation matrix F is 2-dimensional, containing sym_a price and 1
        # there are data.shape[0] observations
        obs_mat_F = np.transpose(np.vstack([scaled_y, np.ones(len(scaled_y))])).reshape(-1, 1, 2)

        kf = KalmanFilter(n_dim_obs=1,                                           # y is 1-dimensional
                        n_dim_state=2,                                           # states (alpha, beta) is 2-dimensional
                        initial_state_mean=np.zeros(2),                          # initial value of intercept and slope theta0|0
                        initial_state_covariance=np.ones((2, 2)),                # initial cov matrix between intercept and slope P0|0
                        transition_matrices=np.eye(2),                           # G, constant
                        observation_matrices=obs_mat_F,                          # F, depends on x
                        observation_covariance=observation_cov,                  # v_t, constant
                        transition_covariance= np.eye(2)*state_cov_multiplier)   # w_t, constant

        state_means, state_covs = kf.filter(scaled_x)                            # observes p1 price

        self.means_trace = [state_means[-1]]
        self.covs_trace = [state_covs[-1]]

        self.hedge_ratio = state_means[-1,0] * self.stdX / self.stdY
        self.kf = kf
        self.scalerX = scalerX
        self.scalerY = scalerY

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
        scaled_x = self.scalerX.transform(np.reshape([p2[-1]], (-1,1))).flatten()[0]
        scaled_y = self.scalerY.transform(np.reshape([p1[-1]], (-1,1))).flatten()[0]

        observation_matrix_stepwise = np.array([[scaled_y, 1]])
        observation_stepwise = scaled_x

        state_means_stepwise, state_covs_stepwise = self.kf.filter_update(
            filtered_state_mean=self.means_trace[-1],
            filtered_state_covariance=self.covs_trace[-1],
            observation=observation_stepwise,
            observation_matrix=observation_matrix_stepwise)

        self.means_trace.append(state_means_stepwise)
        self.covs_trace.append(state_covs_stepwise)
        self.hedge_ratio = state_means_stepwise[0] * self.stdX / self.stdY
