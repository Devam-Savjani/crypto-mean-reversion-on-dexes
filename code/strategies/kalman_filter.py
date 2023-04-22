import numpy as np
from pykalman import KalmanFilter
from sklearn.preprocessing import StandardScaler
import pandas as pd
from scipy import poly1d
import statsmodels.api as sm
import matplotlib.pyplot as plt

class Kalman_Filter_Strategy():
    def __init__(self, cointegrated_pair, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest):
        self.number_of_sds_from_mean = number_of_sds_from_mean
        self.cointegrated_pair = cointegrated_pair
        self.has_initialised_historical_data = False
        self.window_size_in_seconds = window_size_in_seconds
        self.window_size_in_hours = window_size_in_seconds // (60 * 60)
        self.account_history = []
        self.percent_to_invest = percent_to_invest
        self.n = 0
        self.N = 3000

    def initialise_historical_data(self, history_p1, history_p2):
        self.history_p1 = history_p1.to_numpy()
        self.history_p2 = history_p2.to_numpy()
        self.upper_thresholds = []
        self.lower_thresholds = []
        self.initialise_kalman_filter_and_thresholds()
        self.has_initialised_historical_data = True
    
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

    def recalculate_thresholds(self):
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

        # if self.n % self.N == 0:
        #     cm = plt.get_cmap('jet')
        #     colors = np.linspace(0.1, 1, len(self.history_p1))
        #     sc = plt.scatter(self.history_p2, self.history_p1, s=30, c=colors, cmap=cm, edgecolor='k', alpha=0.7)

        #     xi = np.linspace(self.history_p2.min(), self.history_p2.max(), 2)
        #     model = sm.OLS(self.history_p1[-1000:], sm.add_constant(self.history_p2[-1000:]))
        #     results = model.fit()
        #     plt.plot(xi, poly1d(results.params[::-1])(xi), alpha=.9, lw=1)
        #     print(f'Hedge Ratio: {self.hedge_ratio}')
        #     print(f'Params: {results.params}')
        #     plt.xlabel('p2')
        #     plt.ylabel('p1')
        #     plt.show()
        self.n = self.n + 1

    def new_tick(self, price_of_pair1, price_of_pair2):
        if not self.has_initialised_historical_data:
            raise Exception(
                'Kalman Filter Strategy not initialised with historical data')

        self.history_p1 = np.append(self.history_p1, price_of_pair1)
        self.history_p2 = np.append(self.history_p2, price_of_pair2)

        self.recalculate_thresholds()

    def generate_signal(self, ctx, prices):

        open_positions = ctx['open_positions']
        account = ctx['account']
        has_trade = (len(open_positions['BUY']) +
                     len(open_positions['SELL'])) > 0

        price_of_pair1 = prices['P1']
        price_of_pair2 = prices['P2']

        self.new_tick(price_of_pair1, price_of_pair2)
        spread = price_of_pair1 - self.hedge_ratio * price_of_pair2

        if has_trade:
            if spread < self.upper_threshold and spread > self.lower_threshold:
                self.account_history.append(account)

                swap_for_a = []
                swap_for_b = []

                if 'BUY' in open_positions:
                    for buy_position in open_positions['BUY'].values():
                        buy_pair, _, buy_volume = buy_position
                        if account[buy_pair][0] - buy_volume < 0:
                            swap_for_a.append(
                                (buy_pair, abs(account[buy_pair][0] - buy_volume)))

                if 'SELL' in open_positions:
                    for sell_position in open_positions['SELL'].values():
                        sell_pair, sell_price, sell_volume = sell_position
                        position_a, position_b = account[sell_pair]
                        if position_a + (sell_volume * ((sell_price / prices[sell_pair]) - 1)) < 0:
                            swap_for_a.append((sell_pair, abs(
                                position_a + (sell_volume * ((sell_price / prices[sell_pair]) - 1)))))

                        if position_b - (sell_price * sell_volume) < 0:
                            swap_for_b.append(
                                (sell_pair, abs(position_b - (sell_price * sell_volume))))

                return {
                    'CLOSE': open_positions,
                    'SWAP': {
                        'A': swap_for_a,
                        'B': swap_for_b
                    }
                }
        else:
            volume_ratio = self.hedge_ratio
            volume_ratios_of_pairs = {
                'P1': (1 if self.hedge_ratio > 0 else -volume_ratio),
                'P2': (volume_ratio if self.hedge_ratio > 0 else 1)
            }
            amount_of_p1_b = account['P1'][1]
            amount_of_p2_b = account['P2'][1]

            if spread > self.upper_threshold:
                if amount_of_p1_b == 0:
                    return None
                volume_factor = (amount_of_p1_b / price_of_pair1)
                volume_to_trade = {
                    'P1': (volume_ratios_of_pairs['P1'] / volume_ratios_of_pairs['P1']) * volume_factor,
                    'P2': (volume_ratios_of_pairs['P2'] / volume_ratios_of_pairs['P1']) * volume_factor
                }
                self.account_history.append(account)

                if amount_of_p2_b - (volume_to_trade['P2'] * self.percent_to_invest * prices['P2']) < 0:
                    volume_factor = (amount_of_p2_b / price_of_pair2)
                    volume_to_trade = {
                        'P1': (volume_ratios_of_pairs['P1'] / volume_ratios_of_pairs['P2']) * volume_factor,
                        'P2': (volume_ratios_of_pairs['P2'] / volume_ratios_of_pairs['P2']) * volume_factor
                    }

                return {
                    'OPEN': {
                        'BUY': [('P2', volume_to_trade['P2'] * self.percent_to_invest)],
                        'SELL': [('P1', volume_to_trade['P1'] * self.percent_to_invest)]
                    }
                }

            elif spread < self.lower_threshold:
                if amount_of_p2_b == 0:
                    return None
                volume_factor = (amount_of_p2_b / price_of_pair2)
                volume_to_trade = {
                    'P1': (volume_ratios_of_pairs['P1'] / volume_ratios_of_pairs['P2']) * volume_factor,
                    'P2': (volume_ratios_of_pairs['P2'] / volume_ratios_of_pairs['P2']) * volume_factor
                }
                self.account_history.append(account)

                if amount_of_p1_b - (volume_to_trade['P1'] * self.percent_to_invest * prices['P1']) < 0:
                    volume_factor = (amount_of_p1_b / price_of_pair1)
                    volume_to_trade = {
                        'P1': volume_factor,
                        'P2': (volume_ratios_of_pairs['P2'] / volume_ratios_of_pairs['P1']) * volume_factor
                    }

                return {
                    'OPEN': {
                        'BUY': [('P1', volume_to_trade['P1'] * self.percent_to_invest)],
                        'SELL': [('P2', volume_to_trade['P2'] * self.percent_to_invest)]
                    }
                }
            else:
                return None
