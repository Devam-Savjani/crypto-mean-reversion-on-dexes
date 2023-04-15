import numpy as np

class Mean_Reversion_Strategy():
    def __init__(self, cointegrated_pair, hedge_ratio, number_of_sds_from_mean, window_size, percent_to_invest):
        self.number_of_sds_from_mean = number_of_sds_from_mean
        self.cointegrated_pair = cointegrated_pair
        self.hedge_ratio = hedge_ratio
        self.has_initialised_historical_data = False
        self.window_size = window_size
        self.account_history = []
        self.percent_to_invest = percent_to_invest

    def initialise_historical_data(self, history_p1, history_p2):
        self.history_p1 = history_p1.to_numpy()[-self.window_size:]
        self.history_p2 = history_p2.to_numpy()[-self.window_size:]
        self.upper_thresholds = []
        self.lower_thresholds = []

        self.recalculate_thresholds()
        self.has_initialised_historical_data = True

    def recalculate_thresholds(self):
        spread = self.history_p1 - self.hedge_ratio * self.history_p2
        spread_mean = spread.mean()
        spread_std = spread.std()

        self.upper_threshold = spread_mean + self.number_of_sds_from_mean * spread_std
        self.lower_threshold = spread_mean - self.number_of_sds_from_mean * spread_std
        self.upper_thresholds.append(spread_mean + self.number_of_sds_from_mean * spread_std)
        self.lower_thresholds.append(spread_mean - self.number_of_sds_from_mean * spread_std)

    def new_tick(self, price_of_pair1, price_of_pair2):
        if not self.has_initialised_historical_data:
            raise Exception('Mean Reversion Strategy not initialised with historical data')

        self.history_p1 = np.append(self.history_p1, price_of_pair1)
        self.history_p2 = np.append(self.history_p2, price_of_pair2)

        self.history_p1 = np.delete(self.history_p1, 0)
        self.history_p2 = np.delete(self.history_p2, 0)

        self.recalculate_thresholds()

    def generate_signal(self, ctx, prices):

        open_positions = ctx['open_positions']
        account = ctx['account']
        has_trade = (len(open_positions['BUY']) + len(open_positions['SELL'])) > 0
        
        price_of_pair1 = prices['P1']
        price_of_pair2 = prices['P2']
        
        self.new_tick(price_of_pair1, price_of_pair2)
        spread = price_of_pair1 - self.hedge_ratio * price_of_pair2

        if has_trade:
            if spread < self.upper_threshold and spread > self.lower_threshold:
                self.account_history.append(account)
                return {'CLOSE': open_positions}
        else:
            volume_ratio = (price_of_pair1 / price_of_pair2) * self.hedge_ratio
            volume_ratios_of_pairs = {
                'P1': (1 if self.hedge_ratio > 0 else -volume_ratio),
                'P2': (volume_ratio if self.hedge_ratio > 0 else 1)
            }
            amount_of_p1_b = account['P1'][1]
            amount_of_p2_b = account['P2'][1]

            if spread > self.upper_threshold:
                volume_to_trade = {
                    'P1': (volume_ratios_of_pairs['P1'] / volume_ratios_of_pairs['P2']) * (amount_of_p2_b / price_of_pair2),
                    'P2': (volume_ratios_of_pairs['P2'] / volume_ratios_of_pairs['P2']) * (amount_of_p2_b / price_of_pair2)
                }
                self.account_history.append(account)
                return {
                    'OPEN': {
                        'BUY': [('P2', volume_to_trade['P2'] * self.percent_to_invest)],
                        'SELL': [('P1', volume_to_trade['P1'] * self.percent_to_invest)]
                    }}
            elif spread < self.lower_threshold:
                volume_to_trade = {
                    'P1': (volume_ratios_of_pairs['P1'] / volume_ratios_of_pairs['P1']) * (amount_of_p1_b / price_of_pair1),
                    'P2': (volume_ratios_of_pairs['P2'] / volume_ratios_of_pairs['P1']) * (amount_of_p1_b / price_of_pair1) 
                }
                self.account_history.append(account)
                return {
                    'OPEN': {
                        'BUY': [('P1', volume_to_trade['P1'] * self.percent_to_invest)],
                        'SELL': [('P2', volume_to_trade['P2'] * self.percent_to_invest)]
                    }}
            else:
                return None

    