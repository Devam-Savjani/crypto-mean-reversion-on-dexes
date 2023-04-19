import numpy as np
import statsmodels.api as sm


class Mean_Reversion_Strategy():
    def __init__(self, cointegrated_pair, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest):
        self.number_of_sds_from_mean = number_of_sds_from_mean
        self.cointegrated_pair = cointegrated_pair
        self.has_initialised_historical_data = False
        self.window_size_in_seconds = window_size_in_seconds
        self.window_size_in_hours = window_size_in_seconds // (60 * 60)
        self.account_history = []
        self.percent_to_invest = percent_to_invest

    def calculate_hedge_ratio(self):
        # Regress the spread on the two assets
        model = sm.OLS(self.history_p1, sm.add_constant(self.history_p2))
        results = model.fit()

        # Calculate the ratio of the coefficients
        # Gradient of the OLS i.e. X = results.params[0] + results.params[1] 'p2_token1_price'
        ratio = results.params[1]
        return ratio

    def initialise_historical_data(self, history_p1, history_p2):
        self.history_p1 = history_p1.to_numpy()[-self.window_size_in_hours:]
        self.history_p2 = history_p2.to_numpy()[-self.window_size_in_hours:]
        self.upper_thresholds = []
        self.lower_thresholds = []
        self.hedge_ratio = self.calculate_hedge_ratio()

        self.recalculate_thresholds()
        self.has_initialised_historical_data = True

    def recalculate_thresholds(self):
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

    def new_tick(self, price_of_pair1, price_of_pair2):
        if not self.has_initialised_historical_data:
            raise Exception(
                'Mean Reversion Strategy not initialised with historical data')

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
            # volume_ratio = (price_of_pair1 / price_of_pair2) * self.hedge_ratio
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
