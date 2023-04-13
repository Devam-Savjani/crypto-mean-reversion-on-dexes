import numpy as np

class Mean_Reversion_Strategy():
    def __init__(self, cointegrated_pair, hedge_ratio, history_p1, history_p2, number_of_sds_from_mean):
        self.history_p1 = history_p1
        self.history_p2 = history_p2
        self.number_of_sds_from_mean = number_of_sds_from_mean
        self.cointegrated_pair = cointegrated_pair
        self.hedge_ratio = hedge_ratio

        self.upper_thresholds = []
        self.lower_thresholds = []

        self.recalculate_thresholds()

    def recalculate_thresholds(self):
        spread = self.history_p1 - self.hedge_ratio * self.history_p2
        spread_mean = spread.mean()
        spread_std = spread.std()

        self.upper_threshold = spread_mean + self.number_of_sds_from_mean * spread_std
        self.lower_threshold = spread_mean - self.number_of_sds_from_mean * spread_std
        self.upper_thresholds.append(spread_mean + self.number_of_sds_from_mean * spread_std)
        self.lower_thresholds.append(spread_mean - self.number_of_sds_from_mean * spread_std)

    def new_tick(self, price_of_pair1, price_of_pair2):
        self.history_p1 = np.append(self.history_p1, price_of_pair1)
        self.history_p2 = np.append(self.history_p2, price_of_pair2)

        self.history_p1 = np.delete(self.history_p1, 0)
        self.history_p2 = np.delete(self.history_p2, 0)

        self.recalculate_thresholds()

    def generate_signal(self, ctx, prices):

        open_positions = ctx['open_positions']
        has_trade = (len(open_positions['BUY']) + len(open_positions['SELL'])) > 0
        
        price_of_pair1 = prices['P1']
        price_of_pair2 = prices['P2']
        
        self.new_tick(price_of_pair1, price_of_pair2)
        spread = price_of_pair1 - self.hedge_ratio * price_of_pair2

        if has_trade:
            if spread < self.upper_threshold and spread > self.lower_threshold:
                return {'CLOSE': open_positions}
        else:
            volume_ratio = (price_of_pair1 / price_of_pair2) * self.hedge_ratio
            if spread > self.upper_threshold:
                return {
                    'OPEN': {
                        'BUY': [('P2', volume_ratio)],
                        'SELL': [('P1', 1)]
                    }}
            elif spread < self.lower_threshold:
                return {
                    'OPEN': {
                        'BUY': [('P1', 1)],
                        'SELL': [('P2', volume_ratio)]
                    }}
            else:
                return None

    