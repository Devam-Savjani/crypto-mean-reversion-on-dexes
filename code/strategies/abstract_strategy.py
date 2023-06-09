import numpy as np
from constants import GAS_USED_BY_SWAP, GAS_USED_BY_BORROW, GAS_USED_BY_REPAY, GAS_USED_BY_DEPOSITING_COLLATERAL, GAS_USED_BY_OPEN_BUY_AND_SELL_POSITION, GAS_USED_BY_CLOSE_BUY_AND_SELL_POSITION


class Abstract_Strategy():
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, strategy_name, gas_price_threshold=1.25e-07, rebalance_threshold_as_percent_of_initial_investment=0.001, should_batch_trade=True):
        self.number_of_sds_from_mean = number_of_sds_from_mean
        self.has_initialised_historical_data = False
        self.window_size_in_seconds = window_size_in_seconds
        self.window_size_in_hours = window_size_in_seconds // (60 * 60)
        self.account_history = []
        self.percent_to_invest = percent_to_invest
        self.strategy_name = strategy_name
        self.gas_price_threshold = gas_price_threshold
        self.initial_WETH = None
        self.rebalance_threshold_as_percent_of_initial_investment = rebalance_threshold_as_percent_of_initial_investment
        self.spreads = []
        self.should_batch_trade = should_batch_trade
        self.gas_used_by_opening_positions = (GAS_USED_BY_OPEN_BUY_AND_SELL_POSITION if should_batch_trade else (GAS_USED_BY_SWAP + GAS_USED_BY_SWAP + GAS_USED_BY_BORROW))
        self.gas_used_by_closing_positions = (GAS_USED_BY_CLOSE_BUY_AND_SELL_POSITION if should_batch_trade else (GAS_USED_BY_SWAP + GAS_USED_BY_SWAP + GAS_USED_BY_REPAY))

    def initialise_historical_data(self, history_p1, history_p2):
        self.history_p1 = history_p1.to_numpy()
        self.history_p2 = history_p2.to_numpy()
        self.upper_thresholds = []
        self.lower_thresholds = []
        self.has_initialised_historical_data = True

    def recalculate_thresholds(self, has_trade=False):
        raise NotImplementedError("recalculate_thresholds not implemented")

    def new_tick(self, price_of_pair1, price_of_pair2, has_trade):
        if not self.has_initialised_historical_data:
            raise Exception(
                f'{self.strategy_name} not initialised with historical data')

        self.history_p1 = np.append(self.history_p1, price_of_pair1)
        self.history_p2 = np.append(self.history_p2, price_of_pair2)

        self.recalculate_thresholds()

    def generate_signal(self, ctx, prices):

        open_positions = ctx['open_positions']
        account = ctx['account']
        gas_price_in_eth = ctx['gas_price_in_eth']
        timestamp = ctx['timestamp']
        apy = ctx['apy']
        ltv_eth = ctx['ltv_eth']
        liquidation_threshold = ctx['liquidation_threshold']

        if self.initial_WETH is None:
            self.initial_WETH = account['WETH']

        has_trade = (len(open_positions['BUY']) +
                     len(open_positions['SELL'])) > 0

        price_of_pair1 = prices['P1']
        price_of_pair2 = prices['P2']

        self.new_tick(price_of_pair1, price_of_pair2, has_trade)
        spread = price_of_pair1 - self.hedge_ratio * price_of_pair2
        self.spreads.append(spread)

        orders = []

        if has_trade:
            should_deposit_more = False
            for sell_trade in open_positions['SELL'].values():
                sell_token, sold_price, sell_volume, _ = sell_trade
                current_token_price = prices[f'P{sell_token[1]}']
                curr_value_of_loan_pct = (
                    sell_volume * current_token_price) / account['collateral_WETH']
                if curr_value_of_loan_pct > liquidation_threshold:
                    should_deposit_more = True

            if spread < self.upper_threshold and spread > self.lower_threshold and gas_price_in_eth < self.gas_price_threshold:
                self.account_history.append(account)

                if account['ETH'] - (self.gas_used_by_closing_positions * gas_price_in_eth) < 0:
                    orders += [('BUY ETH', 0.1)]

                return orders + [
                    ('CLOSE', ('SELL', [
                     sell_position for sell_position in open_positions['SELL']])),
                    ('CLOSE', ('BUY', [
                     buy_position for buy_position in open_positions['BUY']])),
                ]
            
            if should_deposit_more and account['ETH'] - (GAS_USED_BY_DEPOSITING_COLLATERAL * gas_price_in_eth) < 0:
                orders += [('BUY ETH', 0.1)]

            deposit_amount = ((sell_volume * current_token_price) /
                              liquidation_threshold) - account['collateral_WETH']
            return (orders + [('DEPOSIT', deposit_amount)]) if should_deposit_more else []

        else:
            volume_ratios_of_pairs = {
                'T1': (1 if self.hedge_ratio > 0 else -self.hedge_ratio),
                'T2': (self.hedge_ratio if self.hedge_ratio > 0 else 1)
            }

            if gas_price_in_eth > self.gas_price_threshold:
                return []

            swap_for_token1 = []
            if account['WETH'] < self.rebalance_threshold_as_percent_of_initial_investment * self.initial_WETH:
                if account['T1'] > 0:
                    swap_for_token1.append(
                        ('T1', account['T1'] * prices['P1']))
                if account['T2'] > 0:
                    swap_for_token1.append(
                        ('T2', account['T2'] * prices['P2']))

            if spread > self.upper_threshold:
                volume_ratio_coeff = (
                    volume_ratios_of_pairs['T1'] / volume_ratios_of_pairs['T2'])

                volume_a = account['WETH'] / \
                    ((volume_ratio_coeff *
                     prices['P2']) + (prices['P1'] / ltv_eth))
                volume_b = volume_ratio_coeff * volume_a

                self.account_history.append(account)

                if account['ETH'] - ((self.gas_used_by_opening_positions + (GAS_USED_BY_SWAP * len(swap_for_token1))) * gas_price_in_eth) < 0:
                    orders += [('BUY ETH', 0.1)]

                if len(swap_for_token1) > 0:
                    orders += [('SWAP', (False, swap_for_token1))]

                return orders + [
                    ('OPEN', ('BUY', 'T2', volume_b * self.percent_to_invest)),
                    ('OPEN', ('SELL', 'T1', volume_a * self.percent_to_invest))
                ]

            elif spread < self.lower_threshold:
                volume_ratio_coeff = volume_ratios_of_pairs['T2'] / \
                    volume_ratios_of_pairs['T1']

                volume_b = account['WETH'] / \
                    ((volume_ratio_coeff *
                     prices['P1']) + (prices['P2'] / ltv_eth))
                volume_a = volume_ratio_coeff * volume_b

                self.account_history.append(account)

                if account['ETH'] - ((self.gas_used_by_opening_positions + (GAS_USED_BY_SWAP * len(swap_for_token1))) * gas_price_in_eth) < 0:
                    orders += [('BUY ETH', 0.1)]

                if len(swap_for_token1) > 0:
                    orders += [('SWAP', (False, swap_for_token1))]

                return orders + [
                    ('OPEN', ('BUY', 'T1', volume_a * self.percent_to_invest)),
                    ('OPEN', ('SELL', 'T2', volume_b * self.percent_to_invest))
                ]

            else:
                return []
