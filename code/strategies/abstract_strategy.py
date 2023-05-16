import numpy as np

GAS_USED_BY_SWAP = 150000
GAS_USED_BY_LOAN = 100000
GAS_USED = (2 * GAS_USED_BY_SWAP) + GAS_USED_BY_LOAN


class Abstract_Strategy():
    def __init__(self, number_of_sds_from_mean, window_size_in_seconds, percent_to_invest, strategy_name):
        self.number_of_sds_from_mean = number_of_sds_from_mean
        self.has_initialised_historical_data = False
        self.window_size_in_seconds = window_size_in_seconds
        self.window_size_in_hours = window_size_in_seconds // (60 * 60)
        self.account_history = []
        self.percent_to_invest = percent_to_invest
        self.strategy_name = strategy_name

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
        vtl_eth = ctx['vtl_eth']
        liquidation_threshold = ctx['liquidation_threshold']

        has_trade = (len(open_positions['BUY']) +
                     len(open_positions['SELL'])) > 0

        price_of_pair1 = prices['P1']
        price_of_pair2 = prices['P2']

        self.new_tick(price_of_pair1, price_of_pair2, has_trade)
        spread = price_of_pair1 - self.hedge_ratio * price_of_pair2

        if has_trade:
            should_deposit_more = False
            for sell_trade in open_positions['SELL'].values():
                sell_token, sold_price, sell_volume, _ = sell_trade
                current_token_price = prices[f'P{sell_token[1]}']
                collatoral = sell_volume * sold_price / vtl_eth
                curr_value_of_loan_pct = (sell_volume * current_token_price) / collatoral
                if curr_value_of_loan_pct > liquidation_threshold:
                    should_deposit_more = True

            if spread < self.upper_threshold and spread > self.lower_threshold:
                self.account_history.append(account)

                swap_for_a = []
                swap_for_b = []

                if 'BUY' in open_positions:
                    for buy_position in open_positions['BUY'].values():
                        buy_token, _, buy_volume, buy_timestamp = buy_position
                        if account[buy_token] - buy_volume < 0:
                            swap_for_a.append(
                                (buy_token, abs(account[buy_token] - buy_volume)))

                if 'SELL' in open_positions:
                    for sell_position in open_positions['SELL'].values():
                        sell_token, sold_price, sell_volume, sell_timestamp = sell_position
                        position_token = account[sell_token]
                        current_token_price = prices[f'P{sell_token[1]}']

                        number_of_hours = (
                            timestamp - sell_timestamp) / (60 * 60)
                        hourly_yield = (1 + apy[sell_token])**(1 / (365*24)) - 1

                        new_token_balance = position_token + (sell_volume * ((sold_price / current_token_price) - 1)) - sell_volume * (hourly_yield ** number_of_hours)

                        if new_token_balance < 0:
                            swap_for_a.append((sell_token, abs(new_token_balance)))

                        new_eth_balance = account['WETH'] + account['collateral_WETH'] - (sold_price * sell_volume) - ((GAS_USED_BY_SWAP + GAS_USED_BY_LOAN) * gas_price_in_eth)

                        if new_eth_balance < 0:
                            n = len(open_positions['BUY'].values()) + len(swap_for_a) + len(swap_for_b) + 1
                            if account[sell_token] - (abs(new_eth_balance) + (n * GAS_USED_BY_SWAP * gas_price_in_eth)) / prices[f'P{sell_token[1]}'] < 0:
                                swap_for_b.append(
                                    (buy_token, abs(new_eth_balance) + (n * GAS_USED_BY_SWAP * gas_price_in_eth)))
                            else:
                                swap_for_b.append(
                                    (sell_token, abs(new_eth_balance) + (n * GAS_USED_BY_SWAP * gas_price_in_eth)))

                return {
                    'CLOSE': open_positions,
                    'SWAP': {
                        'A': swap_for_a,
                        'B': swap_for_b
                    }
                }

            if should_deposit_more:
                    return {'DEPOSIT': 0.1* account['WETH']}
            
            return {}

        else:
            volume_ratios_of_pairs = {
                'T1': (1 if self.hedge_ratio > 0 else -self.hedge_ratio),
                'T2': (self.hedge_ratio if self.hedge_ratio > 0 else 1)
            }

            swap_for_b = []
            if account['WETH'] < 10:
                swap_for_b.append(('T1', account['T1'] * prices['P1']))
                swap_for_b.append(('T2', account['T2'] * prices['P2']))

            if spread > self.upper_threshold:
                tx_cost = ((GAS_USED_BY_SWAP + GAS_USED_BY_SWAP +
                           GAS_USED_BY_LOAN) * gas_price_in_eth)

                if account['WETH'] < tx_cost:
                    return {}
                
                volume_ratio_coeff = (volume_ratios_of_pairs['T2'] / volume_ratios_of_pairs['T1'])
                volume_a = (account['WETH'] - tx_cost) / ((volume_ratio_coeff * prices['P2']) + (prices['P1'] / vtl_eth))
                volume_b = volume_ratio_coeff * volume_a
                
                # f = account['WETH'] - tx_cost - (volume_a * price_of_pair1 / vtl_eth) - volume_b * price_of_pair2

                # print(f'Collatoral Strat: {(volume_a * price_of_pair1 / vtl_eth)}')
                # print(f)
                # print(self.hedge_ratio - (volume_b / volume_a))

                self.account_history.append(account)
                if len(swap_for_b) > 0:
                    return {
                        'OPEN': {
                            'BUY': [('T2', volume_b * self.percent_to_invest)],
                            'SELL': [('T1', volume_a * self.percent_to_invest)]
                        },
                        'SWAP': {
                            'B': swap_for_b
                        }
                    }
                
                return {
                        'OPEN': {
                            'BUY': [('T2', volume_b * self.percent_to_invest)],
                            'SELL': [('T1', volume_a * self.percent_to_invest)]
                        }
                    }

            elif spread < self.lower_threshold:
                tx_cost = ((GAS_USED_BY_SWAP + GAS_USED_BY_SWAP +
                           GAS_USED_BY_LOAN) * gas_price_in_eth)

                if account['WETH'] < tx_cost:
                    return {}
                
                volume_ratio_coeff = volume_ratios_of_pairs['T1'] / volume_ratios_of_pairs['T2']

                volume_b = (account['WETH'] - tx_cost) / ((volume_ratio_coeff * prices['P1']) + (prices['P2'] / vtl_eth))
                volume_a = volume_ratio_coeff * volume_b

                self.account_history.append(account)

                if len(swap_for_b) > 0:
                    return {
                        'OPEN': {
                            'BUY': [('T1', volume_a * self.percent_to_invest)],
                            'SELL': [('T2', volume_b * self.percent_to_invest)]
                        },
                        'SWAP': {
                            'B': swap_for_b
                        }
                    }
                
                return {
                        'OPEN': {
                            'BUY': [('T1', volume_a * self.percent_to_invest)],
                            'SELL': [('T2', volume_b * self.percent_to_invest)]
                        }
                    }
                
            else:
                return {}
