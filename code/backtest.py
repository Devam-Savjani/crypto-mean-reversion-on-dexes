import matplotlib.pyplot as plt
import warnings
from historical_data.calculate_cointegrated_pairs import load_cointegrated_pairs
from strategies.mean_reversion import Mean_Reversion_Strategy
from historical_data.database_interactions import table_to_df

def days_to_seconds(days): return int(days * 24 * 60 * 60)

class Backtest():
    def __init__(self, initial_account):
        self.trades = None
        self.initial_account = initial_account

    def fetch_and_preprocess_data(self, cointegrated_pair, strategy):
        merged = table_to_df(command=f"""
                        SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_Price_Per_token0 as p1_token1_Price_Per_token0, p2.id as p2_id, p2.token1_Price_Per_token0 as p2_token1_Price_Per_token0
                        FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{cointegrated_pair[1]}" as p2
                        ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price_per_token0 <> 0 AND p2.token1_price_per_token0 <> 0;
                        """, path_to_config='historical_data/database.ini')

        window_size_in_seconds = strategy.window_size

        history_arg = merged.loc[merged['period_start_unix'] <
                                window_size_in_seconds + merged['period_start_unix'][0]]
        strategy.initialise_historical_data(
            history_p1=history_arg['p1_token1_price_per_token0'], history_p2=history_arg['p2_token1_price_per_token0'])

        history_remaining = merged.loc[merged['period_start_unix']
                                    >= window_size_in_seconds + merged['period_start_unix'][0]]
        history_remaining_p1 = history_remaining['p1_token1_price_per_token0']
        history_remaining_p2 = history_remaining['p2_token1_price_per_token0']
        return history_remaining, history_remaining_p1, history_remaining_p2

    def rebalance_account(self, prices):
        position_a, position_b = self.account['P1']
        if position_a < 0:
            self.account['P1'] = (position_a + abs(position_a), position_b - (abs(position_a) * prices['P1']))
            warnings.warn(f'Boosting P1[0] by {abs(position_a)}')
        
        if position_b < 0:
            self.account['P1'] = (position_a - (abs(position_b) / prices['P1']), position_b + abs(position_b))
            warnings.warn(f'Boosting P1[1] by {abs(position_b)}')

        position_a, position_b = self.account['P2']
        if position_a < 0:
            self.account['P2'] = (position_a + abs(position_a), position_b - (abs(position_a) * prices['P2']))
            warnings.warn(f'Boosting P2[0] by {abs(position_a)}')
        
        if position_b < 0:
            self.account['P2'] = (position_a - (abs(position_b) / prices['P2']), position_b + abs(position_b))
            warnings.warn(f'Boosting P2[1] by {abs(position_b)}')

    def check_account(self, open_or_close, buy_or_sell):
        if self.account['P1'][0] < 0:
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P1[0]')

        if self.account['P1'][1] < 0:
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P1[1]')

        if self.account['P2'][0] < 0:
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P2[0]')

        if self.account['P2'][1] < 0:
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P2[1]')

    def swap_currencies(self, pair, should_swap_for_A, volume, prices):
        position_a, position_b = self.account[pair]
        if should_swap_for_A:
            self.account[pair] = (position_a + volume, position_b - (volume * prices[pair]))
            self.trades.append((len(self.trades), 'SWAP FOR A', pair, prices[pair], volume))
        else:
            self.account[pair] = (position_a - (volume / prices[pair]), position_b + volume)
            self.trades.append((len(self.trades), 'SWAP FOR B', pair, prices[pair], volume))

    def backtest_pair(self, cointegrated_pair, strategy):
        print(f'cointegrated_pair: {cointegrated_pair}')

        history_remaining, history_remaining_p1, history_remaining_p2 = self.fetch_and_preprocess_data(
            cointegrated_pair, strategy)

        self.account = self.initial_account
        initial_p1 = self.initial_account['P1']
        initial_p2 = self.initial_account['P2']

        self.trades = []
        open_positions = {
            'BUY': {},
            'SELL': {}
        }

        for i in history_remaining.index:
            prices = {
                'P1': history_remaining_p1[i],
                'P2': history_remaining_p2[i]
            }

            signal = strategy.generate_signal(
                {'open_positions': open_positions, 'account': self.account}, prices)

            if signal is None:
                continue

            if 'OPEN' in signal:
                for buy_order in signal['OPEN']['BUY']:
                    buy_pair, buy_volume = buy_order
                    position_a, position_b = self.account[buy_pair]

                    if position_b - (buy_volume * prices[buy_pair]) < 0:
                        # Rebalance now
                        self.swap_currencies(pair=buy_pair, should_swap_for_A=False, volume=abs(position_b - (buy_volume * prices[buy_pair])), prices=prices)
                        position_a, position_b = self.account[buy_pair]

                    self.account[buy_pair] = (
                        position_a + buy_volume, position_b - (buy_volume * prices[buy_pair]))

                    trade_id = str(len(self.trades))
                    open_positions['BUY'][trade_id] = (
                        buy_pair, prices[buy_pair], buy_volume)
                    self.trades.append(
                        (trade_id, 'BUY', buy_pair, prices[buy_pair], buy_volume))
                    self.check_account('OPEN', 'BUY')

                for sell_order in signal['OPEN']['SELL']:
                    sell_pair, sell_volume = sell_order
                    position_a, position_b = self.account[sell_pair]
                    self.account[sell_pair] = (
                        position_a, position_b + (sell_volume * prices[sell_pair]))
                    trade_id = str(len(self.trades))

                    open_positions['SELL'][trade_id] = (
                        sell_pair, prices[sell_pair], sell_volume)
                    self.trades.append(
                        (trade_id, 'SELL', sell_pair, prices[sell_pair], sell_volume))
                    self.check_account('OPEN', 'SELL')

            if 'CLOSE' in signal:
                for buy_id, buy_position in list(signal['CLOSE']['BUY'].items()):
                    buy_pair, bought_price, buy_volume = buy_position
                    position_a, position_b = self.account[buy_pair]

                    if position_a - buy_volume < 0:
                        # Rebalance now
                        self.swap_currencies(pair=buy_pair, should_swap_for_A=True, volume=abs(position_a - buy_volume), prices=prices)
                        position_a, position_b = self.account[buy_pair]

                    self.account[buy_pair] = (
                        position_a - buy_volume, position_b + (prices[buy_pair] * buy_volume))
                    open_positions['BUY'].pop(buy_id)
                    self.check_account('CLOSE', 'BUY')

                for sell_id, sell_position in list(signal['CLOSE']['SELL'].items()):
                    sell_pair, sold_price, sell_volume = sell_position
                    position_a, position_b = self.account[sell_pair]

                    if position_a + (sell_volume * ((sold_price / prices[sell_pair]) - 1)) < 0:
                        # Rebalance Now
                        self.swap_currencies(pair=sell_pair, should_swap_for_A=True, volume=abs(position_a + (sell_volume * ((sold_price / prices[sell_pair]) - 1))), prices=prices)
                        position_a, position_b = self.account[sell_pair]

                    if position_b - (sold_price * sell_volume) < 0:
                        # Rebalance Now
                        self.swap_currencies(pair=sell_pair, should_swap_for_A=False, volume=abs(position_b - (sold_price * sell_volume)), prices=prices)
                        position_a, position_b = self.account[sell_pair]

                    self.account[sell_pair] = (position_a + (sell_volume * (
                        (sold_price / prices[sell_pair]) - 1)), position_b - (sold_price * sell_volume))
                    open_positions['SELL'].pop(sell_id)
                    self.check_account('CLOSE', 'SELL')

        print(f'Exiting Positions Without Exchanging - {self.account}')

        price_p1 = history_remaining_p1[len(history_remaining) - 1]
        position_p1_a, position_p1_b = self.account['P1']
        self.account['P1'] = (position_p1_a - (position_p1_a - initial_p1[0]),
                        position_p1_b + ((position_p1_a - initial_p1[0]) * price_p1))

        price_p2 = history_remaining_p2[len(history_remaining) - 1]
        position_p2_a, position_p2_b = self.account['P2']
        self.account['P2'] = position_p2_a - (position_p2_a - initial_p2[0]), position_p2_b + (
            ((position_p2_a - initial_p2[0]) * price_p2))

        print(f'Exiting Positions After Exchanging - {self.account}')


cointegrated_pairs = load_cointegrated_pairs(
    'historical_data/cointegrated_pairs.pickle')
cointegrated_pair, hedge_ratio = cointegrated_pairs[0]


NUMBER_OF_DAYS_OF_HISTORY = 30
mean_reversion_strategy = Mean_Reversion_Strategy(cointegrated_pair=cointegrated_pair,
                                                  hedge_ratio=hedge_ratio,
                                                  number_of_sds_from_mean=3,
                                                  window_size=days_to_seconds(NUMBER_OF_DAYS_OF_HISTORY))

initial_p1 = (0, 1)
initial_p2 = (0, 1)
initial_account = {
    'P1': initial_p1,
    'P2': initial_p2
}

backtest = Backtest(initial_account)
backtest.backtest_pair(cointegrated_pair, mean_reversion_strategy)
