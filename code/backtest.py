import matplotlib.pyplot as plt
import warnings
import statsmodels.api as sm
from datetime import datetime
import numpy as np
from strategies.mean_reversion import Mean_Reversion_Strategy
from strategies.kalman_filter import Kalman_Filter_Strategy
from tqdm import tqdm
import sys
sys.path.append('./historical_data')
from calculate_cointegrated_pairs import load_cointegrated_pairs
from database_interactions import table_to_df


GAS_USED_BY_SWAP = 150000
GAS_USED_BY_LOAN = 100000
GAS_USED = (2 * GAS_USED_BY_SWAP) + GAS_USED_BY_LOAN


def days_to_seconds(days): return int(days * 24 * 60 * 60)


class Backtest():
    def __init__(self):
        self.trades = None
        self.account_value_history = []
        self.times = []
        self.v_before = 0

    def initialise_account(self, account_size_in_WETH):
        return {
            'T1': 0,
            'T2': 0,
            'WETH': account_size_in_WETH
        }

    def fetch_and_preprocess_data(self, cointegrated_pair, window_size_in_seconds):
        swapped = False
        p2 = cointegrated_pair[1]
        p2_split = p2.split('_')
        if len(p2_split) == 4:
            swapped = True
            p2 = p2_split[0] + '_' + p2_split[1] + '_' + p2_split[2]

        merged = table_to_df(command=f"""
                        SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price as p1_token1_price, p2.id as p2_id, p2.token1_price as p2_token1_price, p1.gas_price_wei as p1_gas_price_wei, p2.gas_price_wei as p2_gas_price_wei
                        FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{p2}" as p2
                        ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price <> 0 AND p2.token1_price <> 0;
                        """, path_to_config='historical_data/database.ini')

        history_arg = merged.loc[merged['period_start_unix'] <
                                 window_size_in_seconds + merged['period_start_unix'][0]]
        history_arg = history_arg.assign(p2_token1_price=(
            1 / history_arg['p2_token1_price']) if swapped else history_arg['p2_token1_price'])

        history_remaining = merged.loc[merged['period_start_unix']
                                       >= window_size_in_seconds + merged['period_start_unix'][0]]
        history_remaining = history_remaining.assign(p2_token1_price=(
            1 / history_remaining['p2_token1_price']) if swapped else history_remaining['p2_token1_price'])

        return history_arg, history_remaining

    def check_account(self, open_or_close, buy_or_sell, should_print_account=True):
        negative_threshold = -1e-10
        if self.account['T1'] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} T1')

        if self.account['T2'] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} T2')

        if self.account['WETH'] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} WETH')

    def get_account_value_in_WETH(self, prices):

        return (self.account['T1'] * prices['P1']) + (self.account['T2'] * prices['P2']) + self.account['WETH']

    def get_apy_at_timestamp(self, timestamp):
        apy1 = self.apy1_df.iloc[(self.apy1_df['timestamp'] - timestamp).abs().argsort()[:1]]['borrow_rate'].iloc[0]
        apy2 = self.apy2_df.iloc[(self.apy2_df['timestamp'] - timestamp).abs().argsort()[:1]]['borrow_rate'].iloc[0]

        return {
            'T1': apy1,
            'T2': apy2
        }

    def backtest_pair(self, cointegrated_pair, strategy, initial_investment_in_weth=100):
        history_arg, history_remaining = self.fetch_and_preprocess_data(
            cointegrated_pair, strategy.window_size_in_seconds)

        token1_symbol = cointegrated_pair[0].split('_')[0]
        token2_symbol = cointegrated_pair[1].split('_')[0] if len(
            cointegrated_pair[1].split('_')) == 3 else cointegrated_pair[1].split('_')[1]

        self.apy1_df = table_to_df(
            command=f"SELECT * FROM {token1_symbol}_borrowing_rates;", path_to_config='historical_data/database.ini')
        
        self.apy2_df = table_to_df(
            command=f"SELECT * FROM {token2_symbol}_borrowing_rates;", path_to_config='historical_data/database.ini')

        strategy.initialise_historical_data(
            history_p1=history_arg['p1_token1_price'], history_p2=history_arg['p2_token1_price'])
        history_remaining_p1, history_remaining_p2 = history_remaining[
            'p1_token1_price'], history_remaining['p2_token1_price']

        self.history_remaining = history_remaining

        initial_account = self.initialise_account(
            account_size_in_WETH=initial_investment_in_weth)
        self.account = initial_account

        self.trades = []
        self.open_positions = {
            'BUY': {},
            'SELL': {}
        }

        def close_buy_position(buy_id, gas_price_in_eth):
            buy_token, bought_price, buy_volume, buy_timestamp = self.open_positions[
                'BUY'][buy_id]
            self.account[buy_token] = self.account[buy_token] - buy_volume
            self.account['WETH'] = self.account['WETH'] + (
                prices[f'P{buy_token[1]}'] * buy_volume) - (GAS_USED_BY_SWAP * gas_price_in_eth)
            self.open_positions['BUY'].pop(buy_id)

        def close_sell_position(sell_id, gas_price_in_eth, apy, curr_timestamp):
            sell_token, sold_price, sell_volume, sell_timestamp = self.open_positions[
                'SELL'][sell_id]

            number_of_hours = (curr_timestamp - sell_timestamp) / (60 * 60)
            hourly_yield = (1 + apy[sell_token])**(1 / (365*24)) - 1

            self.account[sell_token] = self.account[sell_token] + (sell_volume * (
                (sold_price / prices[f'P{sell_token[1]}']) - 1)) - sell_volume * (hourly_yield ** number_of_hours)
            self.account['WETH'] = self.account['WETH'] - (sold_price * sell_volume) - (
                (GAS_USED_BY_SWAP + GAS_USED_BY_LOAN) * gas_price_in_eth)
            self.open_positions['SELL'].pop(sell_id)

        # for i in history_remaining.index:
        for i in tqdm(history_remaining.index):
            prices = {
                'P1': history_remaining_p1[i],
                'P2': history_remaining_p2[i]
            }

            gas_price_in_eth = (
                (history_remaining.loc[i]['p1_gas_price_wei'] + history_remaining.loc[i]['p2_gas_price_wei']) / 2) * 1e-18

            apy = self.get_apy_at_timestamp(history_remaining['period_start_unix'][i])

            signal = strategy.generate_signal(
                {
                    'open_positions': self.open_positions,
                    'account': self.account,
                    'gas_price_in_eth': gas_price_in_eth,
                    'timestamp': history_remaining['period_start_unix'][i],
                    'apy': apy,
                    'vtl': {
                        'T1': 1,
                        'T2': 1
                    }
                }, prices)

            self.account_value_history.append(
                self.get_account_value_in_WETH(prices))
            self.times.append(history_remaining['period_start_unix'][i])

            if signal is None:
                continue

            if 'SWAP' in signal:
                if 'A' in signal['SWAP']:
                    for swap_for_a in signal['SWAP']['A']:
                        swap_token, swap_volume = swap_for_a
                        swap_price = prices[f'P{swap_token[1]}']
                        self.account[swap_token] = self.account[swap_token] + swap_volume
                        self.account['WETH'] = self.account['WETH'] - \
                            (swap_volume * swap_price) - \
                            (GAS_USED_BY_SWAP * gas_price_in_eth)
                        self.trades.append(
                            (str(len(self.trades)), 'SWAP FOR A', swap_token, swap_price, swap_volume, history_remaining['period_start_unix'][i]))
                        self.check_account('SWAP', f'A {swap_token}')

                if 'B' in signal['SWAP']:
                    for swap_for_b in signal['SWAP']['B']:
                        swap_token, swap_volume = swap_for_b
                        swap_price = prices[f'P{swap_token[1]}']
                        self.account[swap_token] = self.account[swap_token] - \
                            (swap_volume / swap_price)
                        self.account['WETH'] = self.account['WETH'] + \
                            swap_volume - (GAS_USED_BY_SWAP * gas_price_in_eth)
                        self.trades.append(
                            (str(len(self.trades)), 'SWAP FOR B', swap_token, swap_price, swap_volume, history_remaining['period_start_unix'][i]))
                        self.check_account('SWAP', f'B {swap_token}')

            if 'OPEN' in signal:
                trades = []
                next_id = len(self.trades)

                for sell_order in signal['OPEN']['SELL']:
                    sell_token, sell_volume = sell_order
                    sell_price = prices[f'P{sell_token[1]}']
                    self.account['WETH'] = self.account['WETH'] + (sell_volume * sell_price) - (
                        (GAS_USED_BY_SWAP + GAS_USED_BY_LOAN) * gas_price_in_eth)

                    self.open_positions['SELL'][str(next_id)] = (
                        sell_token, sell_price, sell_volume, history_remaining['period_start_unix'][i])
                    trades.append(
                        (str(next_id), 'SELL', sell_token, sell_price, sell_volume, history_remaining['period_start_unix'][i]))

                    next_id += 1
                    self.check_account('OPEN', f'SELL {sell_token}')

                for buy_order in signal['OPEN']['BUY']:
                    buy_token, buy_volume = buy_order
                    buy_price = prices[f'P{buy_token[1]}']
                    self.account[buy_token] = self.account[buy_token] + buy_volume
                    self.account['WETH'] = self.account['WETH'] - \
                        (buy_volume * buy_price) - \
                        (GAS_USED_BY_SWAP * gas_price_in_eth)

                    self.open_positions['BUY'][str(next_id)] = (
                        buy_token, buy_price, buy_volume, history_remaining['period_start_unix'][i])
                    trades.append(
                        (str(next_id), 'BUY', buy_token, buy_price, buy_volume, history_remaining['period_start_unix'][i]))

                    next_id += 1
                    self.check_account('OPEN', f'BUY {buy_token}')

                self.trades.append(trades)

            if 'CLOSE' in signal:
                for buy_id, _ in list(signal['CLOSE']['BUY'].items()):
                    close_buy_position(
                        buy_id=buy_id, gas_price_in_eth=gas_price_in_eth)
                    self.check_account('CLOSE', f'BUY {buy_id}')

                for sell_id, _ in list(signal['CLOSE']['SELL'].items()):
                    close_sell_position(
                        sell_id=sell_id, gas_price_in_eth=gas_price_in_eth, apy=apy, curr_timestamp=history_remaining['period_start_unix'][i])
                    self.check_account('CLOSE', f'SELL {sell_id}')
                # self.account_value_history.append(self.get_account_value_in_WETH(cointegrated_pair, history_remaining.loc[i]['period_start_unix'], prices))

        # Close open short positions
        if len(self.open_positions['SELL']) != 0:
            sell_positions = self.open_positions['SELL'].keys()
            for sell_id in list(sell_positions):
                apy = self.get_apy_at_timestamp(history_remaining['period_start_unix'].iloc[-1])
                close_sell_position(sell_id, gas_price_in_eth=gas_price_in_eth, apy=apy,
                                    curr_timestamp=history_remaining['period_start_unix'].iloc[-1])

        account_value_in_weth = self.get_account_value_in_WETH(prices)
        return_percent = ((account_value_in_weth - initial_investment_in_weth)
                          * 100 / initial_investment_in_weth)

        return return_percent


cointegrated_pairs = load_cointegrated_pairs(
    'historical_data/cointegrated_pairs.pickle')

particular_idx = 0
particular_idx = None

num = particular_idx if particular_idx is not None else 0
pairs = cointegrated_pairs[particular_idx:particular_idx +
                           1] if particular_idx is not None else cointegrated_pairs

ps = []

for p in pairs:
    if ((p[0].split('_')[1] == p[1].split('_')[1]) or (len(p[1].split('_')) == 4 and p[0].split('_')[1] == p[1].split('_')[0])) and p[0].split('_')[1] == 'WETH':
        ps.append(p)

# ps = pairs

bad_pairs = []

number_of_sds_from_mean = 1
window_size_in_seconds = days_to_seconds(30)
percent_to_invest = 1.00
initial_investment = 100

for cointegrated_pair in ps:
    try:
        print(num)
        num += 1
        print(f'cointegrated_pair: {cointegrated_pair}')
        mean_reversion_strategy = Mean_Reversion_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                          window_size_in_seconds=window_size_in_seconds,
                                                          percent_to_invest=percent_to_invest)

        backtest_mean_reversion = Backtest()
        return_percent = backtest_mean_reversion.backtest_pair(
            cointegrated_pair, mean_reversion_strategy, initial_investment)
        if return_percent > 0:
            print(
                f"\033[95mMean_Reversion_Strategy\033[0m Total returns \033[92m{return_percent}%\033[0m - trading from {datetime.fromtimestamp(backtest_mean_reversion.times[0])} to {datetime.fromtimestamp(backtest_mean_reversion.times[-1])} with {len(backtest_mean_reversion.trades)} trades")
        else:
            print(
                f"\033[95mMean_Reversion_Strategy\033[0m Total returns \033[91m{return_percent}%\033[0m - trading from {datetime.fromtimestamp(backtest_mean_reversion.times[0])} to {datetime.fromtimestamp(backtest_mean_reversion.times[-1])} with {len(backtest_mean_reversion.trades)} trades")

        # kalman_filter_strategy = Kalman_Filter_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
        #                                                 window_size_in_seconds=window_size_in_seconds,
        #                                                 percent_to_invest=percent_to_invest)

        # backtest_kalman_filter = Backtest()
        # return_percent = backtest_kalman_filter.backtest_pair(
        #     cointegrated_pair, kalman_filter_strategy, initial_investment)
        # if return_percent > 0:
        #     print(
        #         f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[92m{return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")
        # else:
        #     print(
        #         f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[91m{return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")

        # print(*backtest_mean_reversion.trades, sep='\n')

        # plt.plot(backtest_mean_reversion.times, backtest_mean_reversion.account_value_history, label='account')
        # plt.show()

        # _, axarr = plt.subplots(2, sharex=True)
        # axarr[0].plot(backtest_mean_reversion.account_value_history, label='account')
        # axarr[0].legend()
        # axarr[1].plot(backtest_mean_reversion.foo, label='Spread')
        # axarr[1].plot(mean_reversion_strategy.upper_thresholds[:1], label='upper')
        # axarr[1].plot(mean_reversion_strategy.lower_thresholds[:1], label='lower')
        # axarr[1].legend()
        # plt.tight_layout()
        # plt.show()
    # except Exception as e:
    #     bad_pairs.append((cointegrated_pair))
    #     print(e)
    #     print(backtest_mean_reversion.trades[-1])
    #     print()
    finally:
        pass
