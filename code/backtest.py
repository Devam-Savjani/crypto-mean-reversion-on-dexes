import matplotlib.pyplot as plt
import warnings
import numpy as np
from strategies.mean_reversion import Mean_Reversion_Strategy
from strategies.kalman_filter import Kalman_Filter_Strategy
from tqdm import tqdm
import sys
sys.path.append('./historical_data')
from database_interactions import table_to_df
from calculate_cointegrated_pairs import load_cointegrated_pairs


def days_to_seconds(days): return int(days * 24 * 60 * 60)


def conversion_rates(token, timestamp):
    command = f"""
            CREATE OR REPLACE FUNCTION foo()
                    RETURNS TABLE (pool_addr TEXT, t0 TEXT, t1 TEXT, token1_price BIGINT)
                    LANGUAGE plpgsql AS $$
                DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN
                    (SELECT pool_address, token0, token1 FROM liquidity_pools WHERE ((token0 = '{token}' AND token1 = 'USDT') OR (token1 = '{token}' AND token0 = 'USDT')) AND volume_usd >= 10000000000)
                LOOP
                    EXECUTE FORMAT ('SELECT token1_price FROM "%s_%s_%s" WHERE period_start_unix={timestamp}', r.token0, r.token1, r.pool_address) INTO token1_price;
                    pool_addr := r.pool_address;
                    t0 := r.token0;
                    t1 := r.token1;
                    RETURN next;
                END LOOP;
                END $$;

            SELECT * FROM foo();
        """
    conversion_rate_at_time = table_to_df(
        command=command, path_to_config='historical_data/database.ini', should_print=False)

    conversion_rate_at_time.loc[conversion_rate_at_time['t0'] == token,
                                'price'] = conversion_rate_at_time.loc[conversion_rate_at_time['t0'] == token, 'token1_price'].astype(np.float64)
    conversion_rate_at_time.loc[conversion_rate_at_time['t1'] == token, 'price'] = 1 / \
        conversion_rate_at_time.loc[conversion_rate_at_time['t0']
                                    == token, 'token1_price'].astype(np.float64)
    return conversion_rate_at_time


def get_best_conversion_rate_from_USDT(token, timestamp):
    if token == 'USDT':
        return 1
    return min(conversion_rates(token, timestamp)['price'])


def conversion_rate_tokens_to_USDT(token, timestamp):
    if token == 'USDT':
        return 1
    return max(conversion_rates(token, timestamp)['price'])


class Backtest():
    def __init__(self):
        self.trades = None

    def initialise_account(self, cointegrated_pair, account_size_in_USDT, start_timestamp):
        p1_b = cointegrated_pair[0].split('_')[1]
        p2_b = cointegrated_pair[1].split('_')[1]
        rate_p1_b = get_best_conversion_rate_from_USDT(p1_b, start_timestamp)
        initial_account_p1_b = 0.5 * account_size_in_USDT / rate_p1_b
        if p1_b != p2_b:
            rate_p2_b = get_best_conversion_rate_from_USDT(
                p2_b, start_timestamp)
            initial_account_p2_b = 0.5 * account_size_in_USDT / rate_p2_b
        else:
            initial_account_p2_b = initial_account_p1_b

        return {
            'P1': (0, initial_account_p1_b),
            'P2': (0, initial_account_p2_b)
        }

    def fetch_and_preprocess_data(self, cointegrated_pair, strategy):
        swapped = False
        p2 = cointegrated_pair[1]
        p2_split = p2.split('_')
        if len(p2_split) == 4:
            swapped = True
            p2 = p2_split[0] + '_' + p2_split[1] + '_' + p2_split[2]

        merged = table_to_df(command=f"""
                        SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_price as p1_token1_price, p2.id as p2_id, p2.token1_price as p2_token1_price
                        FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{p2}" as p2
                        ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price <> 0 AND p2.token1_price <> 0;
                        """, path_to_config='historical_data/database.ini')

        window_size_in_seconds = strategy.window_size_in_seconds

        history_arg = merged.loc[merged['period_start_unix'] <
                                 window_size_in_seconds + merged['period_start_unix'][0]]
        
        history_arg_p1 = history_arg['p1_token1_price']
        history_arg_p2 = (1 / history_arg['p2_token1_price']) if swapped else history_arg['p2_token1_price']

        strategy.initialise_historical_data(history_p1=history_arg_p1, history_p2=history_arg_p2)

        history_remaining = merged.loc[merged['period_start_unix']
                                       >= window_size_in_seconds + merged['period_start_unix'][0]]
        history_remaining_p1 = history_remaining['p1_token1_price']
        history_remaining_p2 = (1 / history_remaining['p2_token1_price']) if swapped else history_remaining['p2_token1_price']
        return history_remaining, history_remaining_p1, history_remaining_p2

    def rebalance_account(self, prices):
        position_a, position_b = self.account['P1']
        if position_a < 0:
            self.account['P1'] = (
                position_a + abs(position_a), position_b - (abs(position_a) * prices['P1']))
            warnings.warn(f'Boosting P1[0] by {abs(position_a)}')

        if position_b < 0:
            self.account['P1'] = (
                position_a - (abs(position_b) / prices['P1']), position_b + abs(position_b))
            warnings.warn(f'Boosting P1[1] by {abs(position_b)}')

        position_a, position_b = self.account['P2']
        if position_a < 0:
            self.account['P2'] = (
                position_a + abs(position_a), position_b - (abs(position_a) * prices['P2']))
            warnings.warn(f'Boosting P2[0] by {abs(position_a)}')

        if position_b < 0:
            self.account['P2'] = (
                position_a - (abs(position_b) / prices['P2']), position_b + abs(position_b))
            warnings.warn(f'Boosting P2[1] by {abs(position_b)}')

    def check_account(self, open_or_close, buy_or_sell, should_print_account=False):
        negative_threshold = -1e-10
        if self.account['P1'][0] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P1[0]')

        if self.account['P1'][1] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P1[1]')

        if self.account['P2'][0] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P2[0]')

        if self.account['P2'][1] < negative_threshold:
            if should_print_account:
                print(self.account)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} P2[1]')

    def get_account_value_in_USDT(self, pair, account, timestamp, prices):
        p1_b = account['P1'][1] + account['P1'][0] * prices['P1']
        p2_b = account['P2'][1] + account['P2'][0] * prices['P2']
        p1_tokenb = pair[0].split('_')[1]
        p2_tokenb = pair[1].split('_')[1]
        conversion_rate_p1_b = conversion_rate_tokens_to_USDT(
            p1_tokenb, timestamp)
        p1_usdt = conversion_rate_p1_b * p1_b
        p2_usdt = (conversion_rate_p1_b if p1_tokenb ==
                   p2_tokenb else conversion_rate_tokens_to_USDT(p2_tokenb, timestamp)) * p2_b
        return p1_usdt + p2_usdt

    def backtest_pair(self, cointegrated_pair, strategy, initial_investment_in_USDT=100):
        history_remaining, history_remaining_p1, history_remaining_p2 = self.fetch_and_preprocess_data(
            cointegrated_pair, strategy)

        initial_account = self.initialise_account(
            cointegrated_pair=cointegrated_pair, account_size_in_USDT=initial_investment_in_USDT, start_timestamp=history_remaining['period_start_unix'].iloc[0])
        self.account = initial_account

        initial_p1 = initial_account['P1']
        initial_p2 = initial_account['P2']

        self.trades = []
        self.open_positions = {
            'BUY': {},
            'SELL': {}
        }

        def close_buy_position(buy_id):
            buy_pair, bought_price, buy_volume = self.open_positions['BUY'][buy_id]
            position_a, position_b = self.account[buy_pair]

            self.account[buy_pair] = (
                position_a - buy_volume, position_b + (prices[buy_pair] * buy_volume))
            self.open_positions['BUY'].pop(buy_id)
        
        def close_sell_position(sell_id):
            sell_pair, sold_price, sell_volume = self.open_positions['SELL'][sell_id]
            position_a, position_b = self.account[sell_pair]

            self.account[sell_pair] = (position_a + (sell_volume * (
                (sold_price / prices[sell_pair]) - 1)), position_b - (sold_price * sell_volume))
            self.open_positions['SELL'].pop(sell_id)


        for i in history_remaining.index:
        # for i in tqdm(history_remaining.index):
            prices = {
                'P1': history_remaining_p1[i],
                'P2': history_remaining_p2[i]
            }

            signal = strategy.generate_signal(
                {'open_positions': self.open_positions, 'account': self.account}, prices)

            if signal is None:
                continue

            if 'SWAP' in signal:
                if 'A' in signal['SWAP']:
                    for swap_for_a in signal['SWAP']['A']:
                        swap_pair, swap_volume = swap_for_a
                        position_a, position_b = self.account[swap_pair]
                        self.account[swap_pair] = (position_a + swap_volume,
                                            position_b - (swap_volume * prices[swap_pair]))
                        self.trades.append(
                            (str(len(self.trades)), 'SWAP FOR A', swap_pair, prices[swap_pair], swap_volume))

                
                if 'B' in signal['SWAP']:
                    for swap_for_b in signal['SWAP']['B']:
                        swap_pair, swap_volume = swap_for_b
                        position_a, position_b = self.account[swap_pair]
                        self.account[swap_pair] = (
                            position_a - (swap_volume / prices[swap_pair]), position_b + swap_volume)
                        self.trades.append(
                            (str(len(self.trades)), 'SWAP FOR B', swap_pair, prices[swap_pair], swap_volume))

            if 'OPEN' in signal:
                for buy_order in signal['OPEN']['BUY']:
                    buy_pair, buy_volume = buy_order
                    position_a, position_b = self.account[buy_pair]
                    self.account[buy_pair] = (
                        position_a + buy_volume, position_b - (buy_volume * prices[buy_pair]))

                    trade_id = str(len(self.trades))
                    self.open_positions['BUY'][trade_id] = (
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

                    self.open_positions['SELL'][trade_id] = (
                        sell_pair, prices[sell_pair], sell_volume)
                    self.trades.append(
                        (trade_id, 'SELL', sell_pair, prices[sell_pair], sell_volume))
                    self.check_account('OPEN', 'SELL')

            if 'CLOSE' in signal:
                for buy_id, _ in list(signal['CLOSE']['BUY'].items()):
                    close_buy_position(buy_id=buy_id)
                    self.check_account('CLOSE', 'BUY')

                for sell_id, _ in list(signal['CLOSE']['SELL'].items()):
                    close_sell_position(sell_id)
                    self.check_account('CLOSE', 'SELL')

        if len(self.open_positions['BUY']) != 0 or len(self.open_positions['SELL']) != 0:
            sell_positions = self.open_positions['SELL'].keys()
            for sell_id in list(sell_positions):
                close_sell_position(sell_id)

        price_p1 = history_remaining_p1[len(history_remaining) - 1]
        position_p1_a, position_p1_b = self.account['P1']
        self.account['P1'] = (position_p1_a - (position_p1_a - initial_p1[0]),
                              position_p1_b + ((position_p1_a - initial_p1[0]) * price_p1))

        price_p2 = history_remaining_p2[len(history_remaining) - 1]
        position_p2_a, position_p2_b = self.account['P2']
        self.account['P2'] = (position_p2_a - (position_p2_a - initial_p2[0]), position_p2_b + (
            ((position_p2_a - initial_p2[0]) * price_p2)))

        p1_b = cointegrated_pair[0].split('_')[1]
        p2_b = cointegrated_pair[1].split('_')[1]
        end_timestamp = history_remaining['period_start_unix'].iloc[-1]
        conversion_rate_p1_b = conversion_rate_tokens_to_USDT(
            p1_b, end_timestamp)
        p1_usdt = conversion_rate_p1_b * self.account['P1'][1]
        p2_usdt = (conversion_rate_p1_b if p1_b == p2_b else conversion_rate_tokens_to_USDT(
            p2_b, end_timestamp)) * self.account['P2'][1]
        total_usdt = p1_usdt + p2_usdt
        return_percent = ((total_usdt - initial_investment_in_USDT)
                          * 100 / initial_investment_in_USDT)

        return return_percent

cointegrated_pairs = load_cointegrated_pairs(
    'historical_data/cointegrated_pairs.pickle')

particular_idx = 14
particular_idx = None

num = particular_idx if particular_idx is not None else 0
pairs = cointegrated_pairs[particular_idx:particular_idx +
                           1] if particular_idx is not None else cointegrated_pairs

bad_pairs = []

for cointegrated_pair in pairs:
    try:
        print(num)
        num += 1
        print(f'cointegrated_pair: {cointegrated_pair}')
        mean_reversion_strategy = Mean_Reversion_Strategy(cointegrated_pair=cointegrated_pair,
                                                          number_of_sds_from_mean=1,
                                                          window_size_in_seconds=days_to_seconds(30),
                                                          percent_to_invest=1)

        backtest_mean_reversion = Backtest()
        return_percent = backtest_mean_reversion.backtest_pair(cointegrated_pair, mean_reversion_strategy, 100)
        if return_percent > 0:
            print(f"\033[95mMean_Reversion_Strategy\033[0m Total returns \033[92m{return_percent}%\033[0m with {len(backtest_mean_reversion.trades)} trades")
        else:
            print(f"\033[95mMean_Reversion_Strategy\033[0m Total returns \033[91m{return_percent}%\033[0m with {len(backtest_mean_reversion.trades)} trades")

        kalman_filter_strategy = Kalman_Filter_Strategy(cointegrated_pair=cointegrated_pair,
                                                        number_of_sds_from_mean=1,
                                                        window_size_in_seconds=days_to_seconds(30),
                                                        percent_to_invest=1)

        backtest_kalman_filter = Backtest()
        return_percent = backtest_kalman_filter.backtest_pair(cointegrated_pair, kalman_filter_strategy, 100)
        if return_percent > 0:
            print(f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[92m{return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")
        else:
            print(f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[91m{return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")

        print()
    except Exception as e:
        bad_pairs.append((cointegrated_pair))
        print(e)
        print()
