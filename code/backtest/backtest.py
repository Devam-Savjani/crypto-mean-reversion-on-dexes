import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
from calculate_cointegrated_pairs import load_cointegrated_pairs
import matplotlib.pyplot as plt
import statsmodels.api as sm
from datetime import datetime, date
import calendar
import numpy as np
import pandas as pd
from strategies.constant_hr_strategy import Constant_Hedge_Ratio_Strategy
from strategies.kalman_filter_strategy import Kalman_Filter_Strategy
from strategies.sliding_window_strategy import Sliding_Window_Strategy
from strategies.lagged_strategy import Lagged_Strategy
from strategies.granger_causality_strategy import Granger_Causality_Strategy
from tqdm import tqdm
from utils.database_interactions import table_to_df
from utils.constants import GAS_USED_BY_SWAP, GAS_USED_BY_BUYING_ETH, GAS_USED_BY_DEPOSITING_COLLATERAL, GAS_USED_BY_WITHDRAWING_COLLATERAL, GAS_USED_BY_BORROW, GAS_USED_BY_REPAY, GAS_USED_BY_OPEN_BUY_AND_SELL_POSITION, GAS_USED_BY_CLOSE_BUY_AND_SELL_POSITION
from utils.helpers import days_to_seconds

class Backtest():
    def __init__(self):
        self.trades = None
        self.account_value_history = []
        self.times = []

    def initialise_account(self, account_size_in_WETH):
        return {
            'T1': 0,
            'T2': 0,
            'WETH': account_size_in_WETH - min(account_size_in_WETH * 0.1, 10),
            'collateral_WETH': 0,
            'ETH': min(account_size_in_WETH * 0.1, 10)
        }

    def fetch_and_preprocess_data(self, cointegrated_pair, window_size_in_seconds, start_timestamp):
        merged = table_to_df(command=f"""
                        SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.{'token1_price' if cointegrated_pair[0].split('_')[1] == 'WETH' else 'token0_price'} as p1_token_price, p2.id as p2_id, p2.{'token1_price' if cointegrated_pair[1].split('_')[1] == 'WETH' else 'token0_price'} as p2_token_price
                        FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{cointegrated_pair[1]}" as p2 ON p1.period_start_unix = p2.period_start_unix
                        WHERE p1.token1_price <> 0 AND p2.token1_price <> 0 {f'AND p1.period_start_unix >= {start_timestamp - window_size_in_seconds}' if start_timestamp is not None else ''} ORDER BY p1.period_start_unix;
                        """, path_to_config='../utils/database.ini')

        self.history_p1 = merged['p1_token_price']
        self.history_p2 = merged['p2_token_price']
        self.history_times = merged['period_start_unix']

        history_arg = merged.loc[merged['period_start_unix'] <
                                 window_size_in_seconds + merged['period_start_unix'][0]]

        history_remaining = merged.loc[merged['period_start_unix']
                                       >= window_size_in_seconds + merged['period_start_unix'][0]]

        return history_arg, history_remaining

    def get_uniswap_fee(self, cointegrated_pair):
        pool_addr1 = cointegrated_pair[0].split('_')[2]
        pool_addr2 = cointegrated_pair[1].split('_')[2]
        pools = table_to_df(command=f"""
                        SELECT * FROM liquidity_pools where pool_address='{pool_addr1}' or pool_address='{pool_addr2}';
                        """, path_to_config='../utils/database.ini')

        return pools[pools['pool_address'] == pool_addr1]['fee_tier'].iloc[0] * 1e-6, pools[pools['pool_address'] == pool_addr2]['fee_tier'].iloc[0] * 1e-6

    def check_account(self, open_or_close, buy_or_sell, should_print_account=True, signal=None):
        negative_threshold = -1e-10
        if self.account['T1'] < negative_threshold:
            if should_print_account:
                print(self.account, signal)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} T1')

        if self.account['T2'] < negative_threshold:
            if should_print_account:
                print(self.account, signal)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} T2')

        if self.account['WETH'] < negative_threshold:
            if should_print_account:
                print(self.account, signal)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} WETH')

        if self.account['ETH'] < negative_threshold:
            if should_print_account:
                print(self.account, signal)
            raise Exception(
                f'Account balace goes below 0 - {open_or_close} {buy_or_sell} ETH')

    def get_account_value_in_WETH(self, prices):
        return (self.account['T1'] * prices['P1']) + (self.account['T2'] * prices['P2']) + self.account['WETH'] + self.account['ETH']

    def get_apy_at_timestamp(self, start_timestamp, end_timestamp):
        return {
            'T1': self.aave1_df.loc[(self.aave1_df['timestamp'] >= start_timestamp) & (self.aave1_df['timestamp'] <= end_timestamp)][['timestamp', 'borrow_rate']],
            'T2': self.aave2_df.loc[(self.aave2_df['timestamp'] >= start_timestamp) & (self.aave2_df['timestamp'] <= end_timestamp)][['timestamp', 'borrow_rate']]
        }

    def backtest_pair(self, cointegrated_pair, strategy, initial_investment_in_weth, history_size, start_timestamp=None):
        history_arg, history_remaining = self.fetch_and_preprocess_data(
            cointegrated_pair, history_size, start_timestamp)

        cointegrated_pair_0_split = cointegrated_pair[0].split('_')
        cointegrated_pair_1_split = cointegrated_pair[1].split('_')

        token1_symbol = cointegrated_pair_0_split[0] if cointegrated_pair_0_split[
            1] == 'WETH' else cointegrated_pair_0_split[1]
        token2_symbol = cointegrated_pair_1_split[0] if cointegrated_pair_1_split[
            1] == 'WETH' else cointegrated_pair_1_split[1]

        self.aave1_df = table_to_df(
            command=f"SELECT * FROM {token1_symbol}_borrowing_rates ORDER BY timestamp;", path_to_config='../utils/database.ini')

        self.aave2_df = table_to_df(
            command=f"SELECT * FROM {token2_symbol}_borrowing_rates ORDER BY timestamp;", path_to_config='../utils/database.ini')

        self.gas_prices_df = table_to_df(
            command=f"SELECT * FROM gas_prices ORDER BY timestamp;", path_to_config='../utils/database.ini')

        swap_fee1, swap_fee2 = self.get_uniswap_fee(cointegrated_pair)
        swap_fees = {
            'T1': swap_fee1,
            'T2': swap_fee2
        }

        strategy.initialise_historical_data(
            history_p1=history_arg['p1_token_price'], history_p2=history_arg['p2_token_price'])
        history_remaining_p1, history_remaining_p2 = history_remaining[
            'p1_token_price'], history_remaining['p2_token_price']

        self.history_remaining_p1 = history_remaining_p1
        self.history_remaining_p2 = history_remaining_p2

        self.history_remaining = history_remaining

        initial_account = self.initialise_account(
            account_size_in_WETH=initial_investment_in_weth)
        self.account = initial_account

        self.trades = []
        self.open_positions = {
            'BUY': {},
            'SELL': {}
        }

        def close_buy_position(buy_id):
            buy_token, bought_price, buy_volume, buy_timestamp = self.open_positions[
                'BUY'][buy_id]

            # actual_volume_bought is calculated as from the initial swap a cut is taken as fees
            actual_volume_bought = buy_volume * (1 - swap_fees[buy_token])

            # Swap Token back to WETH
            self.account[buy_token] = self.account[buy_token] - \
                actual_volume_bought
            self.account['WETH'] = self.account['WETH'] + (prices[f'P{buy_token[1]}'] * (
                actual_volume_bought * (1 - swap_fees[buy_token])))

            self.open_positions['BUY'].pop(buy_id)

        def close_sell_position(sell_id, apy):
            sell_token, sold_price, sell_volume, sell_timestamp = self.open_positions[
                'SELL'][sell_id]

            # Swap WETH back to Token
            volume_required_to_return = sell_volume
            previous_timestamp = sell_timestamp

            for apy_idx in apy[sell_token].index:
                local_apy = apy[sell_token].loc[apy_idx]['borrow_rate']
                number_of_seconds = apy[sell_token].loc[apy_idx]['timestamp'] - \
                    previous_timestamp
                secondly_yield = (1 + local_apy)**(1 / (365*24*60*60))
                volume_required_to_return *= secondly_yield ** number_of_seconds
                previous_timestamp = apy[sell_token].loc[apy_idx]['timestamp']

            self.account['WETH'] = self.account['WETH'] - \
                (sold_price * volume_required_to_return /
                 (1 - swap_fees[sell_token]))
            self.account[sell_token] = self.account[sell_token] + \
                volume_required_to_return

            # Return Borrowed Tokens
            self.account[sell_token] = self.account[sell_token] - \
                volume_required_to_return

            # Return Collatoral to WETH
            self.account['WETH'] = self.account['WETH'] + \
                self.account['collateral_WETH']
            self.account['collateral_WETH'] = 0
            self.open_positions['SELL'].pop(sell_id)

        weth_borrowing_rates = table_to_df(
            command=f"SELECT * FROM weth_borrowing_rates ORDER BY timestamp;", path_to_config='../utils/database.ini')

        ltv_eth = weth_borrowing_rates['ltv'].iloc[0]
        liquidation_threshold = weth_borrowing_rates['liquidation_threshold'].iloc[0]

        i = history_remaining.index[0]
        prices = {
                'P1': history_remaining_p1[i],
                'P2': history_remaining_p2[i]
            }
        timestamp = history_remaining['period_start_unix'][i]
        self.account_value_history.append(
            self.get_account_value_in_WETH(prices))
        self.times.append(timestamp)

        # for i in tqdm(history_remaining.index):
        for i in history_remaining.index:
            prices = {
                'P1': history_remaining_p1[i],
                'P2': history_remaining_p2[i]
            }
            timestamp = history_remaining['period_start_unix'][i]

            gas_price_in_eth = (
                self.gas_prices_df.loc[self.gas_prices_df['timestamp'] == timestamp]['gas_price_wei'].iloc[0]) * 1e-18

            if len(self.open_positions['SELL']) > 0:
                apy = self.get_apy_at_timestamp(
                    list(self.open_positions['SELL'].values())[0][3], timestamp)
            else:
                apy = None

            signal = strategy.generate_signal(
                {
                    'open_positions': self.open_positions,
                    'account': self.account,
                    'gas_price_in_eth': gas_price_in_eth,
                    'timestamp': timestamp,
                    'apy': apy,
                    'ltv_eth': ltv_eth,
                    'liquidation_threshold': liquidation_threshold,
                    'uniswap_fees': swap_fees
                }, prices)

            # self.account_value_history.append(
            #     self.get_account_value_in_WETH(prices))
            # self.times.append(timestamp)

            if len([order[0] for order in signal if order[0] == 'OPEN']) > 0:
                self.account_value_history.append(
                    self.get_account_value_in_WETH(prices))
                self.times.append(timestamp)

            for order in signal:
                order_type = order[0]

                if order_type == 'BUY ETH':
                    amount_to_swap = self.account['WETH'] * order[1]
                    self.account['WETH'] = self.account['WETH'] - \
                        amount_to_swap
                    self.account['ETH'] = self.account['ETH'] + \
                        amount_to_swap - \
                        (GAS_USED_BY_BUYING_ETH * gas_price_in_eth)

                elif order_type == 'DEPOSIT':
                    deposit_amount = order[1]
                    self.account['WETH'] = self.account['WETH'] - \
                        deposit_amount
                    self.account['collateral_WETH'] = self.account['collateral_WETH'] + deposit_amount
                    self.account['ETH'] = self.account['ETH'] - \
                        (GAS_USED_BY_DEPOSITING_COLLATERAL * gas_price_in_eth)
                    self.check_account('DEPOSIT', f'WETH', signal=signal)

                if order_type == 'WITHDRAW':
                    withdraw_amount = order[1]
                    self.account['WETH'] = self.account['WETH'] + \
                        withdraw_amount
                    self.account['collateral_WETH'] = self.account['collateral_WETH'] - \
                        withdraw_amount
                    self.account['ETH'] = self.account['ETH'] - \
                        (GAS_USED_BY_WITHDRAWING_COLLATERAL * gas_price_in_eth)
                    self.check_account('WITHDRAW', f'WETH', signal=signal)

                elif order_type == 'SWAP':
                    is_for_token0, swaps = order[1]
                    if is_for_token0:
                        for swap_for_token0 in swaps:
                            swap_token, swap_volume = swap_for_token0
                            swap_price = prices[f'P{swap_token[1]}']

                            self.account['WETH'] = self.account['WETH'] - \
                                (swap_volume * swap_price)
                            self.account[swap_token] = self.account[swap_token] + \
                                (swap_volume * (1 - swap_fees[swap_token]))

                            # Deduct Gas fee from swapping
                            self.account['ETH'] = self.account['ETH'] - \
                                (GAS_USED_BY_SWAP * gas_price_in_eth)

                            self.trades.append(
                                (str(len(self.trades)), 'SWAP FOR A', swap_token, swap_price, swap_volume, timestamp))
                            self.check_account(
                                'SWAP', f'A {swap_token}', signal=signal)

                    else:
                        for swap_for_token1 in swaps:
                            swap_token, swap_volume = swap_for_token1
                            swap_price = prices[f'P{swap_token[1]}']
                            self.account[swap_token] = self.account[swap_token] - \
                                (swap_volume / swap_price)
                            self.account['WETH'] = self.account['WETH'] + \
                                (swap_volume * (1 - swap_fees[swap_token]))
                            # Deduct Gas fee from swapping
                            self.account['ETH'] = self.account['ETH'] - \
                                (GAS_USED_BY_SWAP * gas_price_in_eth)
                            self.trades.append(
                                (str(len(self.trades)), 'SWAP FOR B', swap_token, swap_price, swap_volume, timestamp))
                            self.check_account(
                                'SWAP', f'B {swap_token}', signal=signal)

                elif order_type == 'CLOSE':
                    position_type, position_ids = order[1]
                    if position_type == 'SELL':
                        for sell_id in position_ids:
                            close_sell_position(sell_id=sell_id, apy=apy)
                            self.check_account(
                                'CLOSE', f'SELL {sell_id}', signal=signal)

                    if position_type == 'BUY':
                        for buy_id in position_ids:
                            close_buy_position(buy_id=buy_id)
                            self.check_account(
                                'CLOSE', f'BUY {buy_id}', signal=signal)

                elif order_type == 'OPEN':
                    open_type, token, volume = order[1]
                    next_id = len(self.trades)
                    if open_type == 'BUY':
                        buy_price = prices[f'P{token[1]}']
                        self.account['WETH'] = self.account['WETH'] - \
                            (volume * buy_price)
                        self.account[token] = self.account[token] + \
                            (volume * (1 - swap_fees[token]))

                        self.open_positions['BUY'][str(next_id)] = (
                            token, buy_price, volume, timestamp)
                        self.trades.append(
                            (str(next_id), 'BUY', token, buy_price, volume, timestamp))

                        self.check_account(
                            'OPEN', f'BUY {token}', signal=signal)
                    elif open_type == 'SELL':
                        sell_price = prices[f'P{token[1]}']

                        # Borrow Token from Aave
                        amount_to_move_to_collateral_WETH = (
                            (volume * sell_price) / ltv_eth)
                        self.account[token] = self.account[token] + volume
                        self.account['WETH'] = self.account['WETH'] - \
                            amount_to_move_to_collateral_WETH
                        self.account['collateral_WETH'] = self.account['collateral_WETH'] + \
                            amount_to_move_to_collateral_WETH

                        # Swap borrowed tokens to WETH
                        self.account[token] = self.account[token] - volume
                        self.account['WETH'] = self.account['WETH'] + \
                            (volume * (1 - swap_fees[token]) * sell_price)

                        self.open_positions['SELL'][str(next_id)] = (
                            token, sell_price, volume, timestamp)
                        self.trades.append(
                            (str(next_id), 'SELL', token, sell_price, volume, timestamp))

                        self.check_account(
                            'OPEN', f'SELL {token}', signal=signal)

            if len([order[0] for order in signal if order[0] == 'OPEN']) == 2:
                open_positions = self.trades[-2:]
                self.trades = self.trades[:-2]
                self.trades.append(open_positions)
                if strategy.should_batch_trade:
                    self.account['ETH'] = self.account['ETH'] - (GAS_USED_BY_OPEN_BUY_AND_SELL_POSITION * gas_price_in_eth)
                else:
                    self.account['ETH'] = self.account['ETH'] - ((GAS_USED_BY_SWAP + GAS_USED_BY_SWAP + GAS_USED_BY_BORROW) * gas_price_in_eth)
                
                self.check_account('OPEN', f'BUY and SELL position', signal=signal)

            if len([order[0] for order in signal if order[0] == 'CLOSE']) == 2:
                if strategy.should_batch_trade:
                    self.account['ETH'] = self.account['ETH'] - (GAS_USED_BY_CLOSE_BUY_AND_SELL_POSITION * gas_price_in_eth)
                else:
                    self.account['ETH'] = self.account['ETH'] - ((GAS_USED_BY_SWAP + GAS_USED_BY_SWAP + GAS_USED_BY_REPAY) * gas_price_in_eth)

                # self.account_value_history.append(
                #     self.get_account_value_in_WETH(prices))
                # self.times.append(timestamp)
                self.check_account('CLOSE', f'BUY and SELL position', signal=signal)


            for sell_trade in self.open_positions['SELL'].values():
                sell_token, sold_price, sell_volume, _ = sell_trade
                current_token_price = prices[f'P{sell_token[1]}']
                curr_value_of_loan_pct = (
                    sell_volume * current_token_price) / self.account['collateral_WETH']
                if round(curr_value_of_loan_pct, 4) > liquidation_threshold:
                    raise Exception(f'Short position liquidated')

        # Close open short positions
        if len(self.open_positions['SELL']) != 0:
            sell_positions = self.open_positions['SELL'].keys()
            for sell_id in list(sell_positions):
                apy = self.get_apy_at_timestamp(
                    history_remaining['period_start_unix'].iloc[-2], history_remaining['period_start_unix'].iloc[-1])
                close_sell_position(sell_id, apy=apy)
                self.account['ETH'] = self.account['ETH'] - ((GAS_USED_BY_SWAP + GAS_USED_BY_REPAY) * gas_price_in_eth)

        account_value_in_weth = self.get_account_value_in_WETH(prices)
        return_percent = ((account_value_in_weth - initial_investment_in_weth)
                          * 100 / initial_investment_in_weth)
        self.account_value_history.append(
                    self.get_account_value_in_WETH(prices))
        self.times.append(timestamp)

        return return_percent


cointegrated_pairs = load_cointegrated_pairs('../utils/cointegrated_pairs.pickle')

cointegrated_pairs = [('USDC_WETH_0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
                    ('USDC_WETH_0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
                    ('WETH_USDT_0x11b815efb8f581194ae79006d24e0d814b7697f6', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
                    ('WETH_USDT_0x4e68ccd3e89f51c3074ca5072bbac773960dfa36', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
                    ('DAI_WETH_0x60594a405d53811d3bc4766596efd80fd545a270', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
                    ('DAI_WETH_0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
                    ('USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f', 'WETH_USDT_0xc5af84701f98fa483ece78af83f11b6c38aca71d')]

print(*cointegrated_pairs, sep="\n")

particular_idx = 0
particular_idx = None

num = particular_idx if particular_idx is not None else 0
pairs = cointegrated_pairs[particular_idx:particular_idx +
                           1] if particular_idx is not None else cointegrated_pairs

bad_pairs = []

start_timestamp = calendar.timegm(date.fromisoformat('2022-06-09').timetuple())
start_timestamp = None

betas = pd.DataFrame({}, columns=["Constant", "SW", "Lagged", "GC", "KF"])
sharpe_ratios = pd.DataFrame({}, columns=["Constant", "SW", "Lagged", "GC", "KF"])
returns = pd.DataFrame({}, columns=["Constant", "SW", "Lagged", "GC", "KF"])

use_same_params = False

if use_same_params:
    percent_to_invest = 1
    number_of_sds_from_mean = 1.75
    gas_price_threshold = 1.29e-07
    window_size_in_seconds = days_to_seconds(30)
    history_size = window_size_in_seconds
    should_batch_trade = True
    initial_investment = 100

results_for_table = []
results_for_plot = []
results_for_beta = []
results_for_sr = []

for idx, cointegrated_pair in enumerate(pairs):
    print(idx)
    num += 1
    print(f'cointegrated_pair: {cointegrated_pair}')

    try:
        if not use_same_params:
            percent_to_invest = 1
            number_of_sds_from_mean = 3
            gas_price_threshold = 6.36e-08
            window_size_in_seconds = days_to_seconds(60)
            history_size = window_size_in_seconds
            should_batch_trade = True
            initial_investment = 100

        constant_hr_strategy = Constant_Hedge_Ratio_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=percent_to_invest,
                                                        gas_price_threshold=gas_price_threshold,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5,
                                                        should_batch_trade=should_batch_trade)

        backtest_constant_hr = Backtest()
        constant_return_percent = backtest_constant_hr.backtest_pair(
            cointegrated_pair, constant_hr_strategy, initial_investment, history_size, start_timestamp)
    except Exception as e:
        constant_return_percent = -100    

    if constant_return_percent > 0:
        print(
            f"\033[95mConstant_Hedge_Ratio_Strategy\033[0m Total returns \033[92m{constant_return_percent}%\033[0m - trading from {datetime.fromtimestamp(backtest_constant_hr.times[0])} to {datetime.fromtimestamp(backtest_constant_hr.times[-1])} with {len(backtest_constant_hr.trades)} trades")
    else:
        print(
            f"\033[95mConstant_Hedge_Ratio_Strategy\033[0m Total returns \033[91m{constant_return_percent}%\033[0m - trading from {datetime.fromtimestamp(backtest_constant_hr.times[0])} to {datetime.fromtimestamp(backtest_constant_hr.times[-1])} with {len(backtest_constant_hr.trades)} trades")
        
    try:
        if not use_same_params:
            number_of_sds_from_mean = 2
            gas_price_threshold = 8.83e-08
            percent_to_invest = 1
            window_size_in_seconds = days_to_seconds(60)
            history_size = window_size_in_seconds
            should_batch_trade = True
            initial_investment = 100

        sliding_window_strategy = Sliding_Window_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=percent_to_invest,
                                                        gas_price_threshold=gas_price_threshold,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5,
                                                        should_batch_trade=should_batch_trade)

        backtest_sliding_window = Backtest()
        sw_return_percent = backtest_sliding_window.backtest_pair(
            cointegrated_pair, sliding_window_strategy, initial_investment, history_size, start_timestamp)
    except Exception as e:
        sw_return_percent = -100

    if sw_return_percent > 0:
        print(
            f"\033[94mSliding_Window_Strategy\033[0m Total returns \033[92m{sw_return_percent}%\033[0m with {len(backtest_sliding_window.trades)} trades")
    else:
        print(
            f"\033[94mSliding_Window_Strategy\033[0m Total returns \033[91m{sw_return_percent}%\033[0m with {len(backtest_sliding_window.trades)} trades")
    
    try:
        if not use_same_params:
            percent_to_invest = 1
            gas_price_threshold = 2.13e-05
            number_of_sds_from_mean = 0.5
            window_size_in_seconds = days_to_seconds(30)
            history_size = window_size_in_seconds
            should_batch_trade = True
            initial_investment = 100

        kalman_filter_strategy = Kalman_Filter_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=percent_to_invest,
                                                        gas_price_threshold=gas_price_threshold,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5,
                                                        should_batch_trade=should_batch_trade)

        backtest_kalman_filter = Backtest()
        kf_return_percent = backtest_kalman_filter.backtest_pair(
            cointegrated_pair, kalman_filter_strategy, initial_investment, history_size, start_timestamp)
    except Exception as e:
        kf_return_percent = -100

    if kf_return_percent > 0:
        print(
            f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[92m{kf_return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")
    else:
        print(
            f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[91m{kf_return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")
            
    try:
        if not use_same_params:
            percent_to_invest = 1
            number_of_sds_from_mean = 2
            gas_price_threshold = 1.29e-07
            window_size_in_seconds = days_to_seconds(60)
            history_size = window_size_in_seconds
            should_batch_trade = True
            initial_investment = 100

        lagged_strategy = Lagged_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=percent_to_invest,
                                                        gas_price_threshold=gas_price_threshold,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5,
                                                        should_batch_trade=should_batch_trade,
                                                        lag=1)

        backtest_lagged = Backtest()
        lagged_return_percent = backtest_lagged.backtest_pair(
            cointegrated_pair, lagged_strategy, initial_investment, history_size, start_timestamp)
    except Exception as e:
        lagged_return_percent = -100
    
    if lagged_return_percent > 0:
        print(
            f"\033[33mLagged_Strategy\033[0m Total returns \033[92m{lagged_return_percent}%\033[0m with {len(backtest_lagged.trades)} trades")
    else:
        print(
            f"\033[33mLagged_Strategy\033[0m Total returns \033[91m{lagged_return_percent}%\033[0m with {len(backtest_lagged.trades)} trades")
    
    try:
        if not use_same_params:
            percent_to_invest = 1
            number_of_sds_from_mean = 1.5
            gas_price_threshold = 2.13e-05
            window_size_in_seconds = days_to_seconds(60)
            history_size = window_size_in_seconds
            should_batch_trade = True
            initial_investment = 100

        gc_strategy = Granger_Causality_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=percent_to_invest,
                                                        gas_price_threshold=gas_price_threshold,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5,
                                                        should_batch_trade=should_batch_trade)

        backtest_gc = Backtest()
        gc_return_percent = backtest_gc.backtest_pair(
            cointegrated_pair, gc_strategy, initial_investment, history_size, start_timestamp)
    except Exception as e:
        gc_return_percent = -100

    if gc_return_percent > 0:
        print(
            f"\033[90mGranger_Causality_Strategy\033[0m Total returns \033[92m{gc_return_percent}%\033[0m with {len(backtest_gc.trades)} trades")
    else:
        print(
            f"\033[90mGranger_Causality_Strategy\033[0m Total returns \033[91m{gc_return_percent}%\033[0m with {len(backtest_gc.trades)} trades")

    # apy_adj = ((backtest_constant_hr.times[-1] - backtest_constant_hr.times[0]) / (365*24*60*60))
    # foo = [constant_return_percent, sw_return_percent, lagged_return_percent, gc_return_percent, kf_return_percent]
    # returns.loc[idx] = [((100 * (((r / 100) + 1)**(1 / apy_adj))) - 100) for r in foo]
    # returns.loc[idx] = foo

    #### Prints for Tables
    # rounding_num = 2
    # constant_return_percent_str = f"\\textcolor{{{'green' if constant_return_percent > 0 else 'red'}}}{{{round(returns.loc[idx][0], rounding_num)}}}"
    # sw_return_percent_str = f"\\textcolor{{{'green' if sw_return_percent > 0 else 'red'}}}{{{round(returns.loc[idx][1], rounding_num)}}}"
    # lagged_return_percent_str = f"\\textcolor{{{'green' if lagged_return_percent > 0 else 'red'}}}{{{round(returns.loc[idx][2], rounding_num)}}}"
    # gc_return_percent_str = f"\\textcolor{{{'green' if gc_return_percent > 0 else 'red'}}}{{{round(returns.loc[idx][3], rounding_num)}}}"
    # kf_return_percent_str = f"\\textcolor{{{'green' if kf_return_percent > 0 else 'red'}}}{{{round(returns.loc[idx][4], rounding_num)}}}"
    # to_print = f"& {idx} & {constant_return_percent_str} & {len(backtest_constant_hr.trades)} & {sw_return_percent_str} & {len(backtest_sliding_window.trades)} & {lagged_return_percent_str} & {len(backtest_lagged.trades)} & {gc_return_percent_str} & {len(backtest_gc.trades)} & {kf_return_percent_str} & {len(backtest_kalman_filter.trades)}\\\\\\cline{{3-12}}"
#     if idx == 6:
#         to_print = to_print[:-11] + "hline\\hline"

#     if idx == 3:
#         to_print = f"{int(window_size_in_seconds / (24 * 60 * 60))} days " + to_print

    # results_for_table.append(to_print)
#     results_for_plot.append(f'{int(window_size_in_seconds / (24 * 60 * 60))},{idx},{round(constant_return_percent, rounding_num)},{len(backtest_constant_hr.trades)},{round(sw_return_percent, rounding_num)},{len(backtest_sliding_window.trades)},{round(lagged_return_percent, rounding_num)},{len(backtest_lagged.trades)},{round(gc_return_percent, rounding_num)},{len(backtest_gc.trades)},{round(kf_return_percent, rounding_num)},{len(backtest_kalman_filter.trades)}')

    # print(*results_for_table,sep='\n')
    # print(*results_for_plot,sep='\n')

    # font_size = 13

    # #### Plot for Account History
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_constant_hr.times]), backtest_constant_hr.account_value_history, label='Constant')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_sliding_window.times]), backtest_sliding_window.account_value_history, label='Sliding Window')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_lagged.times]), backtest_lagged.account_value_history, label='Lagged')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_gc.times]), backtest_gc.account_value_history, label='Granger Causality')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_kalman_filter.times]), backtest_kalman_filter.account_value_history, label='Kalman Filter')
    # plt.xlabel('Date', fontsize=font_size)
    # plt.ylabel('Account Value', fontsize=font_size)
    # plt.title(f'Account Value over time when trading {cointegrated_pair[0]} and {cointegrated_pair[1]}')
    # plt.legend()
    # plt.show()

    #### Plot for Evolving Hedge Ratio
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_constant_hr.times[:len(constant_hr_strategy.hedge_ratio_history)]]), constant_hr_strategy.hedge_ratio_history, label='Constant')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_sliding_window.times[:len(sliding_window_strategy.hedge_ratio_history)]]), sliding_window_strategy.hedge_ratio_history, label='Sliding Window')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_lagged.times[:len(lagged_strategy.hedge_ratio_history)]]), lagged_strategy.hedge_ratio_history, label='Lagged')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_gc.times[:len(gc_strategy.hedge_ratio_history)]]), gc_strategy.hedge_ratio_history, label='Granger Causality')
    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_kalman_filter.times[:len(kalman_filter_strategy.hedge_ratio_history)]]), kalman_filter_strategy.hedge_ratio_history, label='Kalman Filter')
    # plt.xlabel('Date', fontsize=font_size)
    # plt.ylabel('Hedge Ratio', fontsize=font_size)
    # plt.title(f'How the Hedge Ratio evolves over time between {cointegrated_pair[0]} and {cointegrated_pair[1]}')
    # plt.legend()
    # plt.show()

    # market_price_label = 'token0_price'

    # pools = table_to_df(command="SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%_WETH_0x%';", path_to_config='../utils/database.ini')['tablename']

    # market_return = table_to_df(command=f'SELECT period_start_unix, {market_price_label} FROM "USDC_WETH_0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640" where period_start_unix >= {backtest_constant_hr.times[0]};', path_to_config='../utils/database.ini')
    # fig, axs = plt.subplots(2, sharex=True,)
    # fig.suptitle(f'Trading {cointegrated_pair[0]} and {cointegrated_pair[1]}')
    
    # axs[0].plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_constant_hr.times]), pd.DataFrame({'account': backtest_constant_hr.account_value_history})['account'], label='Constant')
    # axs[0].plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_sliding_window.times]), pd.DataFrame({'account': backtest_sliding_window.account_value_history})['account'], label='Sliding Window')
    # axs[0].plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_lagged.times]), pd.DataFrame({'account': backtest_lagged.account_value_history})['account'], label='Lagged')
    # axs[0].plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_gc.times]), pd.DataFrame({'account': backtest_gc.account_value_history})['account'], label='Granger Causality')
    # axs[0].plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_kalman_filter.times]), pd.DataFrame({'account': backtest_kalman_filter.account_value_history})['account'], label='Kalman Filter')
    # axs[0].legend()
    # axs[0].set_ylabel('Account Value in ETH')
    # axs[1].plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in market_return['period_start_unix']]), market_return[market_price_label])
    # axs[1].set_ylabel('Value of ETH in USD')
    # plt.show()

    # #### Calculate Betas
    # market_price_label = 'token0_price'
    # market_return = table_to_df(command=f'SELECT period_start_unix, {market_price_label} FROM "USDC_WETH_0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640";', path_to_config='../utils/database.ini')

    # merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_constant_hr.times, 'value': backtest_constant_hr.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    # returns = merged.pct_change().dropna()
    # beta_constant = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    # merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_sliding_window.times, 'value': backtest_sliding_window.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    # returns = merged.pct_change().dropna()
    # beta_sw = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    # merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_lagged.times, 'value': backtest_lagged.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    # returns = merged.pct_change().dropna()
    # beta_lagged = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    # merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_gc.times, 'value': backtest_gc.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    # returns = merged.pct_change().dropna()
    # beta_GC = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    # merged = pd.merge(market_return, pd.DataFrame({'period_start_unix': backtest_kalman_filter.times, 'value': backtest_kalman_filter.account_value_history}), on="period_start_unix").dropna().set_index('period_start_unix')
    # returns = merged.pct_change().dropna()
    # beta_KF = returns.cov()[market_price_label]['value'] / market_return.var()[market_price_label]

    # round_num = 7
    # to_print = f"{idx} & {round(beta_constant, round_num)} & {round(beta_sw, round_num)} & {round(beta_lagged, round_num)} & {round(beta_GC, round_num)} & {round(beta_KF, round_num)}\\\\\\cline{{2-6}}"
    # to_print = f"{idx} & {beta_constant:.5e} & {beta_sw:.5e} & {beta_lagged:.5e} & {beta_GC:.5e} & {beta_KF:.5e}\\\\\\cline{{2-6}}"

    # if idx == 6:
    #     to_print = to_print[:-10] + "hline\\hline"

    # results_for_beta.append(to_print)
    # print(to_print)
    # betas.loc[idx] = [beta_constant, beta_sw, beta_lagged, beta_GC, beta_KF]

    # round_num = 7

    # def calculate_sharpe_ratio(times, account_value_history, num_of_trades):
    #     interest_rate = (1.045 ** ((times[-1] - times[0]) / (365*24*60*60))) - 1
    #     table = pd.DataFrame({'time': times, 'value': account_value_history})
    #     return_p = table.pct_change()['value']
    #     expected_return = (1 + (return_p.mean())) ** num_of_trades - 1
    #     sr = (expected_return - interest_rate) / .std()
    #     print(sr)
    #     return sr

    # sharpe_ratios.loc[idx] = [
    #     calculate_sharpe_ratio(backtest_constant_hr.times, backtest_constant_hr.account_value_history, len(backtest_constant_hr.trades)),
    #     calculate_sharpe_ratio(backtest_sliding_window.times, backtest_sliding_window.account_value_history, len(backtest_sliding_window.trades)),
    #     calculate_sharpe_ratio(backtest_lagged.times, backtest_lagged.account_value_history, len(backtest_lagged.trades)),
    #     calculate_sharpe_ratio(backtest_gc.times, backtest_gc.account_value_history, len(backtest_gc.trades)),
    #     calculate_sharpe_ratio(backtest_kalman_filter.times, backtest_kalman_filter.account_value_history, len(backtest_kalman_filter.trades))]
    
    # to_print = f"{idx} & {round(sharpe_ratios.iloc[idx]['Constant'], 4)} & {round(sharpe_ratios.iloc[idx]['SW'], round_num)} & {round(sharpe_ratios.iloc[idx]['Lagged'], round_num)} & {round(sharpe_ratios.iloc[idx]['GC'], round_num)} & {round(sharpe_ratios.iloc[idx]['KF'], round_num)}\\\\\\cline{{2-6}}"
    # results_for_sr.append(to_print)

    # table = table_to_df(
    #     command=f"SELECT pool_address, token0, token1, feetier FROM liquidity_pools where volume_usd >= 10000000000 and (token0='WETH' OR token1='WETH');", path_to_config='../utils/database.ini')
    # table['feetier'] = table['feetier'].apply(lambda x: x*1e-4)

    # print(table.to_latex(index=False))

    # table = table_to_df(
    #     command=f'SELECT * FROM "{cointegrated_pair[0]}" where gas_price_wei > 0 ORDER BY period_start_unix;', path_to_config='../utils/database.ini')

    # f = table['gas_price_wei'].to_numpy()*1e-9
    # print(f)
    # plt.plot(table['period_start_unix'], f)
    # plt.ylabel('Gas Price in Gwei')
    # plt.xlabel('UNIX Timestamp')
    # plt.show()

    # model = sm.OLS(backtest_mean_reversion.history_p2, sm.add_constant(backtest_mean_reversion.history_p1))
    # results = model.fit()

    # # Calculate the ratio of the coefficients
    # Gradient of the OLS i.e. X = results.params[0] + results.params[1] 'p2_token1_price'
    # print(results.params)

    # cm = plt.get_cmap('jet')
    # sc2 = plt.scatter(backtest_constant_hr.history_p2, backtest_constant_hr.history_p1, s=30, c=list(backtest_constant_hr.history_times), cmap=cm, alpha=0.3,label='Price',edgecolor='k')
    # sc = plt.scatter(backtest_constant_hr.history_p2, backtest_constant_hr.history_p1, s=30, c=list(backtest_constant_hr.history_times), cmap=cm, alpha=1,label='Price',edgecolor='k').set_visible(False)
    # cb = plt.colorbar(sc)
    # plt.plot(backtest_constant_hr.history_p2, constant_hr_strategy.hedge_ratio * backtest_constant_hr.history_p2 + constant_hr_strategy.foo, alpha=.5, lw=2)
    # cb.ax.get_yaxis().labelpad = 20

    # font_size = 15
    # cb.set_label('UNIX Timestamp', rotation=270, fontsize=font_size)
    # plt.ylabel(f'Price of {cointegrated_pair[0]}', fontsize=font_size)
    # plt.xlabel(f'Price of {cointegrated_pair[1]}', fontsize=font_size)
    # plt.show()

    # state_means = kalman_filter_strategy.means_trace
    # font_size = 15

    # # add regression lines
    # step = int(len(state_means) / 10) # pick slope and intercept every 50 days

    # colors_l = np.linspace(0.1, 1, len(state_means[::step]))
    # for i, b in enumerate(state_means[::step]):
    #     print(b)
    #     plt.plot(backtest_kalman_filter.history_p2, b[0] * backtest_kalman_filter.history_p2 + b[1], alpha=.5, lw=2, c=cm(colors_l[i]))

    # plt.ylabel(f'Price of {cointegrated_pair[0]}', fontsize=font_size)
    # plt.xlabel(f'Price of {cointegrated_pair[1]}', fontsize=font_size)
    # cb.set_label('UNIX Timestamp', rotation=270, fontsize=font_size)
    # plt.show()

    # plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_kalman_filter.history_times[:len(kalman_filter_strategy.hedge_ratio_history)]]), kalman_filter_strategy.hedge_ratio_history)
    # plt.xlabel('Date', fontsize=font_size)
    # plt.ylabel('Hedge Ratio', fontsize=font_size)
    # plt.title(f'How the Hedge Ratio evolves over time between {cointegrated_pair[0]} and {cointegrated_pair[1]}')
    # plt.show()

    # print(*backtest_mean_reversion.trades, sep='\n')

# print(*results_for_beta,sep='\n')
# print(*results_for_plot,sep='\n')
# betas.loc['average'] = betas.mean()
# print(betas)

# print(*results_for_sr,sep='\n')
# sharpe_ratios.loc['avg'] = sharpe_ratios.mean()
# print(sharpe_ratios)

# returns.loc['avg'] = returns.mean()
# returns.loc['std'] = returns.std()
# returns.loc['sharpe_ratio'] = (returns.mean() - 4.5) / (returns - 4.5).std()

# print(returns)
