from correlation_plot import get_correlation_matrix
from constants import GAS_USED_BY_SWAP, GAS_USED_BY_BUYING_ETH, GAS_USED_BY_DEPOSITING_COLLATERAL, GAS_USED_BY_WITHDRAWING_COLLATERAL, GAS_USED_BY_BORROW, GAS_USED_BY_REPAY
from calculate_cointegrated_pairs import load_cointegrated_pairs
from database_interactions import table_to_df
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


def days_to_seconds(days): return int(days * 24 * 60 * 60)


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

    def fetch_and_preprocess_data(self, cointegrated_pair, window_size_in_seconds):
        merged = table_to_df(command=f"""
                        SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.{'token1_price' if cointegrated_pair[0].split('_')[1] == 'WETH' else 'token0_price'} as p1_token_price, p2.id as p2_id, p2.{'token1_price' if cointegrated_pair[1].split('_')[1] == 'WETH' else 'token0_price'} as p2_token_price
                        FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{cointegrated_pair[1]}" as p2 ON p1.period_start_unix = p2.period_start_unix
                        WHERE p1.token1_price <> 0 AND p2.token1_price <> 0 ORDER BY p1.period_start_unix;
                        """, path_to_config='historical_data/database.ini')

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
                        """, path_to_config='historical_data/database.ini')

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

    def backtest_pair(self, cointegrated_pair, strategy, initial_investment_in_weth=100):
        history_arg, history_remaining = self.fetch_and_preprocess_data(
            cointegrated_pair, strategy.window_size_in_seconds)

        cointegrated_pair_0_split = cointegrated_pair[0].split('_')
        cointegrated_pair_1_split = cointegrated_pair[1].split('_')

        token1_symbol = cointegrated_pair_0_split[0] if cointegrated_pair_0_split[
            1] == 'WETH' else cointegrated_pair_0_split[1]
        token2_symbol = cointegrated_pair_1_split[0] if cointegrated_pair_1_split[
            1] == 'WETH' else cointegrated_pair_1_split[1]

        self.aave1_df = table_to_df(
            command=f"SELECT * FROM {token1_symbol}_borrowing_rates ORDER BY timestamp;", path_to_config='historical_data/database.ini')

        self.aave2_df = table_to_df(
            command=f"SELECT * FROM {token2_symbol}_borrowing_rates ORDER BY timestamp;", path_to_config='historical_data/database.ini')

        self.gas_prices_df = table_to_df(
            command=f"SELECT * FROM gas_prices ORDER BY timestamp;", path_to_config='historical_data/database.ini')

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

        def close_buy_position(buy_id, gas_price_in_eth):
            buy_token, bought_price, buy_volume, buy_timestamp = self.open_positions[
                'BUY'][buy_id]

            # actual_volume_bought is calculated as from the initial swap a cut is taken as fees
            actual_volume_bought = buy_volume * (1 - swap_fees[buy_token])

            # Swap Token back to WETH
            self.account[buy_token] = self.account[buy_token] - \
                actual_volume_bought
            self.account['WETH'] = self.account['WETH'] + (prices[f'P{buy_token[1]}'] * (
                actual_volume_bought * (1 - swap_fees[buy_token])))

            # Deduct gas fees
            self.account['ETH'] = self.account['ETH'] - \
                (GAS_USED_BY_SWAP * gas_price_in_eth)
            self.open_positions['BUY'].pop(buy_id)

        def close_sell_position(sell_id, gas_price_in_eth, apy, curr_timestamp):
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
            # Deduct Swap Gas Fee
            self.account['ETH'] = self.account['ETH'] - \
                GAS_USED_BY_SWAP * gas_price_in_eth

            # Return Borrowed Tokens
            self.account[sell_token] = self.account[sell_token] - \
                volume_required_to_return
            # Deduct Gas fee from loaning
            self.account['ETH'] = self.account['ETH'] - \
                GAS_USED_BY_REPAY * gas_price_in_eth

            # Return Collatoral to WETH
            self.account['WETH'] = self.account['WETH'] + \
                self.account['collateral_WETH']
            self.account['collateral_WETH'] = 0
            self.open_positions['SELL'].pop(sell_id)

        weth_borrowing_rates = table_to_df(
            command=f"SELECT * FROM weth_borrowing_rates ORDER BY timestamp;", path_to_config='historical_data/database.ini')

        ltv_eth = weth_borrowing_rates['ltv'].iloc[0]
        liquidation_threshold = weth_borrowing_rates['liquidation_threshold'].iloc[0]

        for i in tqdm(history_remaining.index):
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
                    if position_type == 'BUY':
                        for buy_id in position_ids:
                            close_buy_position(
                                buy_id=buy_id, gas_price_in_eth=gas_price_in_eth)
                            self.check_account(
                                'CLOSE', f'BUY {buy_id}', signal=signal)

                    if position_type == 'SELL':
                        for sell_id in position_ids:
                            close_sell_position(
                                sell_id=sell_id, gas_price_in_eth=gas_price_in_eth, apy=apy, curr_timestamp=timestamp)
                            self.check_account(
                                'CLOSE', f'SELL {sell_id}', signal=signal)

                elif order_type == 'OPEN':
                    open_type, token, volume = order[1]
                    next_id = len(self.trades)
                    if open_type == 'BUY':
                        buy_price = prices[f'P{token[1]}']
                        self.account['WETH'] = self.account['WETH'] - \
                            (volume * buy_price)
                        self.account[token] = self.account[token] + \
                            (volume * (1 - swap_fees[token]))
                        self.account['ETH'] = self.account['ETH'] - \
                            (GAS_USED_BY_SWAP * gas_price_in_eth)

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
                        # Deduct Gas fee from borrowing
                        self.account['ETH'] = self.account['ETH'] - \
                            (GAS_USED_BY_BORROW * gas_price_in_eth)

                        # Swap borrowed tokens to WETH
                        self.account[token] = self.account[token] - volume
                        self.account['WETH'] = self.account['WETH'] + \
                            (volume * (1 - swap_fees[token]) * sell_price)
                        # Deduct Gas fee from swapping
                        self.account['ETH'] = self.account['ETH'] - \
                            (GAS_USED_BY_SWAP * gas_price_in_eth)

                        self.open_positions['SELL'][str(next_id)] = (
                            token, sell_price, volume, timestamp)
                        self.trades.append(
                            (str(next_id), 'SELL', token, sell_price, volume, timestamp))

                        self.check_account(
                            'OPEN', f'SELL {token}', signal=signal)

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
                close_sell_position(sell_id, gas_price_in_eth=gas_price_in_eth, apy=apy,
                                    curr_timestamp=history_remaining['period_start_unix'].iloc[-1])

        account_value_in_weth = self.get_account_value_in_WETH(prices)
        return_percent = ((account_value_in_weth - initial_investment_in_weth)
                          * 100 / initial_investment_in_weth)

        return return_percent


cointegrated_pairs = load_cointegrated_pairs(
    'historical_data/cointegrated_pairs.pickle')

corr_matrix = get_correlation_matrix()

ps_with_corr = []
for pair in cointegrated_pairs:
    corr = corr_matrix[pair[0].lower()][pair[1].lower()]
    ps_with_corr.append((pair[0], pair[1], corr))

cointegrated_pairs = sorted(ps_with_corr, key=lambda x: x[2])

# print(*cointegrated_pairs, sep="\n")

particular_idx = 3
particular_idx = None

num = particular_idx if particular_idx is not None else 0
pairs = cointegrated_pairs[particular_idx:particular_idx +
                           1] if particular_idx is not None else cointegrated_pairs

bad_pairs = []

number_of_sds_from_mean = 1
window_size_in_seconds = days_to_seconds(30)
percent_to_invest = 1.00
initial_investment = 100

for cointegrated_pair in pairs:
    try:
        print(num)
        num += 1
        print(f'cointegrated_pair: {cointegrated_pair}')

        mean_reversion_strategy = Mean_Reversion_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                          window_size_in_seconds=window_size_in_seconds,
                                                          percent_to_invest=percent_to_invest,
                                                          gas_price_threshold=1.25e-07,
                                                          rebalance_threshold_as_percent_of_initial_investment=0.5)

        backtest_mean_reversion = Backtest()
        return_percent = backtest_mean_reversion.backtest_pair(
            cointegrated_pair, mean_reversion_strategy, initial_investment)

        if return_percent > 0:
            print(
                f"\033[95mMean_Reversion_Strategy\033[0m Total returns \033[92m{return_percent}%\033[0m - trading from {datetime.fromtimestamp(backtest_mean_reversion.times[0])} to {datetime.fromtimestamp(backtest_mean_reversion.times[-1])} with {len(backtest_mean_reversion.trades)} trades")
        else:
            print(
                f"\033[95mMean_Reversion_Strategy\033[0m Total returns \033[91m{return_percent}%\033[0m - trading from {datetime.fromtimestamp(backtest_mean_reversion.times[0])} to {datetime.fromtimestamp(backtest_mean_reversion.times[-1])} with {len(backtest_mean_reversion.trades)} trades")

        # fig, axs = plt.subplots(4, sharex=True,)
        # axs[0].plot(backtest_mean_reversion.history_remaining_p1.to_list())
        # axs[1].plot(backtest_mean_reversion.history_remaining_p2.to_list())

        # axs[2].plot(mean_reversion_strategy.upper_thresholds, label='upper')
        # axs[2].plot(mean_reversion_strategy.spreads, label='spread')
        # axs[2].plot(mean_reversion_strategy.lower_thresholds, label='lower')
        # axs[2].legend()

        # axs[3].plot(backtest_mean_reversion.account_value_history)
        # plt.show()

        kalman_filter_strategy = Kalman_Filter_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=percent_to_invest,
                                                        gas_price_threshold=1.25e-07,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5)

        backtest_kalman_filter = Backtest()
        return_percent = backtest_kalman_filter.backtest_pair(
            cointegrated_pair, kalman_filter_strategy, initial_investment)

        if return_percent > 0:
            print(
                f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[92m{return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")
        else:
            print(
                f"\033[96mKalman_Filter_Strategy\033[0m Total returns \033[91m{return_percent}%\033[0m with {len(backtest_kalman_filter.trades)} trades")

        # plt.plot(backtest_kalman_filter.times, backtest_kalman_filter.account_value_history)
        # plt.show()

        # font_size = 15

        # table = table_to_df(
        #     command=f"SELECT pool_address, token0, token1, feetier FROM liquidity_pools where volume_usd >= 10000000000 and (token0='WETH' OR token1='WETH');", path_to_config='historical_data/database.ini')
        # table['feetier'] = table['feetier'].apply(lambda x: x*1e-4)

        # print(table.to_latex(index=False))

        # table = table_to_df(
        #     command=f'SELECT * FROM "{cointegrated_pair[0]}" where gas_price_wei > 0 ORDER BY period_start_unix;', path_to_config='historical_data/database.ini')

        # f = table['gas_price_wei'].to_numpy()*1e-9
        # print(f)
        # plt.plot(table['period_start_unix'], f)
        # plt.ylabel('Gas Price in Gwei')
        # plt.xlabel('UNIX Timestamp')
        # plt.show()

        # plt.plot(backtest_kalman_filter.times, kalman_filter_strategy.hedge_ratio_history, label='hedge ratio')
        # plt.xlabel('Unix timestamp', fontsize=font_size)
        # plt.ylabel('Hedge Ratio', fontsize=font_size)
        # plt.title(f'How the Hedge Ratio evolves over time between {cointegrated_pair[0]} and {cointegrated_pair[1]}')
        # plt.show()

        # model = sm.OLS(backtest_mean_reversion.history_p2, sm.add_constant(backtest_mean_reversion.history_p1))
        # results = model.fit()

        # # Calculate the ratio of the coefficients
        # Gradient of the OLS i.e. X = results.params[0] + results.params[1] 'p2_token1_price'
        # print(results.params)

        # cm = plt.get_cmap('jet')
        # sc2 = plt.scatter(backtest_mean_reversion.history_p2, backtest_mean_reversion.history_p1, s=30, c=list(backtest_mean_reversion.history_times), cmap=cm, alpha=0.3,label='Price',edgecolor='k')
        # sc = plt.scatter(backtest_mean_reversion.history_p2, backtest_mean_reversion.history_p1, s=30, c=list(backtest_mean_reversion.history_times), cmap=cm, alpha=1,label='Price',edgecolor='k').set_visible(False)
        # cb = plt.colorbar(sc)
        # plt.plot(backtest_kalman_filter.history_p2, results.params[1] * backtest_kalman_filter.history_p2 + results.params[0], alpha=.5, lw=2)
        # cb.ax.get_yaxis().labelpad = 20

        # font_size = 15
        # cb.set_label('UNIX Timestamp', rotation=270, fontsize=font_size)
        # plt.ylabel(f'Price of {cointegrated_pair[0]}', fontsize=font_size)
        # plt.xlabel(f'Price of {cointegrated_pair[1]}', fontsize=font_size)
        # plt.show()

        # state_means = kalman_filter_strategy.means_trace
        # font_size = 15

        # add regression lines
        # step = int(len(state_means) / 10) # pick slope and intercept every 50 days

        # colors_l = np.linspace(0.1, 1, len(state_means[::step]))
        # for i, b in enumerate(state_means[::step]):
        #     print(b)
        #     plt.plot(backtest_kalman_filter.history_p2, b[0] * backtest_kalman_filter.history_p2 + b[1], alpha=.5, lw=2, c=cm(colors_l[i]))

        # plt.ylabel(f'Price of {cointegrated_pair[0]}', fontsize=font_size)
        # plt.xlabel(f'Price of {cointegrated_pair[1]}', fontsize=font_size)
        # cb.set_label('UNIX Timestamp', rotation=270, fontsize=font_size)
        # plt.show()

        # print(*backtest_mean_reversion.trades, sep='\n')

    except Exception as e:
        bad_pairs.append((cointegrated_pair))
        # plt.plot(backtest_mean_reversion.times, backtest_mean_reversion.account_value_history)
        # plt.show()
        print(e)
        # print(kalman_filter_strategy.account_history[-1])
        print()
    finally:
        pass
