import matplotlib.pyplot as plt

from historical_data.calculate_cointegrated_pairs import load_cointegrated_pairs
from strategies.mean_reversion import Mean_Reversion_Strategy
from historical_data.database_interactions import table_to_df

cointegrated_pairs = load_cointegrated_pairs('historical_data/cointegrated_pairs.pickle')
cointegrated_pair, hedge_ratio = cointegrated_pairs[0]

print(f'cointegrated_pair: {cointegrated_pair}')
print(f'hedge_ratio: {hedge_ratio}')

merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_Price_Per_token0 as p1_token1_Price_Per_token0, p2.id as p2_id, p2.token1_Price_Per_token0 as p2_token1_Price_Per_token0
                FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{cointegrated_pair[1]}" as p2
                ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price_per_token0 <> 0 AND p2.token1_price_per_token0 <> 0;
                """, path_to_config='historical_data/database.ini')

NUMBER_OF_DAYS_OF_HISTORY = 30
NUMBER_OF_DAYS_OF_HISTORY_IN_SECONDS = NUMBER_OF_DAYS_OF_HISTORY * 24 * 60 * 60

history_arg = merged.loc[merged['period_start_unix'] < NUMBER_OF_DAYS_OF_HISTORY_IN_SECONDS + merged['period_start_unix'][0]]
history_remaining = merged.loc[merged['period_start_unix'] >= NUMBER_OF_DAYS_OF_HISTORY_IN_SECONDS + merged['period_start_unix'][0]]
history_remaining_p1 = history_remaining['p1_token1_price_per_token0']
history_remaining_p2 = history_remaining['p2_token1_price_per_token0']


mean_reversion_strategy = Mean_Reversion_Strategy(cointegrated_pair=cointegrated_pair,
                                                  hedge_ratio=hedge_ratio,
                                                  history_p1=history_arg['p1_token1_price_per_token0'],
                                                  history_p2=history_arg['p2_token1_price_per_token0'],
                                                  number_of_sds_from_mean=1)

initial_p1 = (0, 1)
initial_p2 = (0, 1)

account = {
    'P1': initial_p1,
    'P2': initial_p2
}

trades = []
open_positions = {
    'BUY': {},
    'SELL': {}    
}

for i in history_remaining.index:
    prices = {
        'P1': history_remaining_p1[i],
        'P2': history_remaining_p2[i]
    }

    ctx = {
        'open_positions': open_positions,
    }

    signal = mean_reversion_strategy.generate_signal(ctx, prices)

    if signal is None:
        continue

    if 'CLOSE' in signal:
        for buy_id, buy_position in list(signal['CLOSE']['BUY'].items()):
            buy_pair, bought_price, buy_volume = buy_position
            position_a, position_b = account[buy_pair]
            account[buy_pair] = (position_a - buy_volume, position_b + (prices[buy_pair] * buy_volume))
            open_positions['BUY'].pop(buy_id)

        for sell_id, sell_position in list(signal['CLOSE']['SELL'].items()):
            sell_pair, sold_price, sell_volume = sell_position
            position_a, position_b = account[sell_pair]
            account[sell_pair] = (position_a + (sell_volume * ((sold_price / prices[sell_pair]) - 1)), position_b - (sold_price * sell_volume))
            open_positions['SELL'].pop(sell_id)

    if 'OPEN' in signal:
        for buy_order in signal['OPEN']['BUY']:
            buy_pair, buy_volume = buy_order
            position_a, position_b = account[buy_pair]
            account[buy_pair] = (position_a + buy_volume, position_b - (buy_volume * prices[buy_pair]))
            
            trade_id = str(len(trades))
            open_positions['BUY'][trade_id] = (buy_pair, prices[buy_pair], buy_volume)
            trades.append((trade_id, buy_pair, prices[buy_pair], buy_volume))

        for sell_order in signal['OPEN']['SELL']:
            sell_pair, sell_volume = sell_order
            position_a, position_b = account[sell_pair]
            account[sell_pair] = (position_a, position_b + (sell_volume * prices[sell_pair]))
            trade_id = str(len(trades))

            open_positions['SELL'][trade_id] = (sell_pair, prices[sell_pair], sell_volume)
            trades.append((trade_id, sell_pair, prices[sell_pair], sell_volume))


print(f'Exiting Positions Without Exchanging - {account}')

price_p1 = history_remaining_p1[len(history_remaining) - 1]
position_p1_a, position_p1_b = account['P1']
account['P1'] = (position_p1_a - (position_p1_a - initial_p1[0]), position_p1_b + ((position_p1_a - initial_p1[0]) * price_p1))

price_p2 = history_remaining_p2[len(history_remaining) - 1]
position_p2_a, position_p2_b = account['P2']
account['P2'] = position_p2_a - (position_p2_a - initial_p2[0]), position_p2_b + (((position_p2_a - initial_p2[0]) * price_p2))

print(f'Exiting Positions After Exchanging - {account}')


plt.plot(merged['period_start_unix'], merged['p1_token1_price_per_token0'] - hedge_ratio * merged['p2_token1_price_per_token0'], label='Spread')
plt.plot(history_remaining['period_start_unix'], history_remaining_p1 - hedge_ratio * history_remaining_p2, label='Spread')
plt.plot(history_remaining['period_start_unix'], mean_reversion_strategy.upper_thresholds[:-1], label='Upper Threshold')
plt.plot(history_remaining['period_start_unix'], mean_reversion_strategy.lower_thresholds[:-1], label='Lower Threshold')
# plt.plot(history_remaining['period_start_unix'], foo, label='Foo')
plt.legend()
plt.show()
