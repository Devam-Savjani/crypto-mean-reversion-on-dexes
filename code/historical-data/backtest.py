from calculate_cointegrated_pairs import load_cointegrated_pairs
from database_interactions import table_to_df
import matplotlib.pyplot as plt

cointegrated_pairs = load_cointegrated_pairs()
cointegrated_pair, hedge_ratio = cointegrated_pairs[0]

print(f'cointegrated_pair: {cointegrated_pair}')
print(f'hedge_ratio: {hedge_ratio}')

merged = table_to_df(command=f"""
                SELECT p1.period_start_unix as period_start_unix, p1.id as p1_id, p1.token1_Price_Per_token0 as p1_token1_Price_Per_token0, p2.id as p2_id, p2.token1_Price_Per_token0 as p2_token1_Price_Per_token0
                FROM "{cointegrated_pair[0]}" as p1 INNER JOIN "{cointegrated_pair[1]}" as p2
                ON p1.period_start_unix = p2.period_start_unix WHERE p1.token1_price_per_token0 <> 0 AND p2.token1_price_per_token0 <> 0;
                """)
    
df1 = merged['p1_token1_price_per_token0']
df2 = merged['p2_token1_price_per_token0']

spread = df1 - hedge_ratio * df2
spread_mean = spread.mean()
spread_std = spread.std()

k = 2
upper_threshold = spread_mean + k * spread_std
lower_threshold = spread_mean - k * spread_std

initial_p1 = (0, 1)
initial_p2 = (0, 1)

positions = {
    'P1': initial_p1,
    'P2': initial_p2
}

print(f"upper_threshold: {upper_threshold}")
print(f"lower_threshold: {lower_threshold}")

has_trade = False
trades = []

for i in range(len(df1)):
    prices = {
        'P1': df1[i],
        'P2': df2[i]
    }

    local_spread =  prices['P1'] - hedge_ratio * prices['P2']

    if has_trade:
        if local_spread < upper_threshold and local_spread > lower_threshold:
            # Exit Trade
            # Close Buy Trade
            buy_trade = trades[-1]['BUY']
            position_a, position_b = positions[buy_trade[0]]
            positions[buy_trade[0]] = (position_a - buy_trade[2], position_b + (prices[buy_trade[0]] * buy_trade[2])) 
            
            # Close Sell Trade
            sell_trade = trades[-1]['SELL']
            position_a, position_b = positions[sell_trade[0]]
            positions[sell_trade[0]] = (position_a + (sell_trade[2] * ((sell_trade[1] / prices[sell_trade[0]]) - 1)), position_b - (sell_trade[1] * sell_trade[2]))

            position_p1_a, position_p1_b = positions['P1']
            position_p2_a, position_p2_b = positions['P2']
            # if position_p1_a < 0 or position_p1_b < 0 or position_p2_a < 0 or position_p2_b < 0:
            #     print('2. Balance went below 0')

            has_trade = False
    else:
        volume_ratio = (prices['P1'] / prices['P2']) * hedge_ratio
        if local_spread > upper_threshold:
            # Sell df1 and buy df2

            # Sell df1
            position_p1_a, position_p1_b = positions['P1']
            positions['P1'] = (position_p1_a, position_p1_b + prices['P1'])

            # Buy df2
            position_p2_a, position_p2_b = positions['P2']
            positions['P2'] = (position_p2_a + volume_ratio, position_p2_b - (volume_ratio * prices['P2']))

            trades.append({
                'SELL': ('P1', prices['P1'], 1),
                'BUY': ('P2', prices['P2'], volume_ratio),
            })

            has_trade = True

        elif local_spread < lower_threshold:
            # Buy df1 and sell df2

            # Buy df1
            position_p1_a, position_p1_b = positions['P1']
            positions['P1'] = (position_p1_a + 1, position_p1_b - prices['P1'])

            # Sell df2
            position_p2_a, position_p2_b = positions['P2']
            positions['P2'] = (position_p2_a, position_p2_b + (volume_ratio * prices['P2']))

            trades.append({
                'BUY': ('P1', prices['P1'], 1),
                'SELL': ('P2', prices['P2'], volume_ratio),
            })

            has_trade = True
        
        if has_trade:
            position_p1_a, position_p1_b = positions['P1']
            position_p2_a, position_p2_b = positions['P2']
            # if position_p1_a < 0 or position_p1_b < 0 or position_p2_a < 0 or position_p2_b < 0:
            #     print('1. Balance went below 0')

        # Do nothing

# print(f'Profit: {profit}')
print(f'Exiting Positions Without Exchanging - {positions}')

price_p1 = df1[len(df1) - 1]
position_p1_a, position_p1_b = positions['P1']
positions['P1'] = (position_p1_a - (position_p1_a - initial_p1[0]), position_p1_b + ((position_p1_a - initial_p1[0]) * price_p1))

price_p2 = df2[len(df1) - 1]
position_p2_a, position_p2_b = positions['P2']
positions['P2'] = position_p2_a - (position_p2_a - initial_p2[0]), position_p2_b + (((position_p2_a - initial_p2[0]) * price_p2))

print(f'Exiting Positions After Exchanging - {positions}')


# print(f'{positions['P1'][1] + positions['P2'][1]}')
# plt.plot(merged['period_start_unix'], df1 - hedge_ratio * df2, label='Hedge Ratio')
# plt.plot(merged['period_start_unix'], [upper_threshold for _ in range(len(merged['period_start_unix']))], label='Upper Threshold')
# plt.plot(merged['period_start_unix'], [lower_threshold for _ in range(len(merged['period_start_unix']))], label='Lower Threshold')
# plt.plot(merged['period_start_unix'], foo, label='Foo')
# plt.legend()
# plt.show()
