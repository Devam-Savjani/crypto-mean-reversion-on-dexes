import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import numpy as np

experiment_data = [
    ('backtest_results/varying_num_of_stds.csv', 'std', 'Number of Standard Deviations'),
    ('backtest_results/varying_gas_price_threshold.csv', 'threshold', 'Gas Price threshold in ETH'),
    ('backtest_results/varying_initial_investment.csv', 'ii', 'Initial Investment in ETH'),
    ('backtest_results/varying_window_size.csv', 'sw', 'Window Size in days')
]


def create_plot_for_experiments(experiment_idx):

    experiment_file, key, xaxis_label = experiment_data[experiment_idx]

    df = pd.read_csv(experiment_file)

    groups = []

    for strats in ["Constant Return", "Sliding Window Return", "Lagged Return", "Granger Causality Return", "Kalman Filter Return"]:
        std_and_returns = df[[key, strats]]
        group = std_and_returns.groupby([key]).mean()
        groups.append(group)

    new_df = pd.concat(groups, axis=1)

    if experiment_file == 'backtest_results/varying_gas_price_threshold.csv':
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.set_xscale('log')

    plt.plot(new_df, label=new_df.columns)

    font_size = 13
    plt.xlabel(xaxis_label, fontsize=font_size)
    plt.ylabel('Average Return (%)', fontsize=font_size)
    plt.axhline(y=0, color='black', linestyle='--')
    plt.legend(fontsize=font_size)

    plt.show()

def plot_regression_lines_on_scatter(cointegrated_pair, backtest_instance, strategy):
    cm = plt.get_cmap('jet')
    sc2 = plt.scatter(backtest_instance.history_p2, backtest_instance.history_p1, s=30, c=list(backtest_instance.history_times), cmap=cm, alpha=0.3,label='Price',edgecolor='k')
    sc = plt.scatter(backtest_instance.history_p2, backtest_instance.history_p1, s=30, c=list(backtest_instance.history_times), cmap=cm, alpha=1,label='Price',edgecolor='k').set_visible(False)
    cb = plt.colorbar(sc)

    regression_params = list(zip(strategy.intercept_history, strategy.hedge_ratio_history))
    step = int(len(regression_params) / 10)

    colors_l = np.linspace(0.1, 1, len(regression_params[::step]))
    for i, b in enumerate(regression_params[::step]):
        plt.plot(backtest_instance.history_p2, b[1] * backtest_instance.history_p2 + b[0], alpha=.5, lw=2, c=cm(colors_l[i]))

    plt.ylabel(f'Price of {cointegrated_pair[0]}', fontsize=font_size)
    plt.xlabel(f'Price of {cointegrated_pair[1]}', fontsize=font_size)
    cb.set_label('UNIX Timestamp', rotation=270, fontsize=font_size)
    plt.show()

font_size = 15

def plot_account_histories(cointegrated_pair, strategies):
    def plot_account_history(backtest_instance, label):
        plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in backtest_instance.times]), backtest_instance.account_value_history, label=label)
    
    for backtest_instance, _, label in strategies:
        plot_account_history(backtest_instance, label)

    plt.xlabel('Date', fontsize=font_size)
    plt.ylabel('Account Value in ETH', fontsize=font_size)
    plt.title(f'Account Value over time when trading {cointegrated_pair[0]} and {cointegrated_pair[1]}')
    plt.legend()
    plt.show()

def plot_hedge_ratio_evolutions(cointegrated_pair, strategies):
    def plot_hedge_ratio_evolution(strategy, label):
        plt.plot(pd.to_datetime([datetime.fromtimestamp(ts) for ts in strategy.hedge_ratio_times]), strategy.hedge_ratio_history, label=label)

    for _, strategy, label in strategies:
        plot_hedge_ratio_evolution(strategy, label)

    plt.xlabel('Date', fontsize=font_size)
    plt.ylabel('Hedge Ratio', fontsize=font_size)
    plt.title(f'How the Hedge Ratio evolves over time between {cointegrated_pair[0]} and {cointegrated_pair[1]}')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    create_plot_for_experiments(1)