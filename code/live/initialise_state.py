import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
import pickle
from strategies.kalman_filter_strategy import Kalman_Filter_Strategy
from utils.helpers import days_to_seconds

def initialize_state(initial_amount_of_ETH, initial_amount_of_WETH):
    number_of_sds_from_mean = 0.5
    gas_price_threshold = 1.29e-07
    window_size_in_seconds = days_to_seconds(30)

    state = {
        'pair': ('USDC_WETH_0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'USDC_WETH_0xe0554a476a092703abdb3ef35c80e0d76d32939f'),
        'strategy': Kalman_Filter_Strategy(number_of_sds_from_mean=number_of_sds_from_mean,
                                                        window_size_in_seconds=window_size_in_seconds,
                                                        percent_to_invest=1,
                                                        gas_price_threshold=gas_price_threshold,
                                                        rebalance_threshold_as_percent_of_initial_investment=0.5,
                                                        should_batch_trade=True),
        'open_positions': {},
        'account': {
            'T1': 0,
            'T2': 0,
            'WETH': initial_amount_of_WETH,
            'collateral_WETH': 0,
            'ETH': initial_amount_of_ETH
        }
    }

    with open('state.pickle', 'wb') as f:
        pickle.dump(state, f)
    f.close()

