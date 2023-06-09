import matplotlib.pyplot as plt


amounts_str = ['0.1', '0.5', '1', '5', '10', '15', '20', '25', '30', '35', '40', '45',
               '50', '55', '60', '65', '70', '75', '80', '85', '90', '100', '110', '120', '138', '150']
amounts = [float(i) for i in amounts_str]


def plot_swap_fees():
    gas_fees_using_router = [
        127312, 127312, 135018, 159004,
        134996, 134988, 135228, 135239,
        159266, 135242, 159256, 159239,
        135204, 159428, 159184, 159180,
        159258, 159205, 158482, 158722,
        159458, 178394, 158711, 177706,
        178470, 178396
    ]

    gas_fees_using_pool = [
        94583,  94605, 103293, 103278,
        103234, 103258, 133216, 103580,
        133534, 157309, 180051, 103564,
        133542, 154147, 132936, 103630,
        133938, 132960, 103550, 103568,
        157394, 110838, 103566, 157843,
        181547, 132962
    ]

    plt.plot(amounts, gas_fees_using_router, label="Gas Fee using Router")
    plt.plot(amounts, gas_fees_using_pool, label="Gas Fee using Pool")
    plt.xlabel('Swap Volume in WETH')
    plt.ylabel('Amount of Gas Used')
    plt.legend()
    plt.show()


def plot_deposit_fees():
    gas_fees_deposit = [
        246327, 211618, 211618, 211618,
        211618, 211618, 211630, 211630,
        211630, 211630, 211630, 211630,
        211630, 211630, 211630, 211630,
        211630, 211630, 211630, 211630,
        211618, 211630, 211630, 211630,
        211630, 211630
    ]

    plt.plot(amounts, gas_fees_deposit)
    plt.xlabel('Volume of Collateral deposited in WETH')
    plt.ylabel('Amount of Gas Used')
    plt.show()


def plot_withdraw_fees():
    gas_fees_withdraw = [
        198251, 181155, 181155, 181155,
        181155, 181155, 181167, 181167,
        181167, 181167, 181167, 181167,
        181167, 181167, 181167, 181167,
        181167, 181167, 181167, 181167,
        181155, 181167, 181167, 181167,
        181167, 181167
    ]

    plt.plot(amounts, gas_fees_withdraw)
    plt.xlabel('Volume of Collateral withdrawn in WETH')
    plt.ylabel('Amount of Gas Used')
    plt.show()


def plot_borrow_fees():
    collateralAmounts = ['5', '30', '66', '100']
    borrowAmounts = [float(i) for i in [
        '0.5', '5', '10', '66', '100', '250', '500', '750', '1000', '1500', '2000']]
    gas_fees_borrow = [
        [
            461426, 423481,
            423481, 423493,
            423493, 423493,
            423493, 423493,
            423493, 423493,
            423493
        ],
        [
            423493, 423493,
            423493, 423505,
            423505, 423505,
            423505, 423505,
            423505, 423505,
            423505
        ],
        [
            423493, 423493,
            423493, 423505,
            423505, 423505,
            423505, 423505,
            423505, 423505,
            423505
        ],
        [
            423493, 423493,
            423493, 423505,
            423505, 423505,
            423505, 423505,
            423505, 423505,
            423505
        ]
    ]

    for idx, collateralAmount in enumerate(collateralAmounts):
        plt.plot(borrowAmounts, gas_fees_borrow[idx],
                 label=f"Collateral Amount = {collateralAmount}ETH")
    
    plt.xlabel('Volume of DAI borrowed')
    plt.ylabel('Amount of Gas Used')
    plt.legend()
    plt.show()


def plot_repay_fees():
    collateralAmounts = ['5', '30', '66', '100']
    repayAmounts = [float(i) for i in [
        '0.5', '5', '10', '66', '100', '250', '500', '750', '1000', '1500', '2000']]
    gas_fees_repay = [
        [
            418092, 401000,
            401000, 401012,
            401012, 401012,
            401012, 401012,
            401012, 401012,
            401012
        ],
        [
            401012, 401012,
            401012, 401024,
            401024, 401024,
            401024, 401024,
            401024, 401024,
            401024
        ],
        [
            401012, 401012,
            401012, 401024,
            401024, 401024,
            401024, 401024,
            401024, 401024,
            401024
        ],
        [
            401012, 401012,
            401012, 401024,
            401024, 401024,
            401024, 401024,
            401024, 401024,
            401024
        ]
    ]

    for idx, collateralAmount in enumerate(collateralAmounts):
        plt.plot(
            repayAmounts, gas_fees_repay[idx], label=f"Collateral Amount = {collateralAmount}ETH")

    plt.xlabel('Volume of DAI Repayed')
    plt.ylabel('Amount of Gas Used')
    plt.legend()
    plt.show()

def plot_trade_fees():
    gas_fees_open = [
        568687, 525954, 525954, 525954, 525954, 525942, 525954,
        525954, 525954, 525954, 525954, 525966, 525966, 525966,
        525966, 525942, 525954, 525954, 525954, 525954, 526006,
        525992, 525992, 525992, 525992, 534658, 534670, 534670,
        534670, 535476, 525942, 525954, 525954, 525954, 525954,
        525942, 525954, 525954, 525954, 525954, 525954, 525966,
        525966, 525966, 525966, 525942, 525954, 525954, 525954,
        525954, 526006, 525992, 525992, 525992, 525992, 534658,
        534670, 534670, 534670, 535476, 525942, 525954, 525954,
        525954, 525954, 525942, 525954, 525954, 525954, 525954,
        525954, 525966, 525966, 525966, 525966, 525942, 525954,
        525954, 525954, 525954, 526006, 525992, 525992, 525992,
        525992, 534658, 534670, 534670, 534670, 535476
    ]

    gas_fees_close = [
        503388, 503396, 503396, 503408, 503396, 503408, 503408,
        503408, 503420, 503420, 503408, 503408, 503408, 503420,
        503420, 503408, 503408, 503408, 503408, 503408, 503408,
        503408, 503408, 503420, 503420, 512092, 512092, 512092,
        512104, 503432, 503396, 503396, 503396, 503408, 503408,
        503408, 503408, 503408, 503420, 503420, 503408, 503408,
        503408, 503420, 503420, 503408, 503408, 503408, 503420,
        503420, 503408, 503408, 503408, 503420, 503420, 512092,
        512092, 512092, 512104, 503432, 503396, 503396, 503396,
        503408, 503408, 503408, 503408, 503408, 503420, 503420,
        503408, 503408, 503408, 503420, 503420, 503408, 503408,
        503408, 503420, 503420, 503408, 503408, 503408, 503420,
        503420, 512092, 512080, 512092, 512104, 503432
    ]

    plt.scatter(range(len(gas_fees_open)), gas_fees_open, label="Gas Fee Opening a Buy and Sell Position")
    plt.scatter(range(len(gas_fees_close)), gas_fees_close, label="Gas Fee Closing a Buy and Sell Position")
    plt.ylabel('Amount of Gas Used')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    # plot_swap_fees()
    # plot_deposit_fees()
    # plot_withdraw_fees()
    # plot_borrow_fees()
    # plot_repay_fees()
    plot_trade_fees()
