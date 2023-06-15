from web3 import Web3
from operator import itemgetter
from itertools import groupby
import time


def execute_signal(pair, token0_address, token1_address, signal, account, open_positions, ltv):
    web3 = Web3(Web3.HTTPProvider('ETHEREUM NETWORK ADDRESS'))
    if not web3.is_connected():
        raise Exception('Failed to connect to Blockchain')

    caller = "YOUR_ADDRESS"
    private_key = "PRIVATE_KEY"  # To sign the transaction

    # Initialize address nonce
    nonce = web3.eth.get_transaction_count(caller)

    # Initialize contract ABI and address
    abi = 'CONTRACT ABI'
    contract_address = "CONTRACT_ADDRESS"
    Chain_id = web3.eth.chain_id

    # Create smart contract instance
    contract = web3.eth.contract(address=contract_address, abi=abi)

    signal = dict([(key, [j for i, j in temp])
                  for key, temp in groupby(signal, key=itemgetter(0))])

    if 'BUY ETH' in signal:
        amount_to_buy = sum(signal['BUY ETH'])
        call_function = contract.functions.swapWethForEth(amount_to_buy).buildTransaction(
            {"chainId": Chain_id, "from": caller, "nonce": nonce})
        # Sign transaction
        signed_tx = web3.eth.account.sign_transaction(
            call_function, private_key=private_key)
        # Send transaction
        send_tx = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        # Wait for transaction receipt
        tx_receipt = web3.eth.wait_for_transaction_receipt(send_tx)

    if 'DEPOSIT' in signal:
        amount_to_deposit = sum(signal['DEPOSIT'])
        call_function = contract.functions.depositCollateral(amount_to_deposit).buildTransaction(
            {"chainId": Chain_id, "from": caller, "nonce": nonce})
        # Sign transaction
        signed_tx = web3.eth.account.sign_transaction(
            call_function, private_key=private_key)
        # Send transaction
        send_tx = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        # Wait for transaction receipt
        tx_receipt = web3.eth.wait_for_transaction_receipt(send_tx)

    if 'CLOSE' in signal:
        for type, token, volume, price in signal['CLOSE']:
            if type == 'BUY':
                pool = pair[0] if token == 'T1' else pair[1]
                buy_pool_address = pool.split("_")[2]
                buy_zero_for_one = pool.split("_")[1] == 'WETH'
                buy_amount = volume
            elif type == 'SELL':
                pool = pair[0] if token == 'T1' else pair[1]
                sell_token_address = token0_address if token == 'T1' else token1_address
                sell_amount = volume
                collatoral_amount = account['collatoral_WETH']
                account['collatoral_WETH'] = 0
                sell_pool_address = pool.split("_")[2]
                sell_zero_for_one = pool.split("_")[0] == 'WETH'

        call_function = contract.functions.closeBuySellPositions(buy_pool_address, buy_zero_for_one, buy_amount, sell_token_address, sell_amount,
                                                                 collatoral_amount, sell_pool_address, sell_zero_for_one).buildTransaction({"chainId": Chain_id, "from": caller, "nonce": nonce})
        # Sign transaction
        signed_tx = web3.eth.account.sign_transaction(
            call_function, private_key=private_key)
        # Send transaction
        send_tx = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        # Wait for transaction receipt
        tx_receipt = web3.eth.wait_for_transaction_receipt(send_tx)
        open_positions = {}

    if 'OPEN' in signal:
        for type, token, volume, price in signal['OPEN']:
            if type == 'BUY':
                pool = pair[0] if token == 'T1' else pair[1]
                buy_pool_address = pool.split("_")[2]
                buy_zero_for_one = pool.split("_")[0] == 'WETH'
                buy_amount = volume
                buy_price = price
                buy_token = token
            elif type == 'SELL':
                pool = pair[0] if token == 'T1' else pair[1]
                sell_token_address = token0_address if token == 'T1' else token1_address
                sell_amount = volume
                collatoral_amount = volume / ltv
                account['collatoral_WETH'] = collatoral_amount
                sell_pool_address = pool.split("_")[2]
                sell_zero_for_one = pool.split("_")[1] == 'WETH'
                sell_price = price
                sell_token = token

        call_function = contract.functions.openBuySellPositions(buy_pool_address, buy_zero_for_one, buy_amount, sell_token_address, sell_amount,
                                                                collatoral_amount, sell_pool_address, sell_zero_for_one).buildTransaction({"chainId": Chain_id, "from": caller, "nonce": nonce})
        # Sign transaction
        signed_tx = web3.eth.account.sign_transaction(
            call_function, private_key=private_key)
        # Send transaction
        send_tx = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        # Wait for transaction receipt
        tx_receipt = web3.eth.wait_for_transaction_receipt(send_tx)
        open_positions = {'BUY': {'1': (buy_token, buy_price, buy_amount, time.time())}, 'SELL': {
            '2': (sell_token, sell_price, sell_amount, time.time())}}

    eth_balance = web3.from_wei(web3.eth.get_balance(caller), 'ether')

    weth = web3.eth.contract(
        address='0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', abi="WETH ABI")
    weth_balance = web3.from_wei(
        weth.functions.balanceOf(caller).call(), 'ether')

    token0 = web3.eth.contract(address=token0_address, abi="token0 ABI")
    token0_balance = web3.from_wei(
        token0.functions.balanceOf(caller).call(), 'ether')

    token1 = web3.eth.contract(address=token1_address, abi="token1 ABI")
    token1_balance = web3.from_wei(
        token1.functions.balanceOf(caller).call(), 'ether')

    account['ETH'] = eth_balance
    account['WETH'] = weth_balance
    account['T1'] = token0_balance
    account['T2'] = token1_balance

    return account, open_positions
