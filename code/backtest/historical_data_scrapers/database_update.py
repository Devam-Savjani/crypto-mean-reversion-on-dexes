from gas_price_scraper import refresh_gas_price_data
from aave_scraper import refresh_borrowing_rates_data
from uniswap_pool_scraper import refresh_liquidity_pool_data

def refresh_database():
    refresh_liquidity_pool_data()
    refresh_borrowing_rates_data()
    refresh_gas_price_data()

if __name__ == "__main__":
    refresh_database()