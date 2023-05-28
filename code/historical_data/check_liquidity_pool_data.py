from datetime import datetime, timedelta
import time
from database_interactions import table_to_df

def get_pools_max_timestamp():
    ydays_date = datetime.now().date() - timedelta(days=1)
    ydays_timestamp = int(time.mktime(ydays_date.timetuple()))

    tokens_supported_by_aave = ['DAI', 'EURS', 'USDC', 'USDT', 'AAVE', 'LINK', 'WBTC']
    filter_tokens = ' OR \n\t'.join([f"token0 = '{token}' OR token1 = '{token}'" for token in tokens_supported_by_aave])

    query = f"""
        Select CONCAT(pool.token0, '_', pool.token1, '_', pool.pool_address) as table_name from
        (SELECT *
        FROM liquidity_pools WHERE
        (token0='WETH' or token1='WETH') AND
        \t({filter_tokens}) AND volume_usd >= 10000000
        ORDER BY volume_usd DESC) as pool
    """

    df = table_to_df(command=f"""
            CREATE OR REPLACE FUNCTION get_max_timestamp()
                RETURNS TABLE (table_name TEXT, max_timestamp BIGINT)
                LANGUAGE plpgsql AS $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN
                ({query})
            LOOP
                EXECUTE FORMAT ('SELECT MAX(period_start_unix) FROM %I', r.table_name) INTO max_timestamp;
                table_name := r.table_name;
                RETURN next;
            END LOOP;
            END $$;

            SELECT table_name FROM get_max_timestamp() WHERE max_timestamp >= {ydays_timestamp};
            """)

    return df

if __name__ == "__main__":
    get_pools_max_timestamp()