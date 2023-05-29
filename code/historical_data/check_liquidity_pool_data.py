from datetime import datetime, timedelta
import time
from database_interactions import table_to_df
import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(current))
from constants import LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY

def get_pools_max_timestamp():
    ydays_date = datetime.now().date() - timedelta(days=1)
    ydays_timestamp = int(time.mktime(ydays_date.timetuple()))

    df = table_to_df(command=f"""
            CREATE OR REPLACE FUNCTION get_max_timestamp()
                RETURNS TABLE (table_name TEXT, max_timestamp BIGINT)
                LANGUAGE plpgsql AS $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN
                ({LIQUIDITY_POOLS_OF_INTEREST_TABLENAMES_QUERY})
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
    print(get_pools_max_timestamp())