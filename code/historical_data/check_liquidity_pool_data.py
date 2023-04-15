from datetime import datetime, timedelta
import time
from historical_data.database_interactions import table_to_df

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
                (select i.table_name, i.table_schema from information_schema.tables i WHERE i.table_schema = 'public' AND i.table_name <> 'liquidity_pools')
            LOOP
                EXECUTE FORMAT ('SELECT MAX(period_start_unix) FROM %I.%I', r.table_schema, r.table_name) INTO max_timestamp;
                table_name := r.table_name;
                RETURN next;
            END LOOP;
            END $$;

            SELECT table_name FROM get_max_timestamp() WHERE max_timestamp >= {ydays_timestamp};
            """)

    return df

if __name__ == "__main__":
    get_pools_max_timestamp()