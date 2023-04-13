from configparser import ConfigParser
import psycopg2
import pandas as pd
import sqlalchemy

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def test_connection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def list_tables():
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        cur.execute("""SELECT *
            FROM pg_catalog.pg_tables
            WHERE schemaname != 'pg_catalog' AND 
                schemaname != 'information_schema';
        """)

        # display the PostgreSQL database server version
        tables = cur.fetchall()
        print(tables)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def create_table(table_name, fields, should_print=False):
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        if should_print:
            print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        fields_string = f"{fields[0][0]} {fields[0][1]} PRIMARY KEY, "
        for field in fields[1:]:
            fields_string += f"{field[0]} {field[1]} NOT NULL, "

        command = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_string[:-2]});"
        
        # execute a statement
        cur.execute(command)

        # close the communication with the PostgreSQL
        cur.close()

        # commit the changes
        conn.commit()
       
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            if should_print:
                print('Database connection closed.')

def drop_table(table_name, should_print=False):
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        if should_print:
            print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        command = f"DROP TABLE IF EXISTS {table_name};"
        
        # execute a statement
        cur.execute(command)

        # close the communication with the PostgreSQL
        cur.close()

        # commit the changes
        conn.commit()
       
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            if should_print:
                print('Database connection closed.')

def insert_rows(table_name, rows, should_print=False):
    conn = None
    if len(rows) == 0: return
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        if should_print:
            print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        args_str = b",".join(cur.mogrify("%s", (x, )) for x in rows)
        encoding = 'utf-8'
        cur.execute(f"INSERT INTO {table_name} VALUES " + args_str.decode(encoding)) 

        # # commit the changes
        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            if should_print:
                print('Database connection closed.')

def table_to_df(table_name=None, command=None, should_print=False, path_to_config='database.ini'):
    engine = None
    command = command if command is not None else f"SELECT * FROM {table_name};"
    try:
        # read connection parameters
        params = config(filename=path_to_config)

        # connect to the PostgreSQL server
        if should_print:
            print('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator= lambda: conn)
        df = pd.read_sql_query(sqlalchemy.text(command), engine.connect())
        engine.dispose()
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if engine is not None:
            engine.dispose()
            if should_print:
                print('Database connection closed.')

def drop_all_tables_except_table(table_name=None, should_print=False):
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        if should_print:
            print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        if table_name is not None:
            command = f"""
                        DO $$ DECLARE
                        rec RECORD;
                        BEGIN
                            FOR rec IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> '{table_name}')
                        LOOP
                            EXECUTE 'DROP TABLE IF EXISTS "' || rec.tablename || '" CASCADE';
                        END LOOP;
                        END $$;
                        """
        else:
            command = f"""DO $$ DECLARE
                        rec RECORD;
                        BEGIN
                        FOR rec IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS "' || rec.tablename || '" CASCADE';
                        END LOOP;
                        END $$;
                        """
        
        # execute a statement
        cur.execute(command)

        # close the communication with the PostgreSQL
        cur.close()

        # commit the changes
        conn.commit()
       
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            if should_print:
                print('Database connection closed.')

if __name__ == '__main__':
    # list_tables()
    create_table('liquidity_pools', [('pool_address', 'VARCHAR(255)'), ('token0', 'VARCHAR(255)'), ('token1', 'VARCHAR(255)'), ('volume_USD', 'NUMERIC(80,60)'), ('created_At_Timestamp', 'BIGINT')])
    # list_tables()
    # create_table('liquidity_pools', [('pool_address', 'VARCHAR(255)'), ('token0', 'VARCHAR(255)'), ('token1', 'VARCHAR(255)'), ('volume_USD', 'VARCHAR(255)'), ('created_At_Timestamp', 'BIGINT')])

    insert_rows('liquidity_pools', [('0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801','UNI','WETH','4296011283.605405753201057482744276','1620157956'), ('0x6c6bc977e13df9b0de53b251522280bb72383700','DAI','USDC','7181443762.93080360608231664706628','1620158293')])
    df = table_to_df('liquidity_pools')
    print(df.shape)
    print(df.dtypes)
    # drop_table('liquidity_pools')