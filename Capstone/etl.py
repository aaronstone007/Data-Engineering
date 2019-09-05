import configparser
import psycopg2
from sql_queries import copy_table_queries,insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data into staging tables in sql_queries.py
    """
    for query in copy_table_queries:
        print (query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Inserts data into appropriate analytical tables sql_queries.py
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    1. Initialize a config parser to read configuration on dwh.cfg file
    2. Create connection and cursor for postgresql
    3. Loading functions
    4. Close connection
    """
    print("Please wait while executing... may take upto 30 mins")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()
    print("Done !!")

if __name__ == "__main__":
    main()