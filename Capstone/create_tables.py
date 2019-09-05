import configparser
import psycopg2
from sql_queries import drop_table_queries,create_table_queries

def drop_tables(cur, conn):
    """
    Drops all table if already exists in sql_queries.py 
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    Creates all tables in sql_queries.py 
    """
    for query in create_table_queries:
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
    print("Please wait while executing...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    print("Done !!")


if __name__ == "__main__":
    main()