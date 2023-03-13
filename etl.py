import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the staging tables using the queries in `copy_table_queries` list.
    
    Parameters
    ----------
    cur : cursor object
        Cursor object to execute the queries.
    conn : connection object
        Connection object to commit.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into the tables using the queries in `insert_table_queries` list.
    
    Parameters
    ----------
    cur : cursor object
        Cursor object to execute the queries.
    conn : connection object
        Connection object to commit.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the configuration file named `dwh.cfg` 
    to return the parameters required for the connection object `conn`. 
    
    - Establishes connection with the database and gets
    cursor to it.  
    
    - Loads the staging tables using `load_staging_tables()`.  
    
    - Inserts data into the tables specified using `insert_tables()`. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()