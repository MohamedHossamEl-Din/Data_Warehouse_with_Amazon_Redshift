import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Parameters
    ----------
    cur : cursor object
        Cursor object to execute the queries.
    conn : connection object
        Connection object to commit.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    Parameters
    ----------
    cur : cursor object
        Cursor object to execute the queries.
    conn : connection object
        Connection object to commit.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the configuration file named `dwh.cfg` 
    to return the parameters required for the connection object `conn`. 
    
    - Establishes connection with the database and gets
    cursor to it.  
    
    - Drops all the tables using `drop_tables()`.  
    
    - Creates all tables needed using `create_tables()`. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()