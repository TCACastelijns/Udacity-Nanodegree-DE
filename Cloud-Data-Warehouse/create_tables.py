import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table (staging, fact, dimensions) using the queries in `drop_table_queries` list.
    
    :param cur: psycopg2 cursor object
    :param conn: psycopg2 connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table (staging, fact, dimensions) using the queries in `create_table_queries` list. 
    
    :param cur: psycopg2 cursor object
    :param conn: psycopg2 connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads host, dbname, user, password and port from configuration file
    
    - Establishes connection with the DB running on the Redshift cluster and gets a cursur to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
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