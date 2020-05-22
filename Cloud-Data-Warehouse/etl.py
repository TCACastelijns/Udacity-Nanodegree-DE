import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Method to load data from the logs to the staging tables
    
    :param cur: psycopg2 cursor object
    :param conn: psycopg2 connection object
    """
    
    print('Copying Song en Logs from Music App to staging tables')
    for k, query in enumerate(copy_table_queries):
        print(f'Query {k+1} from {len(copy_table_queries)}:\n{query}')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Method to SELECT and INSERT the data from the staging tables into the Facts and Dimensions from the schema design
    
    :param cur: psycopg2 cursor object
    :param conn: psycopg2 connection object
    """
              
    print('Selecting and Inserting data from staging tables into facts and dimensions')
    for k, query in enumerate(insert_table_queries):
        print(f'Query {k+1} from {len(insert_table_queries)}:\n{query}')
        cur.execute(query)
        conn.commit()


def main():
    """
    Method to:
    
    1. Importing the metadata of the staging tables, the fact and dimensions from `sql_queries`
    2. Copying the song and log data into the staging tables 
    3. Selecting and Inserting the staging tables into the fact and dimensions
    
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