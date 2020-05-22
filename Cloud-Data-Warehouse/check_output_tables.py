import configparser
import psycopg2
import pandas as pd
from sql_queries import analytics

def get_result_analytics(cur):
    """
    Method to get the queries from the analytics and put the result into a DataFrame
    
    :param cur: psycopg2 cursor object
    :return DataFrame with counts of all tables    
    """
    res_df = pd.DataFrame(columns=['count'])
    for k,v in analytics.items():
        cur.execute(v)
        results = cur.fetchone()
        for row in results:
            res_df.loc[k, 'count'] = row
    return res_df.astype(int)
    

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print(get_result_analytics(cur))
    conn.close()

if __name__ == "__main__":
    main()