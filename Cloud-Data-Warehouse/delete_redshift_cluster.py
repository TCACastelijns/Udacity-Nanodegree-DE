import pandas as pd
import boto3
import json
import time
import configparser
from create_redshift_cluster import parse_config_file, create_clients
        
def main(filename='dwh.cfg'):
    
    # Parsing the configuration file
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES,DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, DWH_REGION =\
        parse_config_file(filename, show=False)
    
    # Creating the clients
    ec2, s3, iam, redshift = create_clients(DWH_REGION, KEY, SECRET)
    
    # Deleting the cluster
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    
if __name__ == '__main__':
    main()
