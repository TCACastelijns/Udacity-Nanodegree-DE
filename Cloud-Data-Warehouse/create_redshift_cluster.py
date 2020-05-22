import pandas as pd
import boto3
import json
import time
import configparser
from botocore.exceptions import ClientError



def parse_config_file(filename, show=True):
    """
    Method to parse the configuration file to values
    :param (str) filename: path to filename of configuration file
    :param (bool) show: to show the values of the configuration file in DataFrame format
    """
    config = configparser.ConfigParser()
    config.read_file(open(filename))
    
    
    # AWS Credentials
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    
    # Redshift HW
    DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")
    
    # Identifiers & Credentials
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("CLUSTER","DWH_DB")
    DWH_DB_USER            = config.get("CLUSTER","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DWH_PORT")
    DWH_REGION             = config.get("CLUSTER", "DWH_REGION")
    
    # Roles (for s3 access)
    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

    
    if show:
        pd.DataFrame({"Param":
                          ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", 
                           "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME", "DWH_REGION"],
                      "Value":
                          [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, 
                           DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, DWH_REGION]
                     })
    
    return KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES,DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, DWH_REGION


def create_clients(dwh_region, key, secret):
    """
    Method to create clients for EC2, S3, IAM and Redshift 
    
    :param (str) dwh_region: Region of servers
    :param (str) key: AWS key
    :param (str) secret: AWS secret
    """

    ec2 = boto3.resource('ec2',
                           region_name=dwh_region,
                           aws_access_key_id=key,
                           aws_secret_access_key=secret
                        )

    s3 = boto3.resource('s3',
                           region_name=dwh_region,
                           aws_access_key_id=key,
                           aws_secret_access_key=secret
                       )

    iam = boto3.client('iam',
                           aws_access_key_id=key,
                           aws_secret_access_key=secret,
                           region_name=dwh_region
                      )

    redshift = boto3.client('redshift',
                           region_name=dwh_region,
                           aws_access_key_id=key,
                           aws_secret_access_key=secret
                           )
    return ec2, s3, iam, redshift


def create_iam_role_and_policy(iam, dwh_iam_role_name):
    """
    Method to create IAM role based on DWH_IAM_ROLE_NAME
    
    :param iam: IAM client from boto3
    :param (str) dwh_iam_role_name: DWH_IAM_ROLE_NAME (from configuration file)
    :return pointer to IAM role
    """
    
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=dwh_iam_role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=dwh_iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']
    print(roleArn)
    return roleArn
    
def create_red_shift_cluster(redshift, roleArn, dwh_cluster_type,dwh_node_type, dwh_num_nodes, dwh_db, dwh_cluster_identifier, dwh_db_user, dwh_db_password):
    """
    Method to create a new Redshift cluster
    
    :param redshift: Redshift client from boto3
    :param (str) roleArn: Pointer to IAM role 
    :param (str) dwh_cluster_type: The type of the cluster. When cluster type is specified as + `single-node`, the **dwh_num_nodes** parameter is not required.
                                                + `multi-node`, the **dwh_num_nodes** parameter is required.
    :param (str) dwh_node_type: Parameter to show only the available offerings matching the specified node type.
    :param (str) dwh_num_nodes: The number of compute nodes in the cluster. This parameter is required when the **dwh_cluster_type** parameter is specified as `multi-node`.
    :param (str) dwh_db: The name of the first database to be created when the cluster is created.
    :param (str) dwh_cluster_identifier:A unique identifier for the cluster. You use this identifier to refer to the cluster for any subsequent 
                        cluster operations such as deleting or modifying. The identifier also appears in the Amazon Redshift console.
    :param (str) dwh_db_user: The user name associated with the master user account for the cluster that is being created.
    :param (str) dwh_db_password: The password associated with the master user account for the cluster that is being created.
    """
    

    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=dwh_cluster_type,
            NodeType=dwh_node_type,
            NumberOfNodes=int(dwh_num_nodes),

            #Identifiers & Credentials
            DBName=dwh_db,
            ClusterIdentifier=dwh_cluster_identifier,
            MasterUsername=dwh_db_user,
            MasterUserPassword=dwh_db_password,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def get_cluster_properties(redshift, dwh_cluster_identifier, pretty=False):
    """
    Method to get the properties of the Redshift cluster
    
    :param redshift: Redshift client from boto3
    :param (str) dwh_cluster_identifier:A unique identifier for the cluster. You use this identifier to refer to the cluster for any subsequent 
                    cluster operations such as deleting or modifying. The identifier also appears in the Amazon Redshift console.
    :param (bool) pretty: True if pretty DataFrame output, False regular dictionary
    :return DataFrame with the properties of the Redshift cluster
    """
    

    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
    
    if pretty:
        return prettyRedshiftProps(cluster_props)
    else:
        return cluster_props


def cluster_available(cluster_props):
    """
    Method to check if Redshift cluster is available
    
    :param (dict) cluster_props: Properties of Redshift Cluster
    :return: True if available, else False
    """
    cluster_status = cluster_props['ClusterStatus']
    
    if cluster_status == 'available':
        return True
    else:
        return False


def get_dwh_endpoint_role_arn(cluster_props):
    """
    Method to get DWH_ENDPOINT/HOST and DWH_ROLE_ARN/IAM ROLE
    
    :param (dict) cluster_props: Properties of Redshift Cluster
    :return: True if available, else False
    """    
    DWH_ENDPOINT = cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
    return DWH_ENDPOINT, DWH_ROLE_ARN

def open_incoming_tcp(ec2, cluster_props, dwh_port):
    """
    Method To open an incoming  TCP port to access the cluster endpoint
    
    :param ec2: EC2 client of boto3
    :param (dict) cluster_props: roperties of Redshift Cluster
    :param (str) dwh_port: Port of the DWH
    """
     
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_port),
            ToPort=int(dwh_port)
        )
    except Exception as e:
        print(e)
        
def main(filename='dwh.cfg'):
    
    # Parsing the configuration file
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES,DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, DWH_REGION =\
        parse_config_file(filename, show=False)
    
    # Creating the clients
    ec2, s3, iam, redshift = create_clients(DWH_REGION, KEY, SECRET)
    
    # Create the IAM Role and Access Policy
    roleArn = create_iam_role_and_policy(iam, DWH_IAM_ROLE_NAME)
    
    # Create the Redshift Cluster
    try:
        create_red_shift_cluster(redshift, roleArn, DWH_CLUSTER_TYPE,DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)
    except ClusterAlreadyExists:
        print("Cluster Already exists, continue")
        
    
    while True:
        print('Get cluster properties')
        cluster_props = get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER, pretty=False)
        print(f"Cluster status: {cluster_props['ClusterStatus']}")
        if cluster_available(cluster_props):
            DWH_ENDPOINT, DWH_ROLE_ARN = get_dwh_endpoint_role_arn(cluster_props)
            print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
            print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
            break
        else:
            print("Cluster not available yet, we will wait 60s")
        time.sleep(60)
    
    # Writing to Configuration file
    config = configparser.ConfigParser()

    with open(filename) as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", cluster_props['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open(filename, 'w+') as configfile:
        config.write(configfile)

    # Open an incoming  TCP port to access the cluster endpoint
    open_incoming_tcp(ec2, cluster_props, DWH_PORT)
    
    
if __name__ == '__main__':
    main()
