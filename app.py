from ec2_util import allocate_elastic_ip, add_ip_to_security_group
from iam_util import create_iam_role
from secrets_util import get_secret
from redshift_util import _create_cluster, create_parameter_group, modify_parameter_group     
from glue_util import create_glue_db
from rds_mysql import create_rds_db, describe_instance
import dotenv
import os

def redshift(): 
    ''' This function takes care of redshift related actions '''
    
    '''
    Creating elastic ip for the cluster for public availability
    '''
    elastic_ip = allocate_elastic_ip()

    '''
    Creating a custem parameter group in order to modify the search path 
    '''
    parameter_group_name = 'retail-pg'
    parameter_group_family = 'redshift-1.0'
    description = 'Custom parameter group for retail-cluster'
    res = create_parameter_group(parameter_group_name, parameter_group_family, description)

    '''
    Setting up the search path with our schema
    '''
    schema_name = 'retail_schema'
    search_path = f"$user, public, '{schema_name}'"
    res = modify_parameter_group(parameter_group_name, search_path)

    '''
    Creating IAM role for redshift with required permissions
    '''
    aws_managed_redshift_PolicyArn = 'arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess' # it has permissions for cloudwatch, glue, dynamodb,....
    rds_policy_arn = 'arn:aws:iam::aws:policy/AmazonRDSFullAccess' # For federated queries, policy to access rds db. Its not there in the above policy.
    policy_arn_list = [aws_managed_redshift_PolicyArn, rds_policy_arn]
    iam_role_arn = create_iam_role(policy_arn_list)
    os.environ.setdefault('REDSHIFT_IAM_ROLE_ARN', f'{iam_role_arn}') # to be accessed in queries.py file

    '''
    Getting credentials from secret manager to set up cluster
    '''
    redshift_cluster_secret_name = os.environ.get('REDSHIFT_CLUSTER_SECRET_NAME')
    cluster_secret = get_secret(redshift_cluster_secret_name)[0]
    cluster_user_name = cluster_secret['username']
    cluster_password = cluster_secret['password']

    '''
    Creating the redshift cluster
    '''
    cluster_identifier = 'retail-cluster'
    res = _create_cluster(parameter_group_name, cluster_identifier, elastic_ip, iam_role_arn, cluster_user_name, cluster_password)

    '''
    Modifying the inbound rule of the cluster inorder to access from local
    '''
    security_group_id = res['Cluster']['VpcSecurityGroups'][0]['VpcSecurityGroupId']
    ip_address = os.environ.get('IP_ADDRESS')
    port = 5439
    res = add_ip_to_security_group(security_group_id, ip_address, port)   # to access cluster from local

def glue():
    ''' This function takes care of aws glue related actions '''
    
    '''
    Creating glue database for spectrum
    '''
    glue_database_name = 'retail_db_redshift1'     # as per the default redshift role ploicy resource arn by aws the glue database name should contain the string redshift in it
    glue_database_description = "Database for redshift spectrum"

    res = create_glue_db(glue_database_name, glue_database_description) 

def rds_mysql():
    ''' This function takes care of aws rds related actions '''
    
    '''
    Getting credentials for mysql db from secrets manager
    '''
    mysql_secret_name = os.environ.get('MYSQL_SECRET_NAME')
    mysql_secret = get_secret(mysql_secret_name)[0]
    mysql_admin_user_name = mysql_secret['username']
    mysql_admin_password = mysql_secret['password']

    '''
    Creating mysql instance in rds for federated queries
    '''
    db_instance_identifier = "retail-mysql-db"
    mysql_db_name = "retail_db_redshift"

    res = create_rds_db(db_instance_identifier, mysql_admin_user_name, mysql_admin_password, mysql_db_name)

    '''
    Getting instance details
    '''
    res = describe_instance(db_instance_identifier) 
    security_group_id = res['DBInstances'][0]['VpcSecurityGroups'][0]['VpcSecurityGroupId']

    '''
    Updating the inbound rule of the sequrity group to access from local
    '''
    # security_group_id = 'sg-03b4db065615f216b'
    local_ip_address = os.environ.get('IP_ADDRESS')
    mysql_port = 3306
    res = add_ip_to_security_group(security_group_id, local_ip_address, mysql_port)


def main():
    redshift() 
    glue()
    rds_mysql()


if __name__ == "__main__":

    dotenv.load_dotenv()   # loading environment variables

    main()