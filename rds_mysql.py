import boto3
from secrets_util import get_secret
from logging_config import logger
import time

def create_rds_db(db_instance_identifier, admin_user_name, admin_password, db_name):
    rds_client = boto3.client('rds')
    try:
        response = rds_client.create_db_instance(
                                    DBInstanceIdentifier = db_instance_identifier,
                                    AllocatedStorage = 20,
                                    DBInstanceClass = "db.t3.micro",
                                    Engine = "mysql",
                                    MasterUsername = admin_user_name,
                                    MasterUserPassword = admin_password,
                                    DBName = db_name,
                                    PubliclyAccessible = True, 
                                    StorageType = 'gp2',  
                                )
        time.sleep(5)
        logger.info(f"Creating RDS MySQL instance {db_instance_identifier}...")
        return response
    except Exception as e:
        logger.error(f"Error creating RDS instance: {e}")

def describe_instance(db_instance_identifier):
    rds_client = boto3.client('rds')
    try:
        response = rds_client.describe_db_instances(
                                    DBInstanceIdentifier=db_instance_identifier)
        return response
    except Exception as e:
        logger.info(f'Error describing instance: {e}')

mysql_secret_name = 'rds_mysql_secret'
mysql_secret = get_secret(mysql_secret_name)
admin_user_name = mysql_secret['username']
admin_password = mysql_secret['password']

db_instance_identifier = "retail-mysql-db"
db_name = "retail_db_redshift"

res = create_rds_db(db_instance_identifier, admin_user_name, admin_password, db_name)

res = describe_instance(db_instance_identifier)
print(res)
endpoint = res['DBInstances'][0]['Endpoint']['Address']
print(endpoint)
security_group = res['DBInstances'][0]['VpcSecurityGroups'][0]['VpcSecurityGroupId']
print(security_group)