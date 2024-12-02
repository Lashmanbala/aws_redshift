import boto3
import time
from logging_config import logger

def _create_cluster(elasticIP, iam_role_arn, username, password ):

    redshift_client = boto3.client('redshift')

    try:
        response = redshift_client.create_cluster(
                ClusterIdentifier='retail-cluster',
                ClusterType='single-node',
                NodeType='dc2.large',
                MasterUsername=username,
                MasterUserPassword=password,
                Port=5439,
                # NumberOfNodes=1,
                PubliclyAccessible=True,
                ElasticIp=elasticIP,
                IamRoles=[iam_role_arn]
                # LoadSampleData='string'
            )
        time.sleep(5)
        logger.info(f'Redshift cluster created successfully')
        return response
    
    except Exception as e:
        logger.info(f'Error creating the cluster: {e}')
