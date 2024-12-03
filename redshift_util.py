import boto3
import time
from logging_config import logger

def _create_cluster(cluster_identifier,elasticIP, iam_role_arn, username, password ):

    redshift_client = boto3.client('redshift')

    try:
        response = redshift_client.create_cluster(
                ClusterIdentifier=cluster_identifier,
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


def add_ip_to_redshift_security_group(security_group_id, ip_address):

    ec2_client = boto3.client('ec2')
    
    try:
        cidr_ip = f"{ip_address}/32"  # Specify a single IP address
        
        security_group_id = security_group_id

        # Add inbound rule to allow access from the current IP
        response = ec2_client.authorize_security_group_ingress(
                                GroupId=security_group_id,
                                IpProtocol='tcp',
                                FromPort=5439,  # Default Redshift port
                                ToPort=5439,
                                CidrIp=cidr_ip
                            )
        logger.info(f"Successfully added inbound rule for IP {cidr_ip} to security group {security_group_id}.")
        return response

    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")
