import boto3
import time
from logging_config import logger

def _create_cluster(cluster_identifier, parameter_group, elasticIP, iam_role_arn, username, password ):

    redshift_client = boto3.client('redshift')

    try:
        response = redshift_client.create_cluster(
                ClusterIdentifier=cluster_identifier,
                ClusterParameterGroupName=parameter_group,
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

def _describe_cluster(cluster_identifier):
    client = boto3.client('redshift')

    try:
        response = client.describe_clusters(
                        ClusterIdentifier=cluster_identifier)
        return response

    except Exception as e:
        logger.info(f'Error occured while describing cluster: {e}')


def create_parameter_group(parameter_group_name, parameter_group_family, description):
    redshift_client = boto3.client('redshift')

    try:
        response = redshift_client.create_cluster_parameter_group(
            ParameterGroupName=parameter_group_name,
            ParameterGroupFamily=parameter_group_family,
            Description=description
        )
        logger.info(f"Parameter group '{parameter_group_name}' created successfully.")
        return response
    except Exception as e:
        logger.info(f"Error creating parameter group: {e}")

def modify_parameter_group(parameter_group_name, search_path):
    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.modify_cluster_parameter_group(
            ParameterGroupName=parameter_group_name,
            Parameters=[
                {
                    'ParameterName': 'search_path',
                    'ParameterValue': search_path,
                    'ApplyType': 'static'  # 'dynamic' for immediate changes or 'static' for reboot-required changes # This parameter requires reboot
                }
            ]
        )
        logger.info(f"Parameter group '{parameter_group_name}' modified successfully.")
        return response
    except Exception as e:
        logger.info(f"Error modifying parameter group: {e}")

def apply_parameter_group_to_cluster(cluster_identifier, parameter_group_name):
    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.modify_cluster(
            ClusterIdentifier=cluster_identifier,
            ClusterParameterGroupName=parameter_group_name
        )
        logger.info(f"Parameter group '{parameter_group_name}' applied to cluster '{cluster_identifier}' successfully.")
        return response
    except Exception as e:
        logger.info(f"Error applying parameter group to cluster: {e}")



