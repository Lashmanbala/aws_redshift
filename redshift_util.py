import boto3
import time
import psycopg2
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
                # NumberOfNodes=1,  # its already single node in cluster type
                PubliclyAccessible=True,
                ElasticIp=elasticIP,
                IamRoles=[iam_role_arn]
                # LoadSampleData='string'  # optional
            )
        time.sleep(5)
        logger.info(f'Redshift cluster created successfully')
        return response
    
    except Exception as e:
        logger.info(f'Error creating the cluster: {e}')


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

def run_redshift_queries(endpoint, db_name, user_name, password, queriy_list):
    try:
        conn = psycopg2.connect(host = endpoint,
                            port = 5439,
                            database = db_name,  
                            user = user_name,
                            password = password)

        cursor = conn.cursor()

        for query in queriy_list:
            cursor.execute(query)
            if query.strip().upper().startswith('SELECT') or query.strip().upper().startswith('SHOW'):
                results = cursor.fetchall()
                for row in results:
                    print(row)
            logger.info(f'Successfully executed query: {query}')
        conn.commit()

    except Exception as e:
        logger.info(f'Error with exception: {e}')
    finally:
        cursor.close()
        conn.close()

