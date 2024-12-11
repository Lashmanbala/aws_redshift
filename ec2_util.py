import boto3
import time
from logging_config import logger


def allocate_elastic_ip():
    ec2_client = boto3.client('ec2')

    try:
        response = ec2_client.allocate_address(Domain='vpc')
        logger.info('ElasticIP is created Successfully: %s', response['PublicIp'])

        time.sleep(5) # wait untill its allocated
        return response['PublicIp']
    
    except Exception as e:
        logger.error('Failed to allocate ElasticIP: %s', e)


def add_ip_to_security_group(security_group_id, ip_address, port):

    ec2_client = boto3.client('ec2')
    
    try:
        cidr_ip = f"{ip_address}/32"  
        
        security_group_id = security_group_id

        response = ec2_client.authorize_security_group_ingress(
                                GroupId=security_group_id,
                                IpProtocol='tcp',
                                FromPort=port,  
                                ToPort=port,
                                CidrIp=cidr_ip
                            )
        logger.info(f"Successfully added inbound rule for IP {cidr_ip} to security group {security_group_id}.")
        return response

    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")