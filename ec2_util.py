import boto3
import logging
import os
import time

logging.basicConfig(
    filename=os.environ.get('LOG_FILE_PATH'),
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

def allocate_elastic_ip():
    ec2_client = boto3.client('ec2')

    try:
        response = ec2_client.allocate_address(Domain='vpc')
        logger.info('ElasticIP is created Successfully: %s', response['PublicIp'])

        time.sleep(5) # wait untill its allocated
        return response
    
    except Exception as e:
        logger.error('Failed to allocate ElasticIP: %s', e)
