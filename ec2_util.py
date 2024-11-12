import boto3
import logging

logging.basicConfig(
    filename="/home/bala/code/projects/redshift_project/app.log",
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
        return response
    
    except Exception as e:
        logger.error('Failed to allocate ElasticIP: %s', e)
