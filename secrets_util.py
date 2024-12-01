import boto3
import json
from logging_config import logger
import time

def create_redshift_secret(secret_dict):
    client = boto3.client('secretsmanager')
    
    try:
        response = client.create_secret(
            Name='redshift_secret',
            Description='Secret for redshift database',
            SecretString=json.dumps(secret_dict)
        )

        logger.info(f"Secret rds_secret created successfully.")
        time.sleep(5)       # wait untill secret is created
        return response
    
    except client.exceptions.ResourceExistsException:
        logger.warning(f"The secret '{response['Name']}' already exists.")

    except Exception as e:
        logger.error(f"Error creating the secret: {e}")


def get_kms_key_by_alias():
    client = boto3.client('kms')

    try:
        response = client.list_aliases()
        for alias in response['Aliases']:
            if alias['AliasName'] == 'alias/aws/secretsmanager':
                key_id = alias['TargetKeyId']
                logger.info(f"KeyId retrieved successfully.")
                return key_id

    except Exception as e:
        logger.error(f"Error getting the keyId: {e}")

