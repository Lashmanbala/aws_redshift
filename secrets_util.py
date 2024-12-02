import boto3
import json
from logging_config import logger
import time

def create_redshift_secret(secret_dict):
    client = boto3.client('secretsmanager')

    secret_name = 'redshift_secret'
    
    try:
        response = client.create_secret(
            Name=secret_name,
            Description='Secret for redshift database',
            SecretString=json.dumps(secret_dict)
        )

        logger.info(f"Secret rds_secret created successfully.")
        time.sleep(5)       # wait untill secret is created
        return response
    
    except client.exceptions.ResourceExistsException:
        logger.warning(f"The secret '{secret_name}' already exists.")

    except Exception as e:
        logger.error(f"Error creating the secret: {e}")


def get_secret(secret_name='redshift_secret'):
    client = boto3.client('secretsmanager')
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        raw_secret_string = response['SecretString']
        corrected_secret_string = raw_secret_string.strip('"').replace("'", '"')
        secret = json.loads(corrected_secret_string)
        logger.info('Secret retreived successfully')
        return secret
    except Exception as e:
        logger.exception("Failed to retrieve secret")
        raise e