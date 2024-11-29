import boto3
import json
import time
from logging_config import logger

def create_iam_role():
    iam_client = boto3.client('iam')

    assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com" 
                        },
                        "Action": "sts:AssumeRole"
                    }
                        ]
                    }
    try:
        # Creating the role
        response = iam_client.create_role(
                    RoleName='Redshift_All_Commands_Access_Role',
                    AssumeRolePolicyDocument=json.dumps(assume_role_policy)
                )
        role_arn = response['Role']['Arn']
        logger.info('IAM role is successfully created: %s', role_arn)

    except iam_client.exceptions.EntityAlreadyExistsException:
        response = iam_client.get_role(RoleName='Redshift_All_Commands_Access_Role')
        role_arn = response['Role']['Arn']        
        logger.warning('Role already exists: %s', role_arn)

    except Exception as e:
        logger.error('Error creating the role: %s', e)

    # Attach the existing policy to the role
    try:
        iam_client.attach_role_policy(
            RoleName='Redshift_All_Commands_Access_Role',
            PolicyArn='arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess'
        )
        logger.info('Policy successfully attached to the role')

    except Exception as e:
        logger.info("Error attaching policy: %s", e)

    time.sleep(5)  # Wait untill the role is created

    return role_arn
