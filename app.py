from ec2_util import allocate_elastic_ip
from iam_util import create_iam_role
from secrets_util import create_redshift_secret, get_kms_key_by_alias
from redshift_util import _create_cluster
import dotenv
import os

dotenv.load_dotenv()

# elastic_ip = allocate_elastic_ip()

# role_arn = create_iam_role()

# secret_dict = os.environ.get('REDSHIFT_SECRET_DICT')
# create_redshift_secret(secret_dict)

kms_key_id = get_kms_key_by_alias()
print(kms_key_id)

# elasticIP = '54.174.124.10'
# iam_role_arn = 'arn:aws:iam::585768170668:role/Redshift_All_Commands_Access_Role'
# kms_key_id_arn = 'arn:aws:secretsmanager:us-east-1:585768170668:secret:redshift_secret-gh1lpX'
# res = _create_cluster(elasticIP, iam_role_arn, kms_key_id_arn)
# print(res)

# import boto3

# def list_kms_keys():
#     client = boto3.client('kms')
#     response = client.list_keys()
#     for key in response['Keys']:
#         print(f"KMS Key ID: {key['KeyId']}")
# list_kms_keys()
