from ec2_util import allocate_elastic_ip
from iam_util import create_iam_role
from secrets_util import create_redshift_secret, get_kms_key_by_alias, get_secret
from redshift_util import _create_cluster
import dotenv
import os

dotenv.load_dotenv()

# elastic_ip = allocate_elastic_ip()

# secret_dict = os.environ.get('REDSHIFT_SECRET_DICT')
# create_redshift_secret(secret_dict)

# kms_key_id, kms_key_arn = get_kms_key_by_alias()
# print(kms_key_id)

# role_arn = create_iam_role()

secret = get_secret()
user_name = secret['username']
password = secret['password']

# elasticIP = '54.174.124.10'
# iam_role_arn = 'arn:aws:iam::585768170668:role/Redshift_All_Commands_Access_Role'
# res = _create_cluster(elasticIP, iam_role_arn, user_name, password)
# print(res)


