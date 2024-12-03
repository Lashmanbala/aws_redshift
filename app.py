from ec2_util import allocate_elastic_ip
from iam_util import create_iam_role
from secrets_util import create_redshift_secret, get_secret
from redshift_util import _create_cluster, add_ip_to_redshift_security_group
import dotenv
import os

dotenv.load_dotenv()

# elastic_ip = allocate_elastic_ip()

# secret_dict = os.environ.get('REDSHIFT_SECRET_DICT')
# create_redshift_secret(secret_dict)

# kms_key_id, kms_key_arn = get_kms_key_by_alias()
# print(kms_key_id)

# role_arn = create_iam_role()

# secret = get_secret()
# user_name = secret['username']
# password = secret['password']

cluster_identifier = 'retail-cluster'
# elasticIP = '54.174.124.10'
# iam_role_arn = 'arn:aws:iam::585768170668:role/Redshift_All_Commands_Access_Role'
# res = _create_cluster(cluster_identifier, elasticIP, iam_role_arn, user_name, password)
# print(res)
# security_group_id = res['Cluster']['VpcSecurityGroups'][0]['VpcSecurityGroupId']

security_group_id = 'sg-03b4db065615f216b'
ip_address = os.environ.get('IP_ADDRESS')
res = add_ip_to_redshift_security_group(security_group_id, ip_address)
print(res)