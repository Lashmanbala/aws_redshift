from ec2_util import allocate_elastic_ip
from iam_util import create_iam_role
from secrets_util import create_redshift_secret, get_secret
from redshift_util import (_create_cluster, add_ip_to_redshift_security_group, _describe_cluster, create_parameter_group, modify_parameter_group, 
                           apply_parameter_group_to_cluster)
import dotenv
import os

dotenv.load_dotenv()

elastic_ip = allocate_elastic_ip()

parameter_group_name = 'retail-pg'
parameter_group_family = 'redshift-1.0'
description = 'Custom parameter group for retail-cluster'
res = create_parameter_group(parameter_group_name, parameter_group_family, description)
print(res)

search_path = '$user, public, retail_schema'
res = modify_parameter_group(parameter_group_name, search_path)
print(res)

# redshift_cluster_secret_name = 'redshift_secret'
# redshift_cluster_secret_dict = os.environ.get('REDSHIFT_SECRET_DICT')
# create_redshift_secret(redshift_cluster_secret_name, redshift_cluster_secret_dict)

# role_arn = create_iam_role()

# cluster_secret = get_secret(redshift_cluster_secret_name)
# cluster_user_name = cluster_secret['username']
# cluster_password = cluster_secret['password']

cluster_identifier = 'retail-cluster'
elasticIP = '54.174.124.10'
iam_role_arn = 'arn:aws:iam::585768170668:role/Redshift_All_Commands_Access_Role'
res = _create_cluster(parameter_group_name, cluster_identifier, elasticIP, iam_role_arn, cluster_user_name, cluster_password)
print(res)

# security_group_id = res['Cluster']['VpcSecurityGroups'][0]['VpcSecurityGroupId']

# security_group_id = 'sg-03b4db065615f216b'
# ip_address = os.environ.get('IP_ADDRESS')
# res = add_ip_to_redshift_security_group(security_group_id, ip_address)
# print(res)

# redshift_db_secret_name = 'redshift_db_secret'
# redshift_db_secret_dict = os.environ.get('REDSHIFT_DB_SECRET_DICT')
# create_redshift_secret(redshift_db_secret_name, redshift_db_secret_dict)

# db_secret = get_secret(redshift_db_secret_name)
# cluster_user_name = db_secret['username']
# cluster_password = db_secret['password']
# print(cluster_user_name)
# print(cluster_password)



# res = apply_parameter_group_to_cluster(cluster_identifier, parameter_group_name)
# print(res)                         
