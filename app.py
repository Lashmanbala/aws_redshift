from ec2_util import allocate_elastic_ip
from iam_util import create_iam_role


# elastic_ip_res = allocate_elastic_ip()
# elastic_ip = elastic_ip_res['PublicIp']

# print(elastic_ip)

role_arn = create_iam_role()
