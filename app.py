from ec2_util import allocate_elastic_ip

elastic_ip_res = allocate_elastic_ip()
elastic_ip = elastic_ip_res['PublicIp']

print(elastic_ip)