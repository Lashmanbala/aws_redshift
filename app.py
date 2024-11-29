from ec2_util import allocate_elastic_ip
from iam_util import create_iam_role
from secrets_util import create_redshift_secret, get_kms_key_for_secret
import dotenv
import os

dotenv.load_dotenv()

elastic_ip_res = allocate_elastic_ip()

role_arn = create_iam_role()

secret_dict = os.environ.get('REDSHIFT_SECRET_DICT')
create_redshift_secret(secret_dict)

kms_key_id_arn = get_kms_key_for_secret()

