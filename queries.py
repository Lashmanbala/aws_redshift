import psycopg2
from redshift_util import _describe_cluster
from secrets_util import get_secret
from logging_config import logger

def run_queries(endpoint, db_name, user_name, password, queriy_list):
    try:
        conn = psycopg2.connect(host = endpoint,
                            port = 5439,
                            database = db_name,  
                            user = user_name,
                            password = password)

        cursor = conn.cursor()

        for query in queriy_list:
            cursor.execute(query)
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                for row in results:
                    print(row)
            logger.info(f'Successfully executed query: {query}')
        conn.commit()

    except Exception as e:
        logger.info(f'Error with exception: {e}')
    finally:
        cursor.close()
        conn.close()

cluster_identifier = 'retail-cluster'

response = _describe_cluster(cluster_identifier)
endpoint = response['Clusters'][0]['Endpoint']['Address']

redshift_cluster_secret_name = 'redshift_secret'
cluster_secret = get_secret(redshift_cluster_secret_name)
admin_user_name = cluster_secret['username']
admin_password = cluster_secret['password']

redshift_db_secret_name = 'redshift_db_secret'
db_secret = get_secret(redshift_db_secret_name)
db_user_name = db_secret['username']
db_password = db_secret['password']
db_name = 'retail_db'
default_database = 'dev'

# admin_dev_query_list = [f"CREATE DATABASE {db_name};",
#                     f"CREATE USER {db_user_name} WITH PASSWORD '{db_password}';",
#                     f"GRANT ALL ON DATABASE {db_name} TO {db_user_name};"]

# # creating retail_db and retail_user by admin connecting with default db
# run_queries(endpoint, default_database, admin_user_name, admin_password, admin_dev_query_list)    # connecting to default db with admin user

# db_name = 'retail_db'
schema_name = 'retail_schema'

# admin_retail_db_queries = [f"CREATE SCHEMA {schema_name} AUTHORIZATION {db_user_name};",
#                             f"GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO {db_user_name};",
#                             f"SET search_path = '$user', public, {schema_name};"]

# # creating retail_schema by admin connecting with retail_db
# run_queries(endpoint, db_name, admin_user_name, db_password, admin_retail_db_queries)  # connecting to retail_db with admin user

# create_table_queries = [ '''CREATE TABLE retail_db.retail_schema.departments (
            #                         department_id INT NOT NULL,
            #                         department_name VARCHAR(45) NOT NULL,
            #                         PRIMARY KEY (department_id)
            #                         )DISTSTYLE ALL;'''
#                         ,
#                         '''CREATE TABLE retail_db.retail_schema.categories (
            #                         category_id INT NOT NULL,
            #                         category_department_id INT NOT NULL,
            #                         category_name VARCHAR(45) NOT NULL,
            #                         PRIMARY KEY (category_id)
            #                         )DISTSTYLE ALL
            #; ''' ]

# db_name = 'retail_db'
# schema_name = 'retail_schema'
# run_queries(endpoint, db_name, db_user_name, db_password, create_table_queries)  # connecting to retail_db with retail_user 


iam_role_arn = 'arn:aws:iam::585768170668:role/Redshift_All_Commands_Access_Role'

# copy_cmds = [f'''
#             COPY retail_db.retail_schema.departments
#             FROM 's3://redshift-bucket-123/departments/part-00000'
#             IAM_ROLE '{iam_role_arn}'
#             FORMAT CSV
#             ''',
#             f'''
#             COPY retail_db.retail_schema.categories
#             FROM 's3://redshift-bucket-123/categories/part-00000'
#             IAM_ROLE '{iam_role_arn}'
#             FORMAT CSV
#             ''']
# run_queries(endpoint, db_name, db_user_name, db_password, copy_cmds)

# spectrum_queries = [f'''
#                     CREATE EXTERNAL SCHEMA IF NOT EXISTS retail_spectrum2
#                     FROM  DATA CATALOG
#                     DATABASE 'retail_db_redshift1'
#                     IAM_ROLE '{iam_role_arn}'
#                     CREATE EXTERNAL DATABASE IF NOT EXISTS;
#                     '''
                    ]

# res = run_queries(endpoint, db_name, db_user_name, db_password, spectrum_queries)

rds_endpoint = "retail-mysql-db.cj8gyayqy4pf.us-east-1.rds.amazonaws.com"
rds_secret_arn = 'arn:aws:secretsmanager:us-east-1:585768170668:secret:rds_mysql_redshift_secret-dvud2L'

federated_queries = [f'''
                    CREATE EXTERNAL SCHEMA IF NOT EXISTS retail_federated_queries2
                    FROM MYSQL
                    DATABASE 'retail_db_redshift'
                    IAM_ROLE '{iam_role_arn}'
                    URI '{rds_endpoint}'
                    SECRET_ARN '{rds_secret_arn}';
                    ''']

res = run_queries(endpoint, db_name, db_user_name, db_password, federated_queries)