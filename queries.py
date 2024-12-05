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

admin_dev_query_list = [f"CREATE DATABASE {db_name};",
                    f"CREATE USER {db_user_name} WITH PASSWORD '{db_password}';",
                    f"GRANT ALL ON DATABASE {db_name} TO {db_user_name};"]

# creating retail_db and retail_user by admin connecting with default db
run_queries(endpoint, default_database, admin_user_name, admin_password, admin_dev_query_list)

db_name = 'retail_db'
schema_name = 'retail_schema'

admin_retail_db_queries = [f"CREATE SCHEMA {schema_name} AUTHORIZATION {db_user_name};",
                            f"GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO {db_user_name};",
                            f"SET search_path = '$user', public, {schema_name};"]

# creating retail_schema by admin connecting with retail_db
run_queries(endpoint, db_name, db_user_name, db_password, admin_retail_db_queries)

