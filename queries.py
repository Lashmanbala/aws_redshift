from redshift_util import _describe_cluster, run_redshift_queries
from secrets_util import get_secret
from glue_util import create_glue_table
from rds_mysql import describe_instance, run_mysql_queries, insert_data_into_table
from logging_config import logger
import dotenv
import os

def redshift_queries():
    '''
    Getting the cluster endpoint
    '''
    redshift_cluster_identifier = 'retail-cluster'
    response = _describe_cluster(redshift_cluster_identifier)
    global redshift_endpoint
    redshift_endpoint = response['Clusters'][0]['Endpoint']['Address']

    '''
    Getting redshift admin credentials
    '''
    redshift_cluster_secret_name = os.environ.get('REDSHIFT_CLUSTER_SECRET_NAME')
    cluster_secret = get_secret(redshift_cluster_secret_name)[0]
    redshift_admin_user_name = cluster_secret['username']
    redshift_admin_password = cluster_secret['password']

    '''
    Getting redshift database user credentials
    '''
    redshift_db_secret_name = os.environ.get('REDSHIFT_DB_SECRET_NAME')
    db_secret = get_secret(redshift_db_secret_name)[0]
    global redshift_db_user_name
    redshift_db_user_name = db_secret['username']
    global redshift_db_password
    redshift_db_password = db_secret['password']

    '''
    Creating a user with credentials and granting all previleges to the user
    '''
    global redshift_db_name
    redshift_db_name = 'retail_db'
    redshift_default_database = 'dev'
    redshift_admin_dev_query_list = [f"CREATE DATABASE {redshift_db_name};",
                            f"CREATE USER {redshift_db_user_name} WITH PASSWORD '{redshift_db_password}';",
                            f"GRANT ALL ON DATABASE {redshift_db_name} TO {redshift_db_user_name};"]

    # creating retail_db and retail_user by admin user connecting with default db
    run_redshift_queries(redshift_endpoint, redshift_default_database, redshift_admin_user_name, redshift_admin_password, redshift_admin_dev_query_list)

    '''
    Creating the schema and setting up in search path
    '''
    redshift_db_name = 'retail_db'
    redshift_schema_name = 'retail_schema'
    redshift_admin_retail_db_queries = [f"CREATE SCHEMA {redshift_schema_name} AUTHORIZATION {redshift_db_user_name};",
                                        f"GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO {redshift_db_user_name};",
                                        f"SET search_path = '$user', public, {redshift_schema_name};"]

    # creating retail_schema by admin connecting with retail_db
    run_redshift_queries(redshift_endpoint, redshift_db_name, redshift_admin_user_name, redshift_db_password, redshift_admin_retail_db_queries) 

    '''
    Creating redshift tables with diststyles
    '''
    redshift_create_table_queries = ['''CREATE TABLE retail_db.retail_schema.departments (
                                        department_id INT NOT NULL,
                                        department_name VARCHAR(45) NOT NULL,
                                        PRIMARY KEY (department_id)
                                        )DISTSTYLE ALL;'''
                                    ,
                                    '''CREATE TABLE retail_db.retail_schema.categories (
                                                category_id INT NOT NULL,
                                                category_department_id INT NOT NULL,
                                                category_name VARCHAR(45) NOT NULL,
                                                PRIMARY KEY (category_id)
                                                )DISTSTYLE ALL; ''' 
                                    ]

    redshift_db_name = 'retail_db'
    redshift_schema_name = 'retail_schema'
    run_redshift_queries(redshift_endpoint, redshift_db_name, redshift_db_user_name, redshift_db_password, redshift_create_table_queries)  # connecting to retail_db with retail_user 


    '''
    Running COPY commands to copy the data from s3 into redshift tables 
    '''
    global redshift_iam_role_arn
    redshift_iam_role_arn = os.environ.get('REDSHIFT_IAM_ROLE_ARN') # Its been set in app.py file
    departments_s3_path = os.environ.get('DEPARTMENTS_S3_PATH')
    categories_s3_path = os.environ.get('CATEGORIES_S3_PATH')
    redshift_copy_cmds = [f'''
                            COPY retail_db.retail_schema.departments
                            FROM '{departments_s3_path}'
                            IAM_ROLE '{redshift_iam_role_arn}'
                            FORMAT CSV
                            ''',
                            f'''
                            COPY retail_db.retail_schema.categories
                            FROM '{categories_s3_path}'
                            IAM_ROLE '{redshift_iam_role_arn}'
                            FORMAT CSV
                            ''']
    run_redshift_queries(redshift_endpoint, redshift_db_name, redshift_db_user_name, redshift_db_password, redshift_copy_cmds)


def glue_queries():
    '''
    Creating glue tables for spectrum
    '''
    global glue_database_name
    glue_database_name = 'retail_db_redshift1'     # as per the default redshift role ploicy resource arn by aws, the glue database name should contain the string redshift in it
    glue_table_name = 'redshift_orders' # as per the default redshift role ploicy resource arn by aws, the glue table name should contain the string redshift in it
    orders_s3_path = os.environ.get('ORDERS_S3_PATH')
    clm_list = [{'Name': 'order_id','Type': 'INT'},
                {'Name': 'order_date','Type': 'TIMESTAMP'},
                {'Name': 'order_customer_id','Type': 'INT'},
                {'Name': 'order_status','Type': 'STRING'}]

    res = create_glue_table(glue_database_name, glue_table_name, clm_list, orders_s3_path)

    glue_table_name = 'redshift_order_items' # as per the default redshift role ploicy resource arn by aws, the glue table name should contain the string redshift in it
    order_items_s3_path = os.environ.get('ORDER_ITEMS_S3_PATH')
    clm_list = [{'Name': 'order_item_id','Type': 'INT'},
                {'Name': 'order_item_order_id','Type': 'INT'},
                {'Name': 'order_item_product_id','Type': 'INT'},
                {'Name': 'order_item_quantity','Type': 'INT'},
                {'Name': 'order_item_subtotal','Type': 'FLOAT'},
                {'Name': 'order_item_product_price','Type': 'FLOAT'}]

    res = create_glue_table(glue_database_name, glue_table_name, clm_list, order_items_s3_path)

def mysql_queries():
    '''
    Getting credentials for mysql db from secrets manager
    '''
    mysql_secret_name = os.environ.get('MYSQL_SECRET_NAME')
    mysql_secret = get_secret(mysql_secret_name)[0]
    mysql_user_name = mysql_secret['username']
    mysql_password = mysql_secret['password']

    '''
    Creating Mysql tables for federated queries
    '''
    db_instance_identifier = "retail-mysql-db"
    res = describe_instance(db_instance_identifier)
    global mysql_endpoint
    mysql_endpoint = res['DBInstances'][0]['Endpoint']['Address']
    # mysql_endpoint = "retail-mysql-db.cj8gyayqy4pf.us-east-1.rds.amazonaws.com"
    global mysql_db_name
    mysql_db_name = "retail_db_redshift"

    mysql_queriy_list  = [
                    '''
                    CREATE TABLE retail_db_redshift.products (
                    product_id INT NOT NULL,
                    product_category_id INT NOT NULL,
                    product_name VARCHAR(45) NOT NULL,
                    product_description VARCHAR(255) NOT NULL,
                    product_price FLOAT NOT NULL,
                    product_image VARCHAR(255) NOT NULL,
                    PRIMARY KEY (product_id)
                    );
                    '''
                    ,
                    '''
                    CREATE TABLE retail_db_redshift.customers (
                    customer_id INT NOT NULL,
                    customer_fname VARCHAR(45) NOT NULL,
                    customer_lname VARCHAR(45) NOT NULL,
                    customer_email VARCHAR(45) NOT NULL,
                    customer_password VARCHAR(45) NOT NULL,
                    customer_street VARCHAR(255) NOT NULL,
                    customer_city VARCHAR(45) NOT NULL,
                    customer_state VARCHAR(45) NOT NULL,
                    customer_zipcode VARCHAR(45) NOT NULL,
                    PRIMARY KEY (customer_id)
                    ); '''
                    ,
                    'SHOW TABLES;'  # for validation
                    ]
    run_mysql_queries(mysql_endpoint, mysql_db_name, mysql_user_name, mysql_password, mysql_queriy_list)

    '''
    Inserting data into Mysql tables
    '''
    customers_file_path = os.environ.get('CUSTOMERS_LOCAL_FILE_PATH')
    mysql_table_name = "customers"
    column_names = ["customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode"] 

    insert_data_into_table(mysql_user_name, mysql_password, mysql_endpoint, mysql_db_name, customers_file_path, mysql_table_name, column_names)

    products_file_path = os.environ.get('PRODUCTS_LOCAL_FILE_PATH')
    mysql_table_name = "products"
    column_names = ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"] 

    insert_data_into_table(mysql_user_name, mysql_password, mysql_endpoint, mysql_db_name, products_file_path, mysql_table_name, column_names)

    '''
    Validating Mysql tables
    '''
    mysql_queriy_list = ['SELECT COUNT(*) FROM customers', 'SELECT COUNT(*) FROM products']
    run_mysql_queries(mysql_endpoint, mysql_db_name, mysql_user_name, mysql_password, mysql_queriy_list)

def redshift_copy_commands():
    '''
    Creating external schema with glue data catalog for spectrum queries 
    '''
    external_schema_for_spectrum_queries = [f'''
                                            CREATE EXTERNAL SCHEMA IF NOT EXISTS retail_spectrum2
                                            FROM  DATA CATALOG
                                            DATABASE '{glue_database_name}'
                                            IAM_ROLE '{redshift_iam_role_arn}'
                                            CREATE EXTERNAL DATABASE IF NOT EXISTS;
                                            '''
                                            ]

    res = run_redshift_queries(redshift_endpoint, redshift_db_name, redshift_db_user_name, redshift_db_password, external_schema_for_spectrum_queries)

    '''
    Creating external schema with rds mysql db for federated queries 
    '''
    mysql_secret_name = 'rds_mysql_secret'
    rds_secret_arn = get_secret(mysql_secret_name)[1]
    external_schema_for_federated_queries = [f'''
                                            CREATE EXTERNAL SCHEMA IF NOT EXISTS retail_federated_queries2
                                            FROM MYSQL
                                            DATABASE '{mysql_db_name}'
                                            IAM_ROLE '{redshift_iam_role_arn}'
                                            URI '{mysql_endpoint}'
                                            SECRET_ARN '{rds_secret_arn}';
                                            ''']

    res = run_redshift_queries(redshift_endpoint, redshift_db_name, redshift_db_user_name, redshift_db_password, external_schema_for_federated_queries)


def validate_redshift():
    '''
    Validating Spectrum and federated:
        Get order count per customer for the month of 2014 jan.
        Orders table from glue db (Spectrum layer)
        Customer table from Mysql (Federated query)
    '''
    join_query = [
                    '''
                    SELECT  c.customer_id,
                        c.customer_fname,
                        c.customer_lname,
                        o.order_date,
                        COUNT(o.order_id) AS customer_order_count
                    FROM retail_spectrum2.redshift_orders o 
                         JOIN retail_federated_queries2.customers c
                            ON o.order_customer_id = c.customer_id
                            AND TO_CHAR(o.order_date, 'YYYYMM') = '201401'
                    GROUP BY 1,2,3,4
                    ORDER BY 5 DESC, 1 ASC
                    LIMIT 10
                    '''
                  ]

    run_redshift_queries(redshift_endpoint, redshift_db_name, redshift_db_user_name, redshift_db_password, join_query)


def main():
    redshift_queries()
    glue_queries()
    mysql_queries()
    redshift_copy_commands()
    validate_redshift()


if __name__ == "__main__":

    dotenv.load_dotenv()

    main()