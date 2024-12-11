import boto3
from secrets_util import get_secret
from logging_config import logger
import time
from ec2_util import add_ip_to_security_group
import dotenv
import os
import pymysql
import pandas as pd
from sqlalchemy import create_engine

dotenv.load_dotenv()

def create_rds_db(db_instance_identifier, admin_user_name, admin_password, db_name):
    rds_client = boto3.client('rds')
    try:
        response = rds_client.create_db_instance(
                                    DBInstanceIdentifier = db_instance_identifier,
                                    AllocatedStorage = 20,
                                    DBInstanceClass = "db.t3.micro",
                                    Engine = "mysql",
                                    MasterUsername = admin_user_name,
                                    MasterUserPassword = admin_password,
                                    DBName = db_name,
                                    PubliclyAccessible = True, 
                                    StorageType = 'gp2',  
                                )
        time.sleep(5)
        logger.info(f"Creating RDS MySQL instance {db_instance_identifier}...")
        return response
    except Exception as e:
        logger.error(f"Error creating RDS instance: {e}")

def describe_instance(db_instance_identifier):
    rds_client = boto3.client('rds')
    try:
        response = rds_client.describe_db_instances(
                                    DBInstanceIdentifier=db_instance_identifier)
        return response
    except Exception as e:
        logger.info(f'Error describing instance: {e}')

def run_queries(endpoint, db_name, user_name, password, queriy_list):
    try:
        conn = pymysql.connect(
                                host = endpoint,
                                port = port,
                                user = user_name,
                                password = password,
                                database = db_name
                            )
        
        cursor = conn.cursor()

        for query in queriy_list:
            cursor.execute(query)
            if query.strip().upper().startswith('SELECT') or query.strip().upper().startswith('SHOW'):
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

def insert_data_into_table(user_name, password, endpoint, database, csv_file_path, table_name, column_names):
    try:
        engine = create_engine(f"mysql+pymysql://{user_name}:{password}@{endpoint}/{database}")
        logger.info("Connected to MySQL RDS.")
        
        df = pd.read_csv(csv_file_path, header=None)
        logger.info("CSV file loaded into a DataFrame successfully.")

        df.columns = column_names

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=10000)
        logger.info(f"Data loaded successfully into the table '{table_name}'.")
    
    except Exception as e:
        logger.error(f"Error occured: {e}")
    finally:
        engine.dispose() 
        logger.info("MySQL connection closed.")

'''
Getting credentials for mysql db from secrets manager
'''

mysql_secret_name = 'rds_mysql_secret'
mysql_secret = get_secret(mysql_secret_name)
admin_user_name = mysql_secret['username']
admin_password = mysql_secret['password']

'''
Creating mysql instance in rds
'''

db_instance_identifier = "retail-mysql-db"
db_name = "retail_db_redshift"

res = create_rds_db(db_instance_identifier, admin_user_name, admin_password, db_name)

'''
Getting instance details
'''

res = describe_instance(db_instance_identifier)
endpoint = res['DBInstances'][0]['Endpoint']['Address'] 
security_group_id = res['DBInstances'][0]['VpcSecurityGroups'][0]['VpcSecurityGroupId']

'''
Updating the inbound rule of yhe sequrity group
'''

# security_group_id = 'sg-03b4db065615f216b'
ip_address = os.environ.get('IP_ADDRESS')
port = 3306
res = add_ip_to_security_group(security_group_id, ip_address, port)

'''
Creating tables
'''

# endpoint = "retail-mysql-db.cj8gyayqy4pf.us-east-1.rds.amazonaws.com"
port = 3306
database = "retail_db_redshift"
user_name = admin_user_name
password = admin_password
queriy_list  = [
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
                ); ''',
                'SHOW TABLES;'
                ]
run_queries(endpoint, db_name, user_name, password, queriy_list)

'''
Inserting data into the tables
'''

customers_file_path = '/home/bala/code/projects/redshift_project/data/retail_db/customers/part-00000'
table_name = "customers"
column_names = ["customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode"] 

insert_data_into_table(user_name, password, endpoint, database, customers_file_path, table_name, column_names)


products_file_path = '/home/bala/code/projects/redshift_project/data/retail_db/products/part-00000'
table_name = "products"
column_names = ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"] 

insert_data_into_table(user_name, password, endpoint, database, products_file_path, table_name, column_names)

'''
Validating the tables
'''

queriy_list = ['SELECT COUNT(*) FROM customers', 'SELECT COUNT(*) FROM products']
run_queries(endpoint, db_name, user_name, password, queriy_list)