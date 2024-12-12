import boto3
from logging_config import logger
import time
import pymysql
import pandas as pd
from sqlalchemy import create_engine


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

def run_mysql_queries(endpoint, db_name, user_name, password, queriy_list):
    try:
        conn = pymysql.connect(
                                host = endpoint,
                                port = 3306,
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
