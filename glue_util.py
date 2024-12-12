import boto3
import time
from logging_config import logger

def create_glue_db(database_name, database_description):
    glue_client = boto3.client('glue')
    try:
        response = glue_client.create_database(
                                                DatabaseInput={
                                                    "Name": database_name,
                                                    "Description": database_description
                                                }
                                            )
        logger.info(f"Database {database_name} created successfully.")
        time.sleep(5)
        return response
    except Exception as e:
        logger.info(f'Error while creating db: {e}')

def create_glue_table(database_name, table_name, columns_list, s3_path):
    glue_client = boto3.client('glue')
    try:
        response = glue_client.create_table(
                                DatabaseName=database_name,
                                TableInput={
                                    "Name": table_name,
                                    "StorageDescriptor": {
                                                        "Columns": columns_list,
                                                        "Location": s3_path,
                                                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                                                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                                                        "SerdeInfo": {
                                                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                                                            "Parameters": {
                                                                "field.delim": ","  # delimiter used in your data
                                                            }
                                                        }
                                                    },
                                    "TableType": "EXTERNAL_TABLE",  # Indicates the table references data in S3
                                    "Parameters": {
                                        "classification": "csv"  # data format
                                    }
                                }
                            )
        time.sleep(5)
        logger.info(f'{table_name} table created successfully')
        return response
    except Exception as e:
        logger.error(f'Error while creating table: {e}')
