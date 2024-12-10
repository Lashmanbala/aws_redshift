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
        logger.info(f'{table_name} table created successfully')
        return response
    except Exception as e:
        logger.error(f'Error while creating table: {e}')


database_name = 'retail_db_redshift1'     # as per the default redshift role ploicy resource arn by aws the glue database name should contain the string redshift in it
database_description = "Database for redshift spectrum"

res = create_glue_db(database_name, database_description)
print(res)

clm_list = [{'Name': 'order_id','Type': 'INT'},
            {'Name': 'order_date','Type': 'TIMESTAMP'},
            {'Name': 'order_customer_id','Type': 'INT'},
            {'Name': 'order_status','Type': 'STRING'}]

table_name = 'redshift_orders'
s3_path = 's3://redshift-bucket-123/landing/orders/'

res = create_glue_table(database_name, table_name, clm_list, s3_path)
print(res)

clm_list = [{'Name': 'order_item_id','Type': 'INT'},
            {'Name': 'order_item_order_id','Type': 'INT'},
            {'Name': 'order_item_product_id','Type': 'INT'},
            {'Name': 'order_item_quantity','Type': 'INT'},
            {'Name': 'order_item_subtotal','Type': 'FLOAT'},
            {'Name': 'order_item_product_price','Type': 'FLOAT'}]

table_name = 'redshift_order_items' # as per the default redshift role ploicy resource arn by aws the glue table name should contain the string redshift in it
s3_path = 's3://redshift-bucket-123/landing/order_items/'

res = create_glue_table(database_name, table_name, clm_list, s3_path)
print(res)