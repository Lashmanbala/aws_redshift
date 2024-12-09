import boto3
import time
from logging_config import logger

def create_glue_crawler(crawler_name, role, database_name, s3_target_path):
    glue_client = boto3.client('glue')
    try:
        res = glue_client.create_crawler(
            Name=crawler_name,
            Role=role,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_target_path
                    }
                ]
            },
            Description='Crawler to crawl data in S3 and populate Glue Data Catalog',
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
                }
            # , Schedule='cron(0 12 * * ? *)',  # Optional schedule for periodic runs

            )
        time.sleep(5) # wait untill crawler created
        logger.info(f'Glue crawler created successfully {crawler_name}')
        return res
    except Exception as e:
        logger.error(f'Error occured while creating crawler: {e}')



def start_glue_crawler(crawler_name):
    glue_client = boto3.client('glue')

    res = glue_client.start_crawler(Name=crawler_name)
    logger.info(f'{crawler_name} started successfully')

    return res

crawler_name = 'retail_crawler'
role = 'arn:aws:iam::585768170668:role/s3AccessForGlue'
database_name = 'retail_db_glue'
s3_target_path = 's3://redshift-bucket-123/'

# response = create_glue_crawler(crawler_name, role, database_name, s3_target_path)
# print(response)

res = start_glue_crawler(crawler_name)
print(res)