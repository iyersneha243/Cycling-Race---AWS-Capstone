#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import boto3
import json
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import col, when
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sneha_gluescript")

# Create a SparkContext
sc = SparkContext()

# Create a GlueContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a Glue job
job = Job(glueContext)
job.init("read_and_write_data")

# Initialize AWS clients
sns = boto3.client('sns', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
kinesis = boto3.client('kinesis', region_name='us-east-1')

# Constants
sns_topic_arn = 'arn:aws:sns:us-east-1:*********:sneha-sns'
stream_name = 'awscapstone-cycling-sneha-streamKinesis'
table_name = 'awscapstone-cycling-sneha-tableDynamoDB'
secret_name = "*********"
region_name = "us-east-1"
tablename = "static_threshold_data"

# Initialize variables
record_count = 0
last_sequence_numbers = {}
shard_iterators = []

def get_secret(secret_name, region_name):
    """
    Retrieve secret from AWS Secrets Manager.
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        logger.info("Successfully retrieved secret.")
        return secret
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        raise e

def read_data_from_postgresql():
    """
    Read data from PostgreSQL RDS.
    """
    try:
        rds_secret = get_secret(secret_name, region_name)
        jdbc_url = f"jdbc:postgresql://{rds_secret['host']}/{rds_secret['dbname']}"
        db_properties = {"user": rds_secret['username'], "password": rds_secret['password'], "driver": "org.postgresql.Driver"}
        df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", tablename).option("user", db_properties["user"]).option("password", db_properties["password"]).option("driver", db_properties["driver"]).load()
        logger.info("Successfully read data from PostgreSQL.")
        logger.info("RDS DataFrame:")
        df.show()  
        return df
    except Exception as e:
        logger.error(f"Error reading data from PostgreSQL: {e}")
        raise e

def process_kinesis_stream():
    """
    Process records from Kinesis stream.
    """
    global record_count, shard_iterators
    try:
        # Initialize Kinesis
        response = kinesis.list_shards(StreamName=stream_name)
        shard_ids = [shard['ShardId'] for shard in response['Shards']]
        shard_iterators = [kinesis.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator'] for shard_id in shard_ids]
        logger.info("Kinesis initialized successfully.")

        while True:
            for i, shard_iterator in enumerate(shard_iterators):
                try:
                    records_response = kinesis.get_records(ShardIterator=shard_iterator, Limit=10)
                    records = records_response.get('Records', [])

                    if not records and records_response.get('NextShardIterator'):
                        shard_iterators[i] = records_response['NextShardIterator']
                        continue
                    
                    shard_iterators[i] = records_response['NextShardIterator']
                    process_records(records, shard_ids[i])

                except Exception as e:
                    logger.error(f"Error processing records: {e}")

            if record_count % 1000 == 0:
                job.commit()
                logger.info("Committed job.")

            time.sleep(3)

    except Exception as e:
        logger.error(f"An error occurred while processing Kinesis stream: {e}")

def process_records(records, shard_id):
    """
    Process records and write to DynamoDB.
    """
    global record_count
    try:
        last_sequence_number = last_sequence_numbers.get(shard_id, None)
        parsed_data = spark.read.json(sc.parallelize([json.loads(record['Data']) for record in records]))
        
        cleaned_df = flatten_and_clean(parsed_data)
        logger.info("Flattening and cleaning DataFrame complete.")

        joined_df = cleaned_df.join(rds_data_frame, cleaned_df.device_id == rds_data_frame.device_id, "inner").drop(rds_data_frame.device_id)
        logger.info("Joining DataFrames complete.")
        logger.info("Joined DataFrame:")
        joined_df.show() 
        
        joined_df = joined_df.withColumn("Zone", when(joined_df['speed_km_per_h'] > joined_df['threshold_speed'], "Red zone").otherwise("Safe zone")).na.fill(0, subset=["speed_km_per_h", "elevation_m", "power_watts"])
        dict_data = joined_df.toJSON().map(lambda x: json.loads(x)).collect()

        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for item in dict_data:
                try:
                    batch.put_item(Item=item)
                    record_count += 1
                    if record_count % 100 == 0:
                        sns.publish(TopicArn=sns_topic_arn, Message=f"{record_count} records have been loaded to DynamoDB.")
                        logger.info(f"Message sent to SNS: {record_count} records have been loaded to DynamoDB.")
                except Exception as e:
                    logger.error(f"Error writing item to DynamoDB: {e}")

        if records:
            last_sequence_numbers[shard_id] = records[-1]['SequenceNumber']
    except Exception as e:
        logger.error(f"Error processing records: {e}")

def flatten_and_clean(parsed_data):
    """
    Flatten and clean DataFrame.
    """
    return parsed_data.selectExpr(
        "timestamp",
        "data['\\ufeffDevice ID'] as device_id",
        "data['Speed (km/h)'] as speed_km_per_h",
        "data['Power (watts)'] as power_watts",
        "data['Elevation (m)'] as elevation_m"
    ).na.fill("NA", subset=["speed_km_per_h", "power_watts", "elevation_m"]) \
        .dropna(subset=["device_id", "timestamp"]) \
        .withColumn("device_id", when(col("device_id").isNotNull(), col("device_id")).otherwise(0).cast(IntegerType())) \
        .withColumn("speed_km_per_h", col("speed_km_per_h").cast(IntegerType())) \
        .withColumn("power_watts", col("power_watts").cast(IntegerType())) \
        .withColumn("elevation_m", col("elevation_m").cast(IntegerType())) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

try:
    rds_data_frame = read_data_from_postgresql()
    logger.info("RDS data read successfully.")
    process_kinesis_stream()

except Exception as e:
    logger.error(f"An error occurred: {e}")

