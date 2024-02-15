#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import boto3
import json
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import col, when, lag, sum, unix_timestamp
from pyspark.sql.window import Window
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create a logger
logger = logging.getLogger("sneha_gluescript")

# Create a SparkContext
sc = SparkContext()

# Create a GlueContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a Glue job
job = Job(glueContext)
job.init("read_and_write_data")

# Initialize empty list for shard iterators
shard_iterators = []

# Retrieve credentials from AWS Secrets Manager
def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            return secret
        else:
            logger.error("No secret string found in response.")
            return None

# Get RDS credentials from Secrets Manager
secret_name = "snehapostgreRDSdbsecret"
region_name = "us-east-1"
rds_secret = get_secret(secret_name, region_name)

# Create JDBC URL
jdbc_url = "jdbc:postgresql://" + rds_secret['host'] + "/" + rds_secret['dbname']

# Read data from PostgreSQL
db_properties = {
    "user": rds_secret['username'],
    "password": rds_secret['password'],
    "driver": "org.postgresql.Driver"
}
tablename = "static_threshold_data"

try:
    logger.info("Reading data from PostgreSQL...")
    rds_data_frame = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", tablename) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .load()
    logger.info("Data read from PostgreSQL successfully.")
except Exception as e:
    logger.error(f"Error reading data from PostgreSQL: {e}")

# Initialize SNS client
sns = boto3.client('sns', region_name='us-east-1')

# Define the SNS topic ARN
sns_topic_arn = 'arn:aws:sns:us-east-1:896889985719:sneha-sns'

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='us-east-1')

# Define the name of your Kinesis data stream
stream_name = 'awscapstone-cycling-sneha-streamKinesis'

# Counter for tracking the number of records loaded
record_count = 0

# Initialize empty dictionary to store last processed sequence number for each shard
last_sequence_numbers = {}

def get_shard_ids(stream_name):
    response = kinesis.list_shards(StreamName=stream_name)
    shard_ids = [shard['ShardId'] for shard in response['Shards']]
    return shard_ids

try:
    # Retrieve data from the stream
    logger.info("Retrieving data from the stream...")
    shard_ids = get_shard_ids(stream_name)
    shard_iterators = []  # Reset shard_iterators list
    for shard_id in shard_ids:
        shard_iterator = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )['ShardIterator']
        shard_iterators.append(shard_iterator)
    logger.info("Data retrieved from the stream successfully.")
except Exception as e:
    logger.error(f"Error retrieving data from the stream: {e}")

# Continuously read data from the stream
while True:
    # Periodically update shard IDs and iterators
    if record_count % 1000 == 0:
        try:
            logger.info("Updating shard IDs and iterators...")
            shard_ids = get_shard_ids(stream_name)
            shard_iterators = []  # Reset shard_iterators list
            for shard_id in shard_ids:
                shard_iterator = kinesis.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'
                )['ShardIterator']
                shard_iterators.append(shard_iterator)
            logger.info("Shard IDs and iterators updated successfully.")
        except Exception as e:
            logger.error(f"Error updating shard IDs and iterators: {e}")

    for i, shard_iterator in enumerate(shard_iterators):
        try:
            # Retrieve last processed sequence number for the shard
            last_sequence_number = last_sequence_numbers.get(shard_ids[i], None)
            
            # Retrieve records from Kinesis
            records_response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=10  # Adjust the limit based on your needs
            )
            records = records_response.get('Records', [])
            
            # Refresh shard iterator if it has expired
            if not records and records_response.get('NextShardIterator'):
                shard_iterators[i] = records_response['NextShardIterator']
                continue
            
            # Update shard iterator every time records are retrieved
            shard_iterators[i] = records_response['NextShardIterator']

            # Flatten the JSON and rename columns
            parsed_data = spark.read.json(sc.parallelize([json.loads(record['Data']) for record in records]))

            flattened_df = parsed_data \
                .selectExpr(
                    "timestamp",
                    "data['\\ufeffDevice ID'] as device_id",
                    "data['Speed (km/h)'] as speed_km_per_h",
                    "data['Power (watts)'] as power_watts",
                    "data['Elevation (m)'] as elevation_m"
                )

            # Apply cleaning operations to the flattened DataFrame
            cleaned_df = flattened_df \
                .na.fill("NA", subset=["speed_km_per_h", "power_watts", "elevation_m"]) \
                .dropna(subset=["device_id", "timestamp"]) \
                .withColumn("device_id", when(col("device_id").isNotNull(), col("device_id")).otherwise(0).cast(IntegerType())) \
                .withColumn("speed_km_per_h", col("speed_km_per_h").cast(IntegerType())) \
                .withColumn("power_watts", col("power_watts").cast(IntegerType())) \
                .withColumn("elevation_m", col("elevation_m").cast(IntegerType())) \
                .withColumn("timestamp", col("timestamp").cast(TimestampType()))

            # Join both DataFrames
            joined_df = cleaned_df.join(rds_data_frame, cleaned_df.device_id == rds_data_frame.device_id, "inner").drop(rds_data_frame.device_id)

            # Calculate speed change, elevation change, power change, and time difference
            window_spec = Window.partitionBy("device_id").orderBy("timestamp")
            joined_df = joined_df \
                .withColumn("speed_change", col("speed_km_per_h") - lag("speed_km_per_h").over(window_spec)) \
                .withColumn("elevation_change", col("elevation_m") - lag("elevation_m").over(window_spec)) \
                .withColumn("power_change", col("power_watts") - lag("power_watts").over(window_spec)) \
                .withColumn("time_diff", (unix_timestamp("timestamp") - lag(unix_timestamp("timestamp")).over(window_spec)))

            # Calculate total time in red zone in seconds
            total_time_in_red_zone_sec = joined_df \
                .filter(col("speed_km_per_h") > col("threshold_speed")) \
                .groupBy("device_id") \
                .agg(sum("time_diff").alias("total_time_in_red_zone_sec"))

            # Join total_time_in_red_zone_sec back to joined_df
            joined_df = joined_df.join(total_time_in_red_zone_sec, on="device_id", how="left")

            # Add column for Red Zone as 1 or 0
            joined_df = joined_df.withColumn("is_red_zone", when(joined_df['speed_km_per_h'] > joined_df['threshold_speed'], 1).otherwise(0))

            # Replace null values with 0 for columns
            joined_df = joined_df \
                .na.fill(0, subset=["speed_change", "elevation_change", "power_change", "time_diff", "total_time_in_red_zone_sec"])

            # Print the joined DataFrame
            logger.info("Joined DataFrame:")
            joined_df.show()

            # Convert DataFrame to dictionary
            dict_data = joined_df.toJSON().map(lambda x: json.loads(x)).collect()
            logger.info("Data converted to dictionary format.")

            # Initialize DynamoDB client
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

            # DynamoDB table name
            table_name = 'awscapstone-cycling-sneha-tableDynamoDB'
            table = dynamodb.Table(table_name)
            logger.info("DynamoDB client initialized successfully.")
            logger.info(f"DynamoDB table name: {table_name}")

            # Batch insert items into the DynamoDB table
            with table.batch_writer() as batch:
                for item in dict_data:
                    try:
                        batch.put_item(Item=item)
                        record_count += 1
                        if record_count % 100 == 0:
                            # Send a message to SNS with the count of records loaded
                            message = f"{record_count} records have been loaded to DynamoDB."
                            sns.publish(
                                TopicArn=sns_topic_arn,
                                Message=message
                            )
                            logger.info(f"Message sent to SNS: {message}")
                    except Exception as e:
                        logger.error(f"Error writing item to DynamoDB: {e}")

            # Update last processed sequence number for the shard
            if records:
                last_sequence_numbers[shard_ids[i]] = records[-1]['SequenceNumber']

        except Exception as e:
            logger.error(f"Error processing records: {e}")

    # Commit the job every 5 minutes
    if record_count % 1000 == 0:
        job.commit()
    
    # Wait for 3 seconds before processing the next batch of records
    time.sleep(3)

