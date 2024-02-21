#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import boto3
import json
import csv
from datetime import datetime
import time
import logging

# Environment variables
s3_bucket = os.environ['S3_BUCKET']
s3_key = os.environ['S3_KEY']
kinesis_stream_name = os.environ['KINESIS_STREAM_NAME']
sns_arn = os.environ['SNS_ARN']  
max_iterations = 2  

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Create a Kinesis client
    kinesis_client = boto3.client('kinesis')

    # Read CSV file from S3 and process each row
    s3 = boto3.resource('s3')
    try:
        total_records = 0
        for iteration in range(max_iterations):
            logger.info("Iteration: %d", iteration + 1)
            obj = s3.Object(s3_bucket, s3_key)
            body = obj.get()['Body']
            
            # Create a CSV reader
            csv_reader = csv.DictReader(body.read().decode('utf-8').splitlines())
            
            for row in csv_reader:
                logger.info("Processing row: %s", row)
                
                # Extract device ID from the row
                device_id = next((value for value in row.values() if value.strip()), None)
                
                # Skip processing if device ID is not found
                if not device_id:
                    logger.warning("Device ID not found in row: %s", row)
                    continue
                
                # Add timestamp to record
                timestamp = datetime.utcnow().isoformat()
                record_with_timestamp = {
                    'timestamp': timestamp,
                    'data': row,
                    'device_id': device_id
                }
                
                # Put record into Kinesis Data Stream
                kinesis_client.put_record(
                    StreamName=kinesis_stream_name,
                    Data=json.dumps(record_with_timestamp),
                    PartitionKey=device_id + ':' +timestamp 
                )
                total_records += 1
                
                # Sleep for 3 seconds to mimic real-time streaming input
                time.sleep(3)
            
            logger.info("All records have been added for iteration %d.", iteration + 1)
        
        # Publish message to SNS after all iterations
        sns_client = boto3.client('sns')
        sns_client.publish(
            TopicArn=sns_arn,
            Message="Total records added: {}.".format(total_records)
        )
        
        # Print statement indicating end of execution
        logger.info("All iterations completed. Total records added: %d.", total_records)
    except Exception as e:
        logger.error("Error: %s", e)
        raise e

