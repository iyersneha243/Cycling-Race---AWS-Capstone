#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import boto3
import json
import csv
from datetime import datetime
import random
import time

# Environment variables
s3_bucket = os.environ['S3_BUCKET']
s3_key = os.environ['S3_KEY']
kinesis_stream_name = os.environ['KINESIS_STREAM_NAME']

def lambda_handler(event, context):
    # Create a Kinesis client
    kinesis_client = boto3.client('kinesis')

    # Read CSV file from S3 and process each row
    s3 = boto3.resource('s3')
    try:
        obj = s3.Object(s3_bucket, s3_key)
        body = obj.get()['Body']
        
        # Create a CSV reader
        csv_reader = csv.DictReader(body.read().decode('utf-8').splitlines())
        
        for row in csv_reader:
            print("Row:", row)
            
            # Extract device ID from the row
            device_id = next((value for value in row.values() if value.strip()), None)
            
            # Skip processing if device ID is not found
            if not device_id:
                print("Device ID not found in row:", row)
                continue
            
            # Add timestamp to record
            timestamp = datetime.utcnow().isoformat()
            record_with_timestamp = {
                'timestamp': timestamp,
                'data': row  
            }
            
            # Put record into Kinesis Data Stream
            kinesis_client.put_record(
                StreamName=kinesis_stream_name,
                Data=json.dumps(record_with_timestamp),
                PartitionKey=device_id  # Use Device ID as partition key for even distribution
            )
            
            # Sleep for 3 seconds to mimic real-time streaming input
            time.sleep(3)
        
        # Print statement indicating end of execution
        print("All rows processed and ingested into Kinesis Data Stream.")
    except Exception as e:
        print("Error:", e)
        raise e

