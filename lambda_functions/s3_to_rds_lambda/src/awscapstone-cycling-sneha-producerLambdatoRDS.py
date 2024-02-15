#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv
import boto3
import os
import psycopg2
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Get environment variables
    host = os.environ['DB_HOST']
    database = os.environ['DB_NAME']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    bucket_name = os.environ['S3_BUCKET']
    object_key = os.environ['S3_OBJECT_KEY']

    # Download S3 object data
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    body = response['Body'].read().decode('utf-8-sig')

    # Log content length and content
    logger.info(f"Content length: {len(body)}")
    logger.info(f"Content: {body}")

    # Parse CSV data
    csv_data = csv.DictReader(body.splitlines())

    # Connect to RDS PostgreSQL
    conn = psycopg2.connect(host=host, database=database, user=user, password=password)
    cur = conn.cursor()

    try:
        # Iterate over CSV data and insert into table
        for row in csv_data:
            device_id = row['Device ID']
            cyclist = row['Cyclist']
            threshold_speed = row['Threshold Speed']
            sql = "INSERT INTO static_threshold_data (device_id, cyclist, threshold_speed) VALUES (%s, %s, %s)"
            cur.execute(sql, (device_id, cyclist, threshold_speed))

        # Commit changes and close connection
        conn.commit()
        return {
            'statusCode': 200,
            'body': 'Data inserted successfully!'
        }
    except Exception as e:
        conn.rollback()  # Rollback changes if an error occurs
        return {
            'statusCode': 500,
            'body': f'Error inserting data: {str(e)}'
        }
    finally:
        cur.close()
        conn.close()

