# Sneha's AWS Glue Script

## Overview
This AWS Glue script is designed to read data from a Kinesis data stream, process it using Spark, and load it into a DynamoDB table. It performs various data transformations and calculations to derive insights from the streaming data.

## Features
- Reads data from a Kinesis data stream.
- Cleans and processes the data using Spark SQL.
- Joins the streaming data with static threshold data stored in an RDS PostgreSQL database.
- Calculates metrics such as speed change, elevation change, power change, and time spent in the "red zone" for each cyclist.
- Loads the processed data into a DynamoDB table.

## Requirements
- Python 3.x
- Apache Spark
- AWS Glue
- AWS Kinesis
- AWS DynamoDB
- AWS RDS PostgreSQL

## Usage
1. Ensure that you have the necessary AWS permissions to run Glue jobs, access Kinesis streams, and interact with DynamoDB and RDS.
2. Set up the required AWS resources such as Kinesis data streams, DynamoDB tables, and RDS PostgreSQL databases.
3. Configure the script with the appropriate AWS credentials, stream names, table names, and other parameters.
4. Deploy the script as an AWS Glue job and schedule it to run periodically or in response to events.
5. Monitor the job execution logs and DynamoDB table for successful data ingestion.

## Dependencies
- boto3: Python SDK for AWS
- awsglue: AWS Glue libraries for Spark
- pyspark: Apache Spark libraries for Python

## Author
This script was authored by Sneha Iyer.

## License
All rights reserved. No license is included with this project.