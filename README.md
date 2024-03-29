# Cycling Race AWS Capstone Project

## Overview
This project implements a data pipeline for processing cycling race data using various AWS services such as AWS Glue, AWS Lambda, Amazon Kinesis, Amazon RDS (PostgreSQL), Amazon DynamoDB, Amazon SNS, and AWS Secrets Manager. The pipeline is designed to ingest, transform, and analyze cycling race data, providing insights into race performance and real-time monitoring capabilities.

## Project Structure
The project directory structure is organized as follows:

Cycling-Race---AWS-Capstone/
├── README.md
├── data/
│   ├── race_data.xlsx
│   ├── threshold_speeds.csv
├── lambda_functions/
│   ├── s3_to_kinesis_lambda/
│   │   ├── src/
│   │   │   ├── lambda_function.py
│   │   │   └── requirements.txt
│   │   └── README.md
│   ├── s3_to_rds_lambda/
│       ├── src/
│       │   ├── lambda_function.py
│       │   └── requirements.txt
│       └── README.md
├── glue_scripts/
│   ├── data_transformation_script.py
│   └── README.md
├── powerbi_dashboards/
│   ├── cycling_race_dashboard.pbix
│   └── README.md
├── .gitignore

## Setup Instructions
1. **Clone the Repository**: Clone this repository to your local machine.

2. **Configure AWS Services**: Set up the necessary AWS services (Amazon Kinesis, Amazon RDS, Amazon DynamoDB, AWS Secrets Manager, etc.) and configure access permissions.

3. **Prepare Data**: Place the cycling race data files (`race_data.xlsx`, `threshold_speeds.csv`, etc.) in the `data/` directory.

4. **Set Up AWS Glue and Lambda Functions**: Follow the instructions in the respective README files (`lambda_functions/README.md`, `glue_scripts/README.md`) to set up AWS Glue jobs and Lambda functions.

5. **Configure Secrets**: Store RDS credentials in AWS Secrets Manager and update the secret name in the Lambda function code.

6. **Deploy Power BI Dashboard**: Deploy the Power BI dashboard (`cycling_race_dashboard.pbix`) to your Power BI service and configure data connections.

## Usage
- **Ingest Data**: Use AWS Lambda functions (`s3_to_kinesis_lambda`, `s3_to_rds_lambda`, etc.) to ingest data from S3 into Amazon Kinesis streams or RDS databases.
- **Transform Data**: Use AWS Glue scripts (`data_transformation_script.py`) to transform raw data into a structured format.
- **Analyze Data**: Analyze the processed data using Power BI (`cycling_race_dashboard.pbix`) for insights and visualization.
- **Real-time Monitoring**: Monitor real-time race data using Amazon Kinesis streams and DynamoDB.

## Author
Sneha Iyer

## Disclaimer
This project is provided without any explicit or implied warranties or guarantees. The author assumes no responsibility or liability for any errors or omissions in the content of the project. Any usage of the project is at the user's own risk.
