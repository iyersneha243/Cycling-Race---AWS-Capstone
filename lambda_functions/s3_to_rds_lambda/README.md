# S3 to RDS PostgreSQL Lambda Function

This Lambda function downloads a CSV file from an S3 bucket, parses its contents, and inserts the data into a table in an RDS PostgreSQL database.

## Functionality

- **Download from S3:** The function retrieves a CSV file from an S3 bucket using the provided object key.
- **CSV Parsing:** It parses the CSV data into a dictionary format using the `csv.DictReader` class.
- **Database Insertion:** The parsed data is inserted into a table named `static_threshold_data` in an RDS PostgreSQL database.
- **Error Handling:** Error handling is implemented to rollback changes in case of any exceptions during database insertion.

## Configuration

Ensure the following environment variables are set for the Lambda function:

- `DB_HOST`: The hostname of the RDS PostgreSQL instance.
- `DB_NAME`: The name of the PostgreSQL database.
- `DB_USER`: The username for accessing the PostgreSQL database.
- `DB_PASSWORD`: The password for accessing the PostgreSQL database.
- `S3_BUCKET`: The name of the S3 bucket containing the CSV file.
- `S3_OBJECT_KEY`: The key of the CSV file within the S3 bucket.

## Execution

This Lambda function can be triggered by an event such as an S3 object creation event. It will automatically download the CSV file from S3 and insert its contents into the specified PostgreSQL table.

## Requirements

This Lambda function requires the `psycopg2` library to interact with the PostgreSQL database. Ensure that the `psycopg2` library is included in the Lambda function's deployment package.