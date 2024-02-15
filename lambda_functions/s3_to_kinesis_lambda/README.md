# S3 to Kinesis Lambda Function

This Lambda function reads a CSV file from an S3 bucket, processes each row, and ingests the data into a Kinesis Data Stream.

## Configuration

Ensure the following environment variables are set for the Lambda function:

- `S3_BUCKET`: The name of the S3 bucket where the CSV file is stored.
- `S3_KEY`: The key of the CSV file within the S3 bucket.
- `KINESIS_STREAM_NAME`: The name of the Kinesis Data Stream where the data will be ingested.

## Execution

This Lambda function will be triggered automatically when an object is uploaded to the specified S3 bucket. Each row from the CSV file will be processed and ingested into the specified Kinesis Data Stream.

## Notes

- The Lambda function adds a timestamp to each record before ingesting it into Kinesis.
- The partition key for each record in Kinesis is set to the device ID extracted from the CSV row, ensuring even distribution across shards.

## Author

Sneha Iyer

## License

This project is currently not licensed. All rights are reserved by the author.