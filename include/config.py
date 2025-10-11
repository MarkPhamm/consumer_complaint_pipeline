"""Configuration for Consumer Complaints ETL Pipeline."""

# Company configuration for data extraction
COMPANY_CONFIG = [
    {
        "company_name": "jpmorgan",
        "start_date": "2024-01-01",
        "end_date": "2025-12-31",
    },
    {
        "company_name": "bank of america",
        "start_date": "2024-01-01",
        "end_date": "2025-12-31",
    },
    # Add more companies as needed
]

# S3 Configuration
S3_BUCKET_PREFIX = "consumer_complaints"
S3_CONN_ID = "aws_default"

# Snowflake Configuration
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_STAGE_NAME = "CONSUMER_COMPLAINTS_S3_STAGE"
