"""
# Consumer Complaints ETL Pipeline - S3 to Snowflake

This DAG orchestrates the extraction, transformation, and loading (ETL) of consumer
complaint data from the CFPB (Consumer Financial Protection Bureau) API into Snowflake
via S3.

## Overview

The pipeline performs the following steps:
1. **Extract**: Fetches consumer complaint data from the CFPB API for multiple companies
2. **Upload to S3**: Saves data to S3 with automatic cleanup of old files
3. **Stage**: Creates Snowflake external stage pointing to S3
4. **Load**: Uses COPY INTO to load data from S3 to Snowflake
5. **Validate**: Performs data quality checks

## Architecture

CFPB API → Local CSV → S3 Bucket → Snowflake (via COPY INTO)

## Configuration

### Airflow Connections Required:
- `aws_default`: AWS connection for S3 access
- `snowflake_default`: Snowflake connection

### Airflow Variables (Optional):
- `snowflake_database`: Target Snowflake database (default: CONSUMER_DATA)
- `snowflake_schema`: Target Snowflake schema (default: PUBLIC)
- `snowflake_warehouse`: Snowflake warehouse to use (default: COMPUTE_WH)
- `aws_s3_bucket`: S3 bucket name (required if not in connection)

### Company Configuration:
Edit `include/config.py` to add/modify companies to fetch data for.

## Features

- ✅ Multi-company data extraction
- ✅ Automatic S3 file cleanup (keeps only latest per company)
- ✅ Smart file selection (only loads most recent files)
- ✅ Append mode (no data loss)
- ✅ Comprehensive error handling and logging
- ✅ Modular design for easy maintenance
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime as pendulum_datetime

# Add include directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))

from cfpb_api_client import CFPBAPIClient
from config import (
    COMPANY_CONFIG,
    S3_BUCKET_PREFIX,
    S3_CONN_ID,
    SNOWFLAKE_CONN_ID,
    SNOWFLAKE_STAGE_NAME,
)
from s3_loader import S3Loader
from s3_to_snowflake import S3ToSnowflakeLoader

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_DATABASE = "CONSUMER_DATA"
DEFAULT_SCHEMA = "PUBLIC"
DEFAULT_WAREHOUSE = "COMPUTE_WH"
DEFAULT_TABLE_NAME = "CONSUMER_COMPLAINTS"

# Snowflake table DDL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS CONSUMER_COMPLAINTS (
    complaint_id VARCHAR(50) PRIMARY KEY,
    date_received TIMESTAMP,
    date_sent_to_company TIMESTAMP,
    product VARCHAR(255),
    sub_product VARCHAR(255),
    issue VARCHAR(500),
    sub_issue VARCHAR(500),
    company VARCHAR(500),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    tags VARCHAR(255),
    consumer_consent_provided VARCHAR(100),
    submitted_via VARCHAR(100),
    company_response_to_consumer VARCHAR(255),
    timely_response VARCHAR(10),
    consumer_disputed VARCHAR(10),
    complaint_what_happened TEXT,
    company_public_response TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Consumer complaints data from CFPB loaded from S3'
"""


@dag(
    dag_id="consumer_complaints_elt_pipeline",
    start_date=pendulum_datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "data_engineering",
        "retries": 3,
        "retry_delay": 300,  # 5 minutes
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["etl", "cfpb", "consumer_complaints", "s3", "snowflake"],
    description="ETL pipeline for CFPB consumer complaint data via S3 to Snowflake",
)
def consumer_complaints_elt_pipeline():
    """Main DAG function for consumer complaints ETL pipeline via S3."""

    @task
    def extract_complaints_for_companies(**context) -> Dict[str, str]:
        """
        Extract consumer complaints from CFPB API for multiple companies.

        Fetches complaint data for each company defined in COMPANY_CONFIG
        and saves them as CSV files in a temporary directory.

        Returns:
            Dictionary mapping company names to local CSV file paths
        """
        try:
            logger.info(f"Starting complaint extraction for {len(COMPANY_CONFIG)} companies")

            # Create temporary directory for CSV files
            temp_dir = Path("/tmp/consumer_complaints")
            temp_dir.mkdir(exist_ok=True)

            api_client = CFPBAPIClient()
            company_files = {}

            try:
                for config in COMPANY_CONFIG:
                    company_name = config["company_name"]
                    start_date = config["start_date"]
                    end_date = config["end_date"]

                    logger.info(f"Fetching complaints for: {company_name}")
                    logger.info(f"  Date range: {start_date} to {end_date}")

                    # Fetch complaints for this company
                    complaints = api_client.get_complaints_by_company(
                        company_name=company_name,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    if complaints:
                        # Convert to DataFrame and save as CSV
                        df = pd.DataFrame(complaints)

                        # Sanitize filename
                        filename = company_name.replace(" ", "_").lower()
                        file_path = temp_dir / f"{filename}_complaints.csv"

                        df.to_csv(file_path, index=False)
                        company_files[company_name] = str(file_path)

                        logger.info(f"✓ Saved {len(df)} complaints for {company_name}")
                    else:
                        logger.warning(f"⚠ No data returned for {company_name}")

                logger.info(f"✓ Extraction complete. Processed {len(company_files)} companies")

                # Push stats to XCom
                context["ti"].xcom_push(
                    key="extraction_stats",
                    value={
                        "companies_processed": len(company_files),
                        "extraction_timestamp": datetime.now().isoformat(),
                    },
                )

                return company_files

            finally:
                api_client.close()

        except Exception as e:
            logger.error(f"Failed to extract complaints: {e}", exc_info=True)
            raise AirflowException(f"Complaint extraction failed: {e}")

    @task
    def upload_to_s3(company_files: Dict[str, str], **context) -> List[Dict[str, str]]:
        """
        Upload CSV files to S3 with timestamped names and cleanup old files.

        Args:
            company_files: Dictionary mapping company names to local file paths

        Returns:
            List of dictionaries with upload info (company, s3_key, size_mb)
        """
        try:
            if not company_files:
                logger.warning("No files to upload to S3")
                return []

            # Get S3 configuration
            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
            bucket_name = Variable.get("aws_s3_bucket", None)

            if not bucket_name:
                # Try to get from connection
                connection = s3_hook.get_connection(S3_CONN_ID)
                bucket_name = connection.extra_dejson.get("bucket_name")

            if not bucket_name:
                raise ValueError(
                    "S3 bucket name not configured. Configure it using one of these methods:\n"
                    "1. Set Airflow Variable 'aws_s3_bucket' via UI (Admin → Variables)\n"
                    "2. Set via CLI: airflow variables set aws_s3_bucket your-bucket-name\n"
                    "3. Add 'bucket_name' to aws_default connection Extra JSON"
                )

            logger.info(f"Uploading {len(company_files)} files to S3 bucket: {bucket_name}")

            # Initialize S3Loader
            s3_loader = S3Loader(
                s3_hook=s3_hook,
                bucket_name=bucket_name,
                prefix=S3_BUCKET_PREFIX,
            )

            # Upload files
            uploaded_files = s3_loader.upload_files(company_files)

            # Push stats to XCom
            context["ti"].xcom_push(
                key="upload_stats",
                value={
                    "files_uploaded": len(uploaded_files),
                    "upload_timestamp": datetime.now().isoformat(),
                },
            )

            return uploaded_files

        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}", exc_info=True)
            raise AirflowException(f"S3 upload failed: {e}")

    @task
    def create_snowflake_table_and_stage(**context):
        """
        Create Snowflake table and S3 external stage if they don't exist.

        This task:
        1. Creates the CONSUMER_COMPLAINTS table
        2. Creates an external stage pointing to the S3 bucket
        """
        try:
            # Get Snowflake configuration
            database = Variable.get("snowflake_database", DEFAULT_DATABASE)
            schema = Variable.get("snowflake_schema", DEFAULT_SCHEMA)
            warehouse = Variable.get("snowflake_warehouse", DEFAULT_WAREHOUSE)

            # Get S3 bucket name
            bucket_name = Variable.get("aws_s3_bucket", None)

            # Initialize S3 hook for connection fallback
            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

            if not bucket_name:
                # Try to get from connection
                connection = s3_hook.get_connection(S3_CONN_ID)
                bucket_name = connection.extra_dejson.get("bucket_name")

            if not bucket_name:
                raise ValueError(
                    "S3 bucket name not configured. Configure it using one of these methods:\n"
                    "1. Set Airflow Variable 'aws_s3_bucket' via UI (Admin → Variables)\n"
                    "2. Set via CLI: airflow variables set aws_s3_bucket your-bucket-name\n"
                    "3. Add 'bucket_name' to aws_default connection Extra JSON"
                )

            logger.info(f"Setting up Snowflake: {database}.{schema}.{DEFAULT_TABLE_NAME}")

            # Initialize Snowflake hook
            snowflake_hook = SnowflakeHook(
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse=warehouse,
                database=database,
                schema=schema,
            )

            # Initialize S3ToSnowflakeLoader
            loader = S3ToSnowflakeLoader(
                snowflake_hook=snowflake_hook,
                s3_hook=s3_hook,
                database=database,
                schema=schema,
                warehouse=warehouse,
            )

            # Create table and stage
            loader.create_table_and_stage(
                table_name=DEFAULT_TABLE_NAME,
                stage_name=SNOWFLAKE_STAGE_NAME,
                bucket_name=bucket_name,
                s3_prefix=S3_BUCKET_PREFIX,
                create_table_sql=CREATE_TABLE_SQL,
            )

            logger.info("✓ Snowflake setup complete")

        except Exception as e:
            logger.error(f"Failed to setup Snowflake: {e}", exc_info=True)
            raise AirflowException(f"Snowflake setup failed: {e}")

    @task
    def load_from_s3_to_snowflake(
        uploaded_files: List[Dict[str, str]], **context
    ) -> Dict[str, Any]:
        """
        Load data from S3 to Snowflake using COPY INTO command.

        This task identifies the most recent file for each company and loads
        only those files to prevent duplicate data loads.

        Args:
            uploaded_files: List of uploaded file info

        Returns:
            Load statistics dictionary
        """
        try:
            if not uploaded_files:
                logger.warning("No files to load from S3")
                return {"rows_loaded": 0, "files_loaded": 0}

            # Get Snowflake configuration
            database = Variable.get("snowflake_database", DEFAULT_DATABASE)
            schema = Variable.get("snowflake_schema", DEFAULT_SCHEMA)
            warehouse = Variable.get("snowflake_warehouse", DEFAULT_WAREHOUSE)

            logger.info(f"Loading data from S3 to {database}.{schema}.{DEFAULT_TABLE_NAME}")

            # Initialize hooks
            snowflake_hook = SnowflakeHook(
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse=warehouse,
                database=database,
                schema=schema,
            )

            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

            # Initialize S3ToSnowflakeLoader
            loader = S3ToSnowflakeLoader(
                snowflake_hook=snowflake_hook,
                s3_hook=s3_hook,
                database=database,
                schema=schema,
                warehouse=warehouse,
            )

            # Get latest files by company
            latest_files = loader.get_latest_files_by_company(
                stage_name=SNOWFLAKE_STAGE_NAME,
                prefix=S3_BUCKET_PREFIX,
            )

            if not latest_files:
                logger.warning("No files found in stage")
                return {"rows_loaded": 0, "files_loaded": 0}

            # Copy data from S3 to Snowflake
            total_files, total_rows, total_errors = loader.copy_from_s3_to_snowflake(
                table_name=DEFAULT_TABLE_NAME,
                stage_name=SNOWFLAKE_STAGE_NAME,
                file_list=latest_files,
            )

            load_stats = {
                "rows_loaded": total_rows,
                "files_loaded": total_files,
                "errors": total_errors,
                "table": f"{database}.{schema}.{DEFAULT_TABLE_NAME}",
                "load_timestamp": datetime.now().isoformat(),
            }

            # Push stats to XCom
            context["ti"].xcom_push(key="load_stats", value=load_stats)

            return load_stats

        except Exception as e:
            logger.error(f"Failed to load from S3 to Snowflake: {e}", exc_info=True)
            raise AirflowException(f"S3 to Snowflake load failed: {e}")

    @task
    def validate_data_quality(load_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform data quality validation on loaded data.

        Checks:
        - Row count verification
        - Duplicate complaint IDs
        - Null values in critical fields
        - Date range validation
        - Top companies statistics

        Args:
            load_stats: Statistics from the load operation

        Returns:
            Validation results dictionary
        """
        try:
            logger.info("Starting data quality validation")

            # Get Snowflake configuration
            database = Variable.get("snowflake_database", DEFAULT_DATABASE)
            schema = Variable.get("snowflake_schema", DEFAULT_SCHEMA)
            warehouse = Variable.get("snowflake_warehouse", DEFAULT_WAREHOUSE)

            # Initialize hooks
            snowflake_hook = SnowflakeHook(
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse=warehouse,
                database=database,
                schema=schema,
            )

            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

            # Initialize S3ToSnowflakeLoader
            loader = S3ToSnowflakeLoader(
                snowflake_hook=snowflake_hook,
                s3_hook=s3_hook,
                database=database,
                schema=schema,
                warehouse=warehouse,
            )

            # Validate data quality
            validation_results = loader.validate_data_quality(
                table_name=DEFAULT_TABLE_NAME,
                rows_loaded_this_run=load_stats.get("rows_loaded", 0),
            )

            # Add files loaded count
            validation_results["files_loaded_this_run"] = load_stats.get("files_loaded", 0)

            return validation_results

        except Exception as e:
            logger.error(f"Data quality validation failed: {e}", exc_info=True)
            raise AirflowException(f"Validation failed: {e}")

    # Define task dependencies
    company_files = extract_complaints_for_companies()
    uploaded_files = upload_to_s3(company_files)
    table_and_stage_created = create_snowflake_table_and_stage()
    load_stats = load_from_s3_to_snowflake(uploaded_files)
    validation_results = validate_data_quality(load_stats)

    # Set explicit dependencies
    uploaded_files >> table_and_stage_created >> load_stats


# Instantiate the DAG
consumer_complaints_elt_pipeline()
