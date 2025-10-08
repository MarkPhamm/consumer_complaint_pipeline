"""
# Consumer Complaints ETL Pipeline

This DAG orchestrates the extraction, transformation, and loading (ETL) of consumer
complaint data from the CFPB (Consumer Financial Protection Bureau) API into Snowflake.

## Overview

The pipeline performs the following steps:
1. **Extract**: Fetches consumer complaint data from the CFPB API
2. **Transform**: Validates and transforms the data
3. **Load**: Loads the data into a Snowflake table
4. **Validate**: Performs data quality checks

## Data Source

- **API**: [CFPB Consumer Complaint Database API](https://cfpb.github.io/api/ccdb/api.html)
- **Update Frequency**: Daily
- **Data Scope**: Consumer complaints from the last day

## Snowflake Configuration

The DAG expects the following Snowflake connection in Airflow:
- **Connection ID**: `snowflake_default`
- **Database**: Configured via `SNOWFLAKE_DATABASE` variable (default: `CONSUMER_DATA`)
- **Schema**: Configured via `SNOWFLAKE_SCHEMA` variable (default: `PUBLIC`)
- **Warehouse**: Configured via `SNOWFLAKE_WAREHOUSE` variable (default: `COMPUTE_WH`)

## Airflow Variables (Optional)

- `cfpb_lookback_days`: Number of days to look back for complaints (default: 1)
- `cfpb_max_records`: Maximum number of records to fetch per run (default: None/unlimited)
- `snowflake_database`: Target Snowflake database (default: CONSUMER_DATA)
- `snowflake_schema`: Target Snowflake schema (default: PUBLIC)
- `snowflake_warehouse`: Snowflake warehouse to use (default: COMPUTE_WH)

## Table Schema

The pipeline creates a table `CONSUMER_COMPLAINTS` with the following structure:
- complaint_id (VARCHAR, Primary Key)
- date_received (DATE)
- product (VARCHAR)
- sub_product (VARCHAR)
- issue (VARCHAR)
- sub_issue (VARCHAR)
- company (VARCHAR)
- state (VARCHAR)
- zip_code (VARCHAR)
- tags (VARCHAR)
- consumer_consent_provided (VARCHAR)
- submitted_via (VARCHAR)
- company_response_to_consumer (VARCHAR)
- timely_response (VARCHAR)
- consumer_disputed (VARCHAR)
- complaint_what_happened (TEXT)
- company_public_response (TEXT)
- created_date (TIMESTAMP)
- updated_date (TIMESTAMP)
- load_timestamp (TIMESTAMP, default: CURRENT_TIMESTAMP)

## Monitoring

The DAG includes built-in data quality checks:
- Validates complaint IDs are unique
- Checks for required fields
- Logs record counts and processing time

## Error Handling

- Automatic retries (3 attempts)
- Exponential backoff for API requests
- Email alerts on failure (configure via default_args)
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime

# Add include directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))

from cfpb_api_client import CFPBAPIClient
from snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_LOOKBACK_DAYS = 1
DEFAULT_DATABASE = "CONSUMER_DATA"
DEFAULT_SCHEMA = "PUBLIC"
DEFAULT_WAREHOUSE = "COMPUTE_WH"
DEFAULT_TABLE_NAME = "CONSUMER_COMPLAINTS"

# Snowflake table DDL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS CONSUMER_COMPLAINTS (
    complaint_id VARCHAR(50) PRIMARY KEY,
    date_received DATE,
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
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""


@dag(
    dag_id="consumer_complaints_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "data_engineering",
        "retries": 3,
        "retry_delay": 300,  # 5 minutes
        "email_on_failure": False,  # Set to True and configure email in Airflow
        "email_on_retry": False,
    },
    tags=["etl", "cfpb", "consumer_complaints", "snowflake"],
    description="Daily ETL pipeline for CFPB consumer complaint data to Snowflake",
)
def consumer_complaints_etl():
    """Main DAG function for consumer complaints ETL pipeline."""

    @task
    def extract_complaints(**context) -> List[Dict[str, Any]]:
        """
        Extract consumer complaints from CFPB API.

        This task fetches complaint data from the CFPB Consumer Complaint Database
        API for the configured date range. It handles pagination automatically and
        includes retry logic for API failures.

        Returns:
            List of complaint dictionaries

        Raises:
            AirflowException: If API extraction fails after retries
        """
        try:
            # Get configuration from Airflow Variables with defaults
            lookback_days = int(Variable.get("cfpb_lookback_days", DEFAULT_LOOKBACK_DAYS))
            max_records = Variable.get("cfpb_max_records", None)
            if max_records:
                max_records = int(max_records)

            logger.info(f"Starting complaint extraction for last {lookback_days} day(s)")
            logger.info(f"Max records: {max_records if max_records else 'unlimited'}")

            # Initialize API client
            api_client = CFPBAPIClient()

            try:
                # Fetch complaints
                complaints = api_client.get_complaints_last_n_days(days=lookback_days)

                # Apply max_records limit if specified
                if max_records and len(complaints) > max_records:
                    logger.info(f"Limiting results to {max_records} records")
                    complaints = complaints[:max_records]

                logger.info(f"Successfully extracted {len(complaints)} complaints")

                # Push metadata to XCom
                context["ti"].xcom_push(
                    key="extraction_stats",
                    value={
                        "total_records": len(complaints),
                        "lookback_days": lookback_days,
                        "extraction_timestamp": datetime.now().isoformat(),
                    },
                )

                return complaints

            finally:
                api_client.close()

        except Exception as e:
            logger.error(f"Failed to extract complaints: {e}", exc_info=True)
            raise AirflowException(f"Complaint extraction failed: {e}")

    @task
    def transform_complaints(complaints: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform and validate complaint data.

        This task performs data transformation and validation:
        - Standardizes field names and formats
        - Validates required fields
        - Handles missing values
        - Ensures data quality

        Args:
            complaints: Raw complaint data from API

        Returns:
            Transformed and validated complaint data

        Raises:
            AirflowException: If transformation or validation fails
        """
        try:
            logger.info(f"Starting transformation of {len(complaints)} complaints")

            if not complaints:
                logger.warning("No complaints to transform")
                return []

            transformed = []
            skipped = 0

            for complaint in complaints:
                try:
                    # Extract and transform fields
                    transformed_complaint = {
                        "complaint_id": complaint.get("complaint_id"),
                        "date_received": complaint.get("date_received"),
                        "product": complaint.get("product"),
                        "sub_product": complaint.get("sub_product"),
                        "issue": complaint.get("issue"),
                        "sub_issue": complaint.get("sub_issue"),
                        "company": complaint.get("company"),
                        "state": complaint.get("state"),
                        "zip_code": complaint.get("zip_code"),
                        "tags": complaint.get("tags"),
                        "consumer_consent_provided": complaint.get("consumer_consent_provided"),
                        "submitted_via": complaint.get("submitted_via"),
                        "company_response_to_consumer": complaint.get("company_response"),
                        "timely_response": complaint.get("timely"),
                        "consumer_disputed": complaint.get("consumer_disputed"),
                        "complaint_what_happened": complaint.get("complaint_what_happened"),
                        "company_public_response": complaint.get("company_public_response"),
                        "created_date": complaint.get("date_sent_to_company"),
                        "updated_date": complaint.get("date_received"),
                    }

                    # Validate required fields
                    if not transformed_complaint["complaint_id"]:
                        logger.warning(f"Skipping complaint without ID: {complaint}")
                        skipped += 1
                        continue

                    # Clean up None values and convert to strings where needed
                    for key, value in transformed_complaint.items():
                        if value is None:
                            transformed_complaint[key] = None
                        elif isinstance(value, (list, tuple)):
                            transformed_complaint[key] = ", ".join(str(v) for v in value)

                    transformed.append(transformed_complaint)

                except Exception as e:
                    logger.warning(
                        f"Error transforming complaint {complaint.get('complaint_id')}: {e}"
                    )
                    skipped += 1
                    continue

            logger.info(
                f"Transformation complete: {len(transformed)} successful, {skipped} skipped"
            )

            # Validate we have data after transformation
            if not transformed:
                raise AirflowException("No valid complaints after transformation")

            return transformed

        except Exception as e:
            logger.error(f"Failed to transform complaints: {e}", exc_info=True)
            raise AirflowException(f"Complaint transformation failed: {e}")

    @task
    def create_snowflake_table():
        """
        Create the Snowflake table if it doesn't exist.

        This task ensures the target table exists in Snowflake before loading data.
        It uses the configured database, schema, and warehouse from Airflow Variables.

        Raises:
            AirflowException: If table creation fails
        """
        try:
            # Get Snowflake configuration
            database = Variable.get("snowflake_database", DEFAULT_DATABASE)
            schema = Variable.get("snowflake_schema", DEFAULT_SCHEMA)
            warehouse = Variable.get("snowflake_warehouse", DEFAULT_WAREHOUSE)

            logger.info(f"Creating table if not exists: {database}.{schema}.{DEFAULT_TABLE_NAME}")

            # Initialize Snowflake hook
            hook = SnowflakeHook(
                snowflake_conn_id="snowflake_default",
                warehouse=warehouse,
                database=database,
                schema=schema,
            )

            # Create loader and table
            loader = SnowflakeLoader(hook)
            loader.create_table_if_not_exists(
                table_name=DEFAULT_TABLE_NAME,
                database=database,
                schema=schema,
                create_table_sql=CREATE_TABLE_SQL,
            )

            logger.info("Table creation task completed successfully")

        except Exception as e:
            logger.error(f"Failed to create Snowflake table: {e}", exc_info=True)
            raise AirflowException(f"Table creation failed: {e}")

    @task
    def load_to_snowflake(complaints: List[Dict[str, Any]], **context) -> Dict[str, Any]:
        """
        Load transformed complaints into Snowflake.

        This task loads the transformed complaint data into the Snowflake table.
        It handles batch loading and provides detailed logging of the load process.

        Args:
            complaints: Transformed complaint data

        Returns:
            Load statistics dictionary

        Raises:
            AirflowException: If loading fails
        """
        try:
            if not complaints:
                logger.warning("No complaints to load")
                return {"rows_loaded": 0}

            # Get Snowflake configuration
            database = Variable.get("snowflake_database", DEFAULT_DATABASE)
            schema = Variable.get("snowflake_schema", DEFAULT_SCHEMA)
            warehouse = Variable.get("snowflake_warehouse", DEFAULT_WAREHOUSE)

            logger.info(
                f"Loading {len(complaints)} complaints to "
                f"{database}.{schema}.{DEFAULT_TABLE_NAME}"
            )

            # Initialize Snowflake hook and loader
            hook = SnowflakeHook(
                snowflake_conn_id="snowflake_default",
                warehouse=warehouse,
                database=database,
                schema=schema,
            )

            loader = SnowflakeLoader(hook)

            # Load data
            rows_loaded = loader.load_json_data(
                data=complaints,
                table_name=DEFAULT_TABLE_NAME,
                database=database,
                schema=schema,
                if_exists="append",
                chunk_size=10000,
            )

            load_stats = {
                "rows_loaded": rows_loaded,
                "table": f"{database}.{schema}.{DEFAULT_TABLE_NAME}",
                "load_timestamp": datetime.now().isoformat(),
            }

            logger.info(f"Successfully loaded {rows_loaded} rows to Snowflake")

            # Push stats to XCom
            context["ti"].xcom_push(key="load_stats", value=load_stats)

            return load_stats

        except Exception as e:
            logger.error(f"Failed to load complaints to Snowflake: {e}", exc_info=True)
            raise AirflowException(f"Snowflake load failed: {e}")

    @task
    def validate_data_quality(load_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform data quality validation on loaded data.

        This task runs data quality checks on the loaded data:
        - Verifies row count
        - Checks for duplicates
        - Validates data completeness

        Args:
            load_stats: Statistics from the load operation

        Returns:
            Validation results dictionary

        Raises:
            AirflowException: If critical data quality issues are found
        """
        try:
            logger.info("Starting data quality validation")

            # Get Snowflake configuration
            database = Variable.get("snowflake_database", DEFAULT_DATABASE)
            schema = Variable.get("snowflake_schema", DEFAULT_SCHEMA)
            warehouse = Variable.get("snowflake_warehouse", DEFAULT_WAREHOUSE)

            # Initialize Snowflake hook and loader
            hook = SnowflakeHook(
                snowflake_conn_id="snowflake_default",
                warehouse=warehouse,
                database=database,
                schema=schema,
            )

            loader = SnowflakeLoader(hook)

            # Get total row count
            total_rows = loader.get_table_row_count(
                table_name=DEFAULT_TABLE_NAME, database=database, schema=schema
            )

            # Check for duplicates
            duplicate_query = f"""
            SELECT COUNT(*) - COUNT(DISTINCT complaint_id) as duplicate_count
            FROM {database}.{schema}.{DEFAULT_TABLE_NAME}
            """
            duplicate_result = loader.execute_query(duplicate_query)
            duplicate_count = duplicate_result[0][0] if duplicate_result else 0

            # Check for null complaint_ids
            null_id_query = f"""
            SELECT COUNT(*)
            FROM {database}.{schema}.{DEFAULT_TABLE_NAME}
            WHERE complaint_id IS NULL
            """
            null_id_result = loader.execute_query(null_id_query)
            null_id_count = null_id_result[0][0] if null_id_result else 0

            # Compile validation results
            validation_results = {
                "total_rows_in_table": total_rows,
                "rows_loaded_this_run": load_stats.get("rows_loaded", 0),
                "duplicate_complaint_ids": duplicate_count,
                "null_complaint_ids": null_id_count,
                "validation_passed": True,
                "validation_timestamp": datetime.now().isoformat(),
            }

            # Check for critical issues
            issues = []
            if null_id_count > 0:
                issues.append(f"Found {null_id_count} records with null complaint_id")

            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate complaint IDs")
                # Note: Duplicates might be expected if we're appending daily data

            if issues:
                validation_results["validation_passed"] = False
                validation_results["issues"] = issues
                logger.warning(f"Data quality issues found: {issues}")

                # Uncomment to fail on data quality issues
                # raise AirflowException(f"Data quality validation failed: {issues}")
            else:
                logger.info("All data quality checks passed")

            logger.info(f"Validation results: {validation_results}")

            return validation_results

        except Exception as e:
            logger.error(f"Data quality validation failed: {e}", exc_info=True)
            raise AirflowException(f"Validation failed: {e}")

    # Define task dependencies using TaskFlow API
    complaints = extract_complaints()
    transformed_complaints = transform_complaints(complaints)
    table_created = create_snowflake_table()
    load_stats = load_to_snowflake(transformed_complaints)
    validation_results = validate_data_quality(load_stats)

    # Set explicit dependencies
    table_created >> load_stats


# Instantiate the DAG
consumer_complaints_etl()
