"""
S3 to Snowflake Loader Module

Handles creating Snowflake external stages and loading data from S3 to Snowflake.
"""

import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)


class S3ToSnowflakeLoader:
    """Manages S3 external stage creation and data loading to Snowflake."""

    def __init__(
        self,
        snowflake_hook: SnowflakeHook,
        s3_hook: S3Hook,
        database: str,
        schema: str,
        warehouse: str,
    ):
        """
        Initialize S3ToSnowflakeLoader.

        Args:
            snowflake_hook: Airflow SnowflakeHook
            s3_hook: Airflow S3Hook for AWS credentials
            database: Snowflake database name
            schema: Snowflake schema name
            warehouse: Snowflake warehouse name
        """
        self.snowflake_hook = snowflake_hook
        self.s3_hook = s3_hook
        self.database = database
        self.schema = schema
        self.warehouse = warehouse

    def create_table_and_stage(
        self,
        table_name: str,
        stage_name: str,
        bucket_name: str,
        s3_prefix: str,
        create_table_sql: str,
    ):
        """
        Create Snowflake table and external stage if they don't exist.

        Args:
            table_name: Name of the Snowflake table
            stage_name: Name of the Snowflake stage
            bucket_name: S3 bucket name
            s3_prefix: S3 key prefix
            create_table_sql: SQL statement to create the table
        """
        conn = self.snowflake_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Use the correct warehouse, database, and schema
            cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")

            # Create table
            logger.info(f"Creating table if not exists: {self.database}.{self.schema}.{table_name}")
            cursor.execute(create_table_sql)
            logger.info(f"✓ Table {self.database}.{self.schema}.{table_name} is ready")

            # Get AWS credentials
            credentials = self.s3_hook.get_credentials()

            # Drop and recreate stage (to ensure it points to correct location)
            logger.info(f"Creating external stage: {stage_name}")

            drop_stage_sql = f"DROP STAGE IF EXISTS {stage_name}"
            cursor.execute(drop_stage_sql)

            create_stage_sql = f"""
            CREATE STAGE {stage_name}
                URL = 's3://{bucket_name}/{s3_prefix}/'
                CREDENTIALS = (
                    AWS_KEY_ID = '{credentials.access_key}'
                    AWS_SECRET_KEY = '{credentials.secret_key}'
                )
                FILE_FORMAT = (
                    TYPE = CSV
                    FIELD_DELIMITER = ','
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    ESCAPE_UNENCLOSED_FIELD = NONE
                    NULL_IF = ('NULL', 'null', '')
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                )
                COMMENT = 'External stage for S3 bucket containing consumer complaints data'
            """

            cursor.execute(create_stage_sql)
            logger.info(f"✓ Stage '{stage_name}' created successfully")

        finally:
            cursor.close()
            conn.close()

    def get_latest_files_by_company(
        self, stage_name: str, prefix: str = "consumer_complaints"
    ) -> List[str]:
        """
        List files in stage and return only the most recent file for each company.

        Args:
            stage_name: Name of the Snowflake stage
            prefix: S3 prefix pattern to match

        Returns:
            List of filenames (most recent for each company)
        """
        conn = self.snowflake_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(f"LIST @{stage_name}")
            stage_files = cursor.fetchall()

            if not stage_files:
                logger.warning("No files found in stage")
                return []

            # Parse and group files by company
            # Pattern: consumer_complaints/YYYYMMDD_HHMMSS_company_complaints.csv
            pattern = rf"{prefix}/(\d{{8}}_\d{{6}})_(.+?)_complaints\.csv"
            files_by_company = defaultdict(list)

            for file_info in stage_files:
                filename = file_info[0]
                match = re.search(pattern, filename)

                if match:
                    timestamp = match.group(1)
                    company = match.group(2)
                    files_by_company[company].append(
                        {
                            "filename": filename,
                            "timestamp": timestamp,
                            "size": file_info[1],
                        }
                    )

            # Get most recent file for each company
            latest_files = []
            logger.info("\nIdentifying most recent files by company:")
            logger.info("-" * 80)

            for company, file_list in sorted(files_by_company.items()):
                # Sort by timestamp descending to get the latest
                file_list.sort(key=lambda x: x["timestamp"], reverse=True)
                latest = file_list[0]
                latest_files.append(latest["filename"])

                size_mb = latest["size"] / (1024 * 1024)
                logger.info(f"  {company}: {latest['filename'].split('/')[-1]} ({size_mb:.2f} MB)")

            logger.info("-" * 80)
            logger.info(f"Total: {len(latest_files)} file(s) to copy")

            return latest_files

        finally:
            cursor.close()
            conn.close()

    def copy_from_s3_to_snowflake(
        self,
        table_name: str,
        stage_name: str,
        file_list: List[str],
    ) -> Tuple[int, int, int]:
        """
        Copy data from S3 (via stage) to Snowflake table using COPY INTO.

        Args:
            table_name: Target Snowflake table name
            stage_name: Snowflake stage name
            file_list: List of specific files to copy

        Returns:
            Tuple of (total_files, total_rows, total_errors)
        """
        conn = self.snowflake_hook.get_conn()
        cursor = conn.cursor()

        total_files = 0
        total_rows = 0
        total_errors = 0

        try:
            logger.info(f"\nCopying data from S3 to {self.database}.{self.schema}.{table_name}...")
            logger.info("=" * 80)

            for file_path in file_list:
                filename = file_path.split("/")[-1]

                # COPY INTO with column mapping based on CSV structure
                copy_sql = f"""
                COPY INTO {self.database}.{self.schema}.{table_name} (
                    product,
                    complaint_what_happened,
                    date_sent_to_company,
                    issue,
                    sub_product,
                    zip_code,
                    tags,
                    complaint_id,
                    timely_response,
                    consumer_consent_provided,
                    company_response_to_consumer,
                    submitted_via,
                    company,
                    date_received,
                    state,
                    consumer_disputed,
                    company_public_response,
                    sub_issue
                )
                FROM @{stage_name}/{filename}
                ON_ERROR = CONTINUE
                FORCE = TRUE
                """

                try:
                    cursor.execute(copy_sql)
                    result = cursor.fetchall()

                    for row in result:
                        file_loaded = row[0]
                        status = row[1]
                        rows_loaded = row[2]
                        rows_parsed = row[3]
                        errors = row[5]

                        total_files += 1
                        total_rows += rows_loaded
                        total_errors += errors

                        logger.info(f"  File: {file_loaded}")
                        logger.info(f"    Status: {status}")
                        logger.info(f"    Rows loaded: {rows_loaded:,}")
                        logger.info(f"    Rows parsed: {rows_parsed:,}")
                        if errors > 0:
                            logger.warning(f"    Errors: {errors}")
                        logger.info("-" * 80)

                except Exception as e:
                    logger.error(f"Failed to copy file {filename}: {e}")
                    continue

            logger.info("\n" + "=" * 80)
            logger.info("✓ SUCCESS! Data copied from S3 to Snowflake")
            logger.info(f"  Files processed: {total_files}")
            logger.info(f"  Rows loaded: {total_rows:,}")
            if total_errors > 0:
                logger.warning(f"  Errors encountered: {total_errors}")
            logger.info("=" * 80)

            return total_files, total_rows, total_errors

        finally:
            cursor.close()
            conn.close()

    def validate_data_quality(self, table_name: str, rows_loaded_this_run: int) -> Dict[str, Any]:
        """
        Perform data quality validation on the Snowflake table.

        Args:
            table_name: Snowflake table name
            rows_loaded_this_run: Number of rows loaded in current run

        Returns:
            Dictionary with validation results
        """
        conn = self.snowflake_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Get total row count
            cursor.execute(f"SELECT COUNT(*) FROM {self.database}.{self.schema}.{table_name}")
            total_rows = cursor.fetchone()[0]

            # Check for duplicates
            duplicate_query = f"""
            SELECT COUNT(*) - COUNT(DISTINCT complaint_id) as duplicate_count
            FROM {self.database}.{self.schema}.{table_name}
            """
            cursor.execute(duplicate_query)
            duplicate_count = cursor.fetchone()[0]

            # Check for null complaint_ids
            null_id_query = f"""
            SELECT COUNT(*)
            FROM {self.database}.{self.schema}.{table_name}
            WHERE complaint_id IS NULL
            """
            cursor.execute(null_id_query)
            null_id_count = cursor.fetchone()[0]

            # Get date range
            date_range_query = f"""
            SELECT 
                MIN(date_received)::DATE as earliest_date,
                MAX(date_received)::DATE as latest_date
            FROM {self.database}.{self.schema}.{table_name}
            WHERE date_received IS NOT NULL
            """
            cursor.execute(date_range_query)
            date_range = cursor.fetchone()

            # Get top companies
            top_companies_query = f"""
            SELECT company, COUNT(*) as count
            FROM {self.database}.{self.schema}.{table_name}
            GROUP BY company
            ORDER BY count DESC
            LIMIT 5
            """
            cursor.execute(top_companies_query)
            top_companies = cursor.fetchall()

            # Compile validation results
            validation_results = {
                "total_rows_in_table": total_rows,
                "rows_loaded_this_run": rows_loaded_this_run,
                "duplicate_complaint_ids": duplicate_count,
                "null_complaint_ids": null_id_count,
                "earliest_date": str(date_range[0]) if date_range and date_range[0] else None,
                "latest_date": str(date_range[1]) if date_range and date_range[1] else None,
                "top_companies": [
                    {"company": company, "count": count} for company, count in top_companies
                ],
                "validation_passed": True,
            }

            # Check for critical issues
            issues = []
            if null_id_count > 0:
                issues.append(f"Found {null_id_count} records with null complaint_id")

            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate complaint IDs")

            if issues:
                validation_results["validation_passed"] = False
                validation_results["issues"] = issues
                logger.warning(f"Data quality issues found: {issues}")
            else:
                logger.info("✓ All data quality checks passed")

            # Log statistics
            self._log_statistics(validation_results)

            return validation_results

        finally:
            cursor.close()
            conn.close()

    def _log_statistics(self, validation_results: Dict[str, Any]):
        """Log table statistics in a formatted manner."""
        logger.info("\n" + "=" * 80)
        logger.info("Table Statistics:")
        logger.info("-" * 80)
        logger.info(f"Total rows in table: {validation_results['total_rows_in_table']:,}")
        logger.info(f"Rows loaded this run: {validation_results['rows_loaded_this_run']:,}")

        if validation_results.get("earliest_date") and validation_results.get("latest_date"):
            logger.info(
                f"Date range: {validation_results['earliest_date']} to {validation_results['latest_date']}"
            )

        logger.info("\nTop 5 companies by complaint count:")
        for item in validation_results.get("top_companies", []):
            logger.info(f"  {item['company']}: {item['count']:,}")

        logger.info("=" * 80)
