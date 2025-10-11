"""
S3 Data Loader Module

Handles uploading CSV files to S3 and managing file lifecycle.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


class S3Loader:
    """Manages S3 file uploads and cleanup operations."""

    def __init__(self, s3_hook: S3Hook, bucket_name: str, prefix: str = "consumer_complaints"):
        """
        Initialize S3Loader.

        Args:
            s3_hook: Airflow S3Hook for S3 operations
            bucket_name: S3 bucket name
            prefix: S3 key prefix for uploaded files
        """
        self.s3_hook = s3_hook
        self.bucket_name = bucket_name
        self.prefix = prefix

    def upload_files(self, company_files: Dict[str, str]) -> List[Dict[str, any]]:
        """
        Upload CSV files to S3 with timestamped names and cleanup old files.

        Args:
            company_files: Dictionary mapping company names to local file paths

        Returns:
            List of dictionaries with upload info (company, s3_key, size_mb)
        """
        if not company_files:
            logger.warning("No files to upload to S3")
            return []

        logger.info(f"Uploading {len(company_files)} files to S3 bucket: {self.bucket_name}")

        uploaded_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for company_name, file_path in company_files.items():
            try:
                # Sanitize company name for S3 key
                sanitized_name = company_name.replace(" ", "_").lower()

                # Generate S3 key with timestamp
                s3_key = f"{self.prefix}/{timestamp}_{sanitized_name}_complaints.csv"

                # Get file size
                file_size = Path(file_path).stat().st_size
                file_size_mb = file_size / (1024 * 1024)

                logger.info(f"📁 Uploading {sanitized_name}_complaints.csv ({file_size_mb:.2f} MB)")

                # Upload to S3
                self.s3_hook.load_file(
                    filename=file_path,
                    key=s3_key,
                    bucket_name=self.bucket_name,
                    replace=True,
                )

                logger.info(f"✓ Uploaded to s3://{self.bucket_name}/{s3_key}")

                uploaded_files.append(
                    {
                        "company": company_name,
                        "s3_key": s3_key,
                        "size_mb": file_size_mb,
                    }
                )

                # Clean up old files for this company
                self.cleanup_old_files(sanitized_name, s3_key)

            except Exception as e:
                logger.error(f"Failed to upload {company_name}: {e}")
                continue

        logger.info(f"✓ Uploaded {len(uploaded_files)} files to S3")
        return uploaded_files

    def cleanup_old_files(self, company_name: str, current_key: str):
        """
        Delete old S3 files for a specific company, keeping only the current one.

        Args:
            company_name: Sanitized company name
            current_key: The current S3 key to keep (don't delete this one)
        """
        try:
            # List all files in the prefix
            keys = self.s3_hook.list_keys(
                bucket_name=self.bucket_name,
                prefix=f"{self.prefix}/",
            )

            if not keys:
                return

            # Pattern to match company files: prefix/YYYYMMDD_HHMMSS_company_complaints.csv
            pattern = rf"{self.prefix}/\d{{8}}_\d{{6}}_{re.escape(company_name)}_complaints\.csv"

            files_to_delete = []
            for key in keys:
                if re.match(pattern, key) and key != current_key:
                    files_to_delete.append(key)

            if files_to_delete:
                logger.info(f"🗑️  Cleaning up {len(files_to_delete)} old file(s) for {company_name}")
                for key in files_to_delete:
                    self.s3_hook.delete_objects(bucket=self.bucket_name, keys=[key])
                    logger.info(f"   Deleted: {key}")

        except Exception as e:
            logger.warning(f"Failed to cleanup old files for {company_name}: {e}")
            # Don't fail on cleanup errors

    def list_files(self) -> List[str]:
        """
        List all files in the S3 prefix.

        Returns:
            List of S3 keys
        """
        try:
            keys = self.s3_hook.list_keys(
                bucket_name=self.bucket_name,
                prefix=f"{self.prefix}/",
            )
            return keys or []
        except Exception as e:
            logger.error(f"Failed to list S3 files: {e}")
            return []
