"""
S3 Data Loader Demo

Simple script to upload CSV data from the data directory to Amazon S3.
No Airflow dependencies - uses direct S3 connection via boto3.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get project root
PROJECT_ROOT = Path(__file__).parent.parent


def load_env():
    """Load environment variables from .env file."""
    env_path = PROJECT_ROOT / '.env'
    
    if not env_path.exists():
        logger.error(f".env file not found at {env_path}")
        logger.info("Please create a .env file with your AWS credentials:")
        logger.info("  AWS_ACCESS_KEY_ID=your_access_key")
        logger.info("  AWS_SECRET_ACCESS_KEY=your_secret_key")
        logger.info("  AWS_S3_BUCKET=your-bucket-name")
        logger.info("  AWS_REGION=us-east-1  # Optional, defaults to us-east-1")
        raise FileNotFoundError(f".env file not found at {env_path}")
    
    load_dotenv(env_path)
    logger.info("✓ Environment variables loaded from .env")


def get_s3_client():
    """Create and return an S3 client."""
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('AWS_S3_BUCKET')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    if not all([access_key, secret_key, bucket_name]):
        raise ValueError("Missing required AWS credentials in .env file")
    
    logger.info(f"Connecting to S3 bucket: {bucket_name}")
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        # Verify bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"✓ Connected to S3 bucket: {bucket_name} (region: {region})")
        
        return s3_client, bucket_name
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise ValueError(f"Bucket '{bucket_name}' does not exist")
        elif error_code == '403':
            raise ValueError(f"Access denied to bucket '{bucket_name}'")
        else:
            raise
    except NoCredentialsError:
        raise ValueError("Invalid AWS credentials")


def upload_file_to_s3(s3_client, file_path, bucket_name, s3_key=None):
    """
    Upload a file to S3 bucket.
    
    Args:
        s3_client: boto3 S3 client
        file_path: Path to the local file
        bucket_name: Name of the S3 bucket
        s3_key: S3 object key (path in bucket). If None, uses filename with timestamp.
    
    Returns:
        S3 URI of the uploaded file
    """
    if s3_key is None:
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = Path(file_path).name
        s3_key = f"consumer_complaints/{timestamp}_{filename}"
    
    try:
        logger.info(f"Uploading {Path(file_path).name} to s3://{bucket_name}/{s3_key}")
        
        # Get file size for progress
        file_size = Path(file_path).stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        logger.info(f"File size: {file_size_mb:.2f} MB")
        
        # Upload file
        s3_client.upload_file(
            str(file_path),
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        logger.info(f"✓ Successfully uploaded to {s3_uri}")
        
        return s3_uri
        
    except ClientError as e:
        logger.error(f"Failed to upload file: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise


def list_bucket_files(s3_client, bucket_name, prefix=''):
    """List files in S3 bucket with optional prefix filter."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            logger.info(f"No files found in s3://{bucket_name}/{prefix}")
            return []
        
        files = [obj['Key'] for obj in response['Contents']]
        logger.info(f"Found {len(files)} file(s) in s3://{bucket_name}/{prefix}")
        
        return files
        
    except ClientError as e:
        logger.error(f"Failed to list bucket files: {e}")
        raise


def load_csv_to_s3():
    """Main function to upload CSV data to S3."""
    try:
        logger.info("=" * 80)
        logger.info("S3 DATA LOADER - Starting...")
        logger.info("=" * 80)
        
        # Load environment variables
        load_env()
        
        # Find CSV file
        data_dir = PROJECT_ROOT / 'data'
        csv_files = list(data_dir.glob('*.csv'))
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {data_dir}")
        
        csv_path = csv_files[0]
        logger.info(f"Found CSV file: {csv_path.name}")
        
        # Connect to S3
        s3_client, bucket_name = get_s3_client()
        
        # Upload file
        s3_uri = upload_file_to_s3(
            s3_client=s3_client,
            file_path=csv_path,
            bucket_name=bucket_name
        )
        
        logger.info("=" * 80)
        logger.info(f"SUCCESS! File uploaded to S3")
        logger.info(f"   S3 URI: {s3_uri}")
        logger.info("=" * 80)
        
        # List files in the consumer_complaints prefix
        logger.info("\nFiles in consumer_complaints folder:")
        files = list_bucket_files(s3_client, bucket_name, prefix='consumer_complaints/')
        
        for i, file_key in enumerate(files, 1):
            # Get file metadata
            try:
                metadata = s3_client.head_object(Bucket=bucket_name, Key=file_key)
                size_mb = metadata['ContentLength'] / (1024 * 1024)
                last_modified = metadata['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"  {i}. {file_key} ({size_mb:.2f} MB, modified: {last_modified})")
            except ClientError:
                logger.info(f"  {i}. {file_key}")
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    load_csv_to_s3()
