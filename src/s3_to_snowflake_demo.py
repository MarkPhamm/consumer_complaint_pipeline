"""
S3 to Snowflake Copy Demo

Creates an external stage in Snowflake pointing to S3 and uses COPY INTO
to load CSV data from S3 into Snowflake tables.

This is a common ELT pattern:
1. Extract and Load raw data to S3
2. Use Snowflake's COPY command to ingest from S3 (fast and efficient)
3. Transform data in Snowflake using SQL/dbt
"""

import logging
import os
from pathlib import Path

import snowflake.connector
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
        logger.info("Please create a .env file with the following credentials:")
        logger.info("\n# Snowflake Credentials")
        logger.info("  SNOWFLAKE_ACCOUNT=your_account.region")
        logger.info("  SNOWFLAKE_USER=your_username")
        logger.info("  SNOWFLAKE_PASSWORD=your_password")
        logger.info("  SNOWFLAKE_WAREHOUSE=COMPUTE_WH")
        logger.info("  SNOWFLAKE_DATABASE=CONSUMER_DATA")
        logger.info("  SNOWFLAKE_SCHEMA=PUBLIC")
        logger.info("  SNOWFLAKE_ROLE=ACCOUNTADMIN")
        logger.info("\n# AWS Credentials (for S3 access)")
        logger.info("  AWS_ACCESS_KEY_ID=your_access_key")
        logger.info("  AWS_SECRET_ACCESS_KEY=your_secret_key")
        logger.info("  AWS_S3_BUCKET=your-bucket-name")
        logger.info("  AWS_REGION=us-east-1")
        raise FileNotFoundError(f".env file not found at {env_path}")
    
    load_dotenv(env_path)
    logger.info("✓ Environment variables loaded from .env")


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    database = os.getenv('SNOWFLAKE_DATABASE', 'CONSUMER_DATA')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    
    if not all([account, user, password]):
        raise ValueError("Missing required Snowflake credentials in .env file")
    
    logger.info(f"Connecting to Snowflake account: {account}")
    
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role
    )
    
    # Explicitly use warehouse, database, and schema
    cursor = conn.cursor()
    try:
        # Try to use warehouse
        try:
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            logger.info(f"✓ Using warehouse: {warehouse}")
        except Exception as e:
            logger.error(f"Failed to use warehouse '{warehouse}': {e}")
            logger.info("Fetching available warehouses...")
            cursor.execute("SHOW WAREHOUSES")
            warehouses = cursor.fetchall()
            if warehouses:
                logger.info("Available warehouses:")
                for wh in warehouses:
                    wh_name = wh[0]
                    wh_state = wh[1]
                    logger.info(f"  - {wh_name} (state: {wh_state})")
                logger.info(f"\nPlease update SNOWFLAKE_WAREHOUSE in your .env file to one of the above.")
            raise ValueError(f"Warehouse '{warehouse}' does not exist or you don't have access to it")
        
        # Use database and schema
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        
    finally:
        cursor.close()
    
    logger.info(f"✓ Connected to Snowflake ({database}.{schema})")
    return conn


def drop_table_if_exists(conn, database, schema, table_name):
    """Drop the table if it exists (useful for schema changes)."""
    cursor = conn.cursor()
    try:
        drop_sql = f"DROP TABLE IF EXISTS {database}.{schema}.{table_name}"
        cursor.execute(drop_sql)
        logger.info(f"✓ Dropped existing table {database}.{schema}.{table_name}")
    except Exception as e:
        logger.warning(f"Could not drop table: {e}")
    finally:
        cursor.close()


def create_table_if_not_exists(conn, database, schema):
    """Create the CONSUMER_COMPLAINTS table if it doesn't exist."""
    cursor = conn.cursor()
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.CONSUMER_COMPLAINTS (
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
    
    try:
        logger.info("Creating table if not exists...")
        cursor.execute(create_table_sql)
        logger.info(f"✓ Table {database}.{schema}.CONSUMER_COMPLAINTS is ready")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise
    finally:
        cursor.close()


def create_s3_stage(conn, stage_name, bucket_name, aws_key_id, aws_secret_key, region='us-east-1'):
    """
    Create an external stage in Snowflake that points to S3.
    
    Args:
        conn: Snowflake connection
        stage_name: Name of the stage to create
        bucket_name: S3 bucket name
        aws_key_id: AWS access key ID
        aws_secret_key: AWS secret access key
        region: AWS region
    """
    cursor = conn.cursor()
    
    # Drop existing stage if it exists (for demo purposes)
    drop_stage_sql = f"DROP STAGE IF EXISTS {stage_name}"
    
    # Create stage with AWS credentials
    create_stage_sql = f"""
    CREATE STAGE {stage_name}
        URL = 's3://{bucket_name}/consumer_complaints/'
        CREDENTIALS = (
            AWS_KEY_ID = '{aws_key_id}'
            AWS_SECRET_KEY = '{aws_secret_key}'
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
    
    try:
        logger.info(f"Creating external stage: {stage_name}")
        cursor.execute(drop_stage_sql)
        cursor.execute(create_stage_sql)
        logger.info(f"✓ Stage '{stage_name}' created successfully")
        
        # List files in stage to verify
        logger.info("\nListing files in stage...")
        cursor.execute(f"LIST @{stage_name}")
        files = cursor.fetchall()
        
        if files:
            logger.info(f"Found {len(files)} file(s) in stage:")
            for file_info in files:
                filename = file_info[0]
                size_bytes = file_info[1]
                size_mb = size_bytes / (1024 * 1024)
                logger.info(f"  - {filename} ({size_mb:.2f} MB)")
        else:
            logger.warning("No files found in stage. Make sure you've uploaded CSV files to S3 first.")
            
    except Exception as e:
        logger.error(f"Failed to create stage: {e}")
        raise
    finally:
        cursor.close()


def copy_from_stage_to_table(conn, stage_name, database, schema, table_name):
    """
    Copy data from S3 stage into Snowflake table using COPY INTO command.
    
    Args:
        conn: Snowflake connection
        stage_name: Name of the stage
        database: Database name
        schema: Schema name
        table_name: Target table name
    """
    cursor = conn.cursor()
    
    # COPY INTO command with column mapping
    # CSV column order: product,complaint_what_happened,date_sent_to_company,issue,sub_product,
    #                   zip_code,tags,complaint_id,timely,consumer_consent_provided,company_response,
    #                   submitted_via,company,date_received,state,consumer_disputed,company_public_response,sub_issue
    copy_sql = f"""
    COPY INTO {database}.{schema}.{table_name} (
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
    FROM @{stage_name}
    PATTERN = '.*\\.csv'
    ON_ERROR = CONTINUE
    FORCE = TRUE
    """
    
    try:
        logger.info(f"Copying data from stage to {database}.{schema}.{table_name}...")
        logger.info("This may take a moment for large files...")
        
        cursor.execute(copy_sql)
        result = cursor.fetchall()
        
        # Parse results
        total_files = 0
        total_rows = 0
        total_errors = 0
        
        logger.info("\nCopy Results:")
        logger.info("-" * 80)
        
        for row in result:
            filename = row[0]
            status = row[1]
            rows_loaded = row[2]
            rows_parsed = row[3]
            errors = row[5]
            
            total_files += 1
            total_rows += rows_loaded
            total_errors += errors
            
            logger.info(f"  File: {filename}")
            logger.info(f"    Status: {status}")
            logger.info(f"    Rows loaded: {rows_loaded:,}")
            logger.info(f"    Rows parsed: {rows_parsed:,}")
            if errors > 0:
                logger.warning(f"    Errors: {errors}")
            logger.info("-" * 80)
        
        return total_files, total_rows, total_errors
        
    except Exception as e:
        logger.error(f"Failed to copy data: {e}")
        raise
    finally:
        cursor.close()


def get_table_stats(conn, database, schema, table_name):
    """Get statistics about the table."""
    cursor = conn.cursor()
    
    try:
        # Total count
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name}")
        total_count = cursor.fetchone()[0]
        
        # Count by product
        cursor.execute(f"""
            SELECT product, COUNT(*) as count
            FROM {database}.{schema}.{table_name}
            GROUP BY product
            ORDER BY count DESC
            LIMIT 5
        """)
        product_counts = cursor.fetchall()
        
        # Date range
        cursor.execute(f"""
            SELECT 
                MIN(date_received)::DATE as earliest_date,
                MAX(date_received)::DATE as latest_date
            FROM {database}.{schema}.{table_name}
            WHERE date_received IS NOT NULL
        """)
        date_range = cursor.fetchone()
        
        return {
            'total_count': total_count,
            'product_counts': product_counts,
            'date_range': date_range
        }
        
    except Exception as e:
        logger.error(f"Failed to get table stats: {e}")
        raise
    finally:
        cursor.close()


def copy_s3_to_snowflake():
    """Main function to copy data from S3 to Snowflake."""
    try:
        logger.info("=" * 80)
        logger.info("S3 TO SNOWFLAKE COPY - Starting...")
        logger.info("=" * 80)
        
        # Load environment variables
        load_env()
        
        # Get configuration
        database = os.getenv('SNOWFLAKE_DATABASE', 'CONSUMER_DATA')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        table_name = 'CONSUMER_COMPLAINTS'
        stage_name = 'CONSUMER_COMPLAINTS_S3_STAGE'
        
        bucket_name = os.getenv('AWS_S3_BUCKET')
        aws_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region = os.getenv('AWS_REGION', 'us-east-1')
        
        if not all([bucket_name, aws_key_id, aws_secret_key]):
            raise ValueError("Missing required AWS credentials in .env file")
        
        # Connect to Snowflake
        conn = get_snowflake_connection()
        
        try:
            # Drop and recreate table (to ensure schema matches CSV)
            drop_table_if_exists(conn, database, schema, table_name)
            create_table_if_not_exists(conn, database, schema)
            
            # Create S3 stage
            create_s3_stage(
                conn=conn,
                stage_name=stage_name,
                bucket_name=bucket_name,
                aws_key_id=aws_key_id,
                aws_secret_key=aws_secret_key,
                region=region
            )
            
            # Copy data from stage to table
            total_files, total_rows, total_errors = copy_from_stage_to_table(
                conn=conn,
                stage_name=stage_name,
                database=database,
                schema=schema,
                table_name=table_name
            )
            
            logger.info("\n" + "=" * 80)
            logger.info("SUCCESS! Data copied from S3 to Snowflake")
            logger.info(f"  Files processed: {total_files}")
            logger.info(f"  Rows loaded: {total_rows:,}")
            if total_errors > 0:
                logger.warning(f"  Errors encountered: {total_errors}")
            logger.info("=" * 80)
            
            # Get table statistics
            logger.info("\nTable Statistics:")
            logger.info("-" * 80)
            stats = get_table_stats(conn, database, schema, table_name)
            
            logger.info(f"Total rows in table: {stats['total_count']:,}")
            
            if stats['date_range']:
                earliest, latest = stats['date_range']
                logger.info(f"Date range: {earliest} to {latest}")
            
            logger.info("\nTop 5 products by complaint count:")
            for product, count in stats['product_counts']:
                logger.info(f"  {product}: {count:,}")
            
            logger.info("-" * 80)
            
        finally:
            conn.close()
            logger.info("\n✓ Snowflake connection closed")
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    copy_s3_to_snowflake()

