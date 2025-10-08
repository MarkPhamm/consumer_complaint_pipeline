# Setup Guide: Consumer Complaint Pipeline

This guide walks you through setting up the Consumer Complaint ETL pipeline from scratch.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Snowflake Setup](#snowflake-setup)
3. [Airflow Configuration](#airflow-configuration)
4. [Testing the Pipeline](#testing-the-pipeline)
5. [Troubleshooting](#troubleshooting)

## Prerequisites

Before starting, ensure you have:

- [ ] Snowflake account with appropriate permissions
- [ ] Airflow environment (Astronomer or standalone)
- [ ] Python 3.8 or higher
- [ ] Git installed

## Snowflake Setup

### Step 1: Create Database and Schema

Log into your Snowflake account and run the following SQL commands:

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS CONSUMER_DATA
    COMMENT = 'Database for consumer complaint data from CFPB';

-- Create schema
CREATE SCHEMA IF NOT EXISTS CONSUMER_DATA.PUBLIC
    COMMENT = 'Schema for consumer complaint tables';

-- Switch to the new database and schema
USE DATABASE CONSUMER_DATA;
USE SCHEMA PUBLIC;
```

### Step 2: Create or Verify Warehouse

```sql
-- Create a warehouse if you don't have one
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL operations';

-- Or use an existing warehouse
SHOW WAREHOUSES;
```

### Step 3: Create Role and User (if needed)

```sql
-- Create a role for the ETL process
CREATE ROLE IF NOT EXISTS ETL_ROLE
    COMMENT = 'Role for ETL operations';

-- Grant permissions to the role
GRANT USAGE ON DATABASE CONSUMER_DATA TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;
GRANT INSERT, SELECT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ETL_ROLE;

-- Create a user for Airflow (or use existing user)
-- CREATE USER IF NOT EXISTS airflow_user
--     PASSWORD = 'your_secure_password'
--     DEFAULT_ROLE = ETL_ROLE
--     DEFAULT_WAREHOUSE = COMPUTE_WH
--     COMMENT = 'User for Airflow ETL operations';

-- Grant role to user
-- GRANT ROLE ETL_ROLE TO USER airflow_user;
```

### Step 4: Verify Permissions

```sql
-- Verify database access
USE DATABASE CONSUMER_DATA;
USE SCHEMA PUBLIC;

-- Test table creation
CREATE OR REPLACE TABLE test_table (id INT);
SELECT * FROM test_table;
DROP TABLE test_table;
```

## Airflow Configuration

### Step 1: Install Dependencies

If using Astronomer:

```bash
cd consumer_complaint_pipeline
astro dev start
```

The dependencies in `requirements.txt` will be automatically installed.

### Step 2: Configure Snowflake Connection

#### Option A: Using Airflow UI

1. Access Airflow UI (usually <http://localhost:8080>)
2. Navigate to **Admin → Connections**
3. Click the **+** button
4. Fill in the connection details:

```
Connection Id: snowflake_default
Connection Type: Snowflake
Host: your_account.snowflakecomputing.com
Schema: PUBLIC
Login: your_username
Password: your_password
Extra: {
    "account": "your_account",
    "warehouse": "COMPUTE_WH",
    "database": "CONSUMER_DATA",
    "region": "us-east-1",
    "role": "ETL_ROLE"
}
```

5. Click **Test** to verify the connection
6. Click **Save**

#### Option B: Using Airflow CLI

```bash
airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-host 'your_account.snowflakecomputing.com' \
    --conn-schema 'PUBLIC' \
    --conn-extra '{
        "account": "your_account",
        "warehouse": "COMPUTE_WH",
        "database": "CONSUMER_DATA",
        "region": "us-east-1",
        "role": "ETL_ROLE"
    }'
```

#### Option C: Using Environment Variables (Astronomer)

Create a `.env` file (not tracked in git):

```bash
AIRFLOW_CONN_SNOWFLAKE_DEFAULT='snowflake://your_username:your_password@your_account/CONSUMER_DATA/PUBLIC?warehouse=COMPUTE_WH&role=ETL_ROLE'
```

### Step 3: Configure Airflow Variables (Optional)

Set these variables to customize pipeline behavior:

#### Using Airflow UI

1. Navigate to **Admin → Variables**
2. Add the following variables:

| Key | Value | Description |
|-----|-------|-------------|
| `cfpb_lookback_days` | `1` | Days to look back for complaints |
| `cfpb_max_records` | `` | Max records per run (blank = unlimited) |
| `snowflake_database` | `CONSUMER_DATA` | Target database |
| `snowflake_schema` | `PUBLIC` | Target schema |
| `snowflake_warehouse` | `COMPUTE_WH` | Compute warehouse |

#### Using Airflow CLI

```bash
airflow variables set cfpb_lookback_days 1
airflow variables set snowflake_database CONSUMER_DATA
airflow variables set snowflake_schema PUBLIC
airflow variables set snowflake_warehouse COMPUTE_WH
```

### Step 4: Verify DAG is Loaded

1. In Airflow UI, go to the **DAGs** page
2. Look for `consumer_complaints_etl`
3. Verify there are no import errors
4. Check the DAG documentation by clicking on the DAG name

## Testing the Pipeline

### Step 1: Manual Test Run

1. In Airflow UI, find `consumer_complaints_etl` DAG
2. Toggle the DAG **ON** (if it's off)
3. Click the **Play** button → **Trigger DAG**
4. Monitor the task execution in the Graph or Grid view

### Step 2: Verify Data in Snowflake

After a successful run, verify the data in Snowflake:

```sql
USE DATABASE CONSUMER_DATA;
USE SCHEMA PUBLIC;

-- Check if table exists
SHOW TABLES LIKE 'CONSUMER_COMPLAINTS';

-- Check row count
SELECT COUNT(*) FROM CONSUMER_COMPLAINTS;

-- View sample data
SELECT * FROM CONSUMER_COMPLAINTS LIMIT 10;

-- Check data quality
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT complaint_id) as unique_complaints,
    COUNT(*) - COUNT(DISTINCT complaint_id) as duplicates,
    COUNT(CASE WHEN complaint_id IS NULL THEN 1 END) as null_ids,
    MIN(date_received) as earliest_date,
    MAX(date_received) as latest_date,
    MAX(load_timestamp) as last_load
FROM CONSUMER_COMPLAINTS;

-- Check by product
SELECT 
    product,
    COUNT(*) as complaint_count
FROM CONSUMER_COMPLAINTS
GROUP BY product
ORDER BY complaint_count DESC
LIMIT 10;
```

### Step 3: Monitor Logs

1. Click on a task in the DAG view
2. Click **Log** to see detailed execution logs
3. Look for success messages:
   - "Successfully extracted X complaints"
   - "Transformation complete: X successful"
   - "Successfully loaded X rows to Snowflake"
   - "All data quality checks passed"

### Step 4: Run Automated Tests

```bash
# If using Astronomer
astro dev pytest tests/dags/test_consumer_complaints_etl.py -v

# Or using pytest directly
pytest tests/dags/test_consumer_complaints_etl.py -v
```

## Troubleshooting

### Issue 1: DAG Import Errors

**Symptom**: DAG doesn't appear in Airflow UI or shows import errors

**Solutions**:

```bash
# Check DAG for syntax errors
python dags/consumer_complaints_etl.py

# Check include modules
python include/cfpb_api_client.py
python include/snowflake_loader.py

# Restart Airflow
astro dev restart
```

### Issue 2: Snowflake Connection Failed

**Symptom**: Tasks fail with connection errors

**Solutions**:

1. Verify connection details in Airflow UI
2. Test connection manually:

   ```python
   from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
   hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
   conn = hook.get_conn()
   print("Connection successful!")
   ```

3. Check Snowflake user permissions
4. Verify network connectivity to Snowflake

### Issue 3: No Data Extracted

**Symptom**: Pipeline runs successfully but no data is loaded

**Possible Causes**:

1. No complaints in the specified date range
2. API rate limiting
3. Incorrect date configuration

**Solutions**:

```bash
# Increase lookback days
airflow variables set cfpb_lookback_days 7

# Check API directly
curl "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?size=10&format=json"
```

### Issue 4: Table Creation Failed

**Symptom**: Task `create_snowflake_table` fails

**Solutions**:

1. Verify CREATE TABLE permissions in Snowflake
2. Check if table already exists with different schema
3. Try creating table manually:

   ```sql
   USE DATABASE CONSUMER_DATA;
   USE SCHEMA PUBLIC;
   
   -- Check existing tables
   SHOW TABLES;
   
   -- Drop if exists (careful!)
   -- DROP TABLE IF EXISTS CONSUMER_COMPLAINTS;
   ```

### Issue 5: Data Quality Validation Warnings

**Symptom**: Warnings about duplicates or null values

**Solutions**:

1. Check if duplicates are expected (incremental loads)
2. Modify the DAG to handle duplicates:
   - Use MERGE instead of INSERT
   - Add deduplication logic
3. Review data quality rules in `validate_data_quality` task

## Next Steps

After successful setup:

1. **Schedule Configuration**: Adjust the DAG schedule if needed

   ```python
   # In consumer_complaints_etl.py
   schedule="@daily"  # Change to @hourly, @weekly, etc.
   ```

2. **Email Alerts**: Configure email notifications

   ```python
   default_args={
       "email": ["your-email@example.com"],
       "email_on_failure": True,
       "email_on_retry": True,
   }
   ```

3. **Monitoring Dashboard**: Create Snowflake views for monitoring

   ```sql
   CREATE VIEW complaint_summary AS
   SELECT 
       DATE_TRUNC('day', date_received) as date,
       product,
       COUNT(*) as complaint_count
   FROM CONSUMER_COMPLAINTS
   GROUP BY 1, 2
   ORDER BY 1 DESC, 3 DESC;
   ```

4. **Data Retention**: Set up data retention policies

   ```sql
   -- Example: Keep data for 7 years
   ALTER TABLE CONSUMER_COMPLAINTS 
   SET DATA_RETENTION_TIME_IN_DAYS = 2555;
   ```

5. **Performance Optimization**: For large datasets, consider:
   - Clustering keys on Snowflake tables
   - Partitioning strategies
   - Adjusting warehouse size
   - Implementing incremental loads with change data capture

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [CFPB API Documentation](https://cfpb.github.io/api/ccdb/api.html)
- [Astronomer Guides](https://www.astronomer.io/guides/)

## Support

If you encounter issues not covered in this guide:

1. Check the main [README.md](README.md) for additional information
2. Review Airflow task logs for detailed error messages
3. Test API and database connections independently
4. Verify all prerequisites are met

Happy ETL-ing! 🚀
