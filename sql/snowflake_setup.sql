-- ============================================================================
-- Snowflake Setup Script for Consumer Complaint Pipeline
-- ============================================================================
-- This script sets up the necessary database, schema, tables, and permissions
-- for the Consumer Complaint ETL pipeline.
--
-- Run this script as a user with ACCOUNTADMIN or sufficient privileges.
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. CREATE DATABASE AND SCHEMA
-- ============================================================================

CREATE DATABASE IF NOT EXISTS CONSUMER_DATA
    COMMENT = 'Database for consumer complaint data from CFPB';

CREATE SCHEMA IF NOT EXISTS CONSUMER_DATA.PUBLIC
    COMMENT = 'Schema for consumer complaint tables';

-- ============================================================================
-- 2. CREATE OR VERIFY WAREHOUSE
-- ============================================================================

-- Option A: Create a new warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL operations';

-- Option B: Use an existing warehouse (uncomment and modify as needed)
-- SHOW WAREHOUSES;

-- ============================================================================
-- 3. CREATE ROLE AND GRANT PERMISSIONS
-- ============================================================================

-- Create ETL role
CREATE ROLE IF NOT EXISTS ETL_ROLE
    COMMENT = 'Role for ETL operations';

-- Grant database and schema usage
GRANT USAGE ON DATABASE CONSUMER_DATA TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;

-- Grant table permissions
GRANT CREATE TABLE ON SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CONSUMER_DATA.PUBLIC TO ROLE ETL_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ETL_ROLE;

-- ============================================================================
-- 4. CREATE USER (Optional - uncomment if creating a new user)
-- ============================================================================

-- CREATE USER IF NOT EXISTS airflow_user
--     PASSWORD = 'CHANGE_ME_SECURE_PASSWORD'
--     DEFAULT_ROLE = ETL_ROLE
--     DEFAULT_WAREHOUSE = COMPUTE_WH
--     COMMENT = 'User for Airflow ETL operations'
--     MUST_CHANGE_PASSWORD = TRUE;

-- GRANT ROLE ETL_ROLE TO USER airflow_user;

-- ============================================================================
-- 5. CREATE TABLE
-- ============================================================================

USE DATABASE CONSUMER_DATA;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

CREATE TABLE IF NOT EXISTS CONSUMER_COMPLAINTS (
    -- Primary key
    complaint_id VARCHAR(50) PRIMARY KEY,
    
    -- Date fields
    date_received DATE,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    
    -- Product information
    product VARCHAR(255),
    sub_product VARCHAR(255),
    
    -- Issue information
    issue VARCHAR(500),
    sub_issue VARCHAR(500),
    
    -- Company information
    company VARCHAR(500),
    
    -- Location information
    state VARCHAR(2),
    zip_code VARCHAR(10),
    
    -- Metadata
    tags VARCHAR(255),
    consumer_consent_provided VARCHAR(100),
    submitted_via VARCHAR(100),
    
    -- Response information
    company_response_to_consumer VARCHAR(255),
    timely_response VARCHAR(10),
    consumer_disputed VARCHAR(10),
    
    -- Text fields
    complaint_what_happened TEXT,
    company_public_response TEXT,
    
    -- ETL metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Consumer complaints data from CFPB API';

-- ============================================================================
-- 6. CREATE VIEWS FOR ANALYSIS (Optional)
-- ============================================================================

-- Summary view by product
CREATE OR REPLACE VIEW COMPLAINTS_BY_PRODUCT AS
SELECT 
    product,
    COUNT(*) as total_complaints,
    COUNT(DISTINCT company) as unique_companies,
    COUNT(CASE WHEN consumer_disputed = 'Yes' THEN 1 END) as disputed_count,
    COUNT(CASE WHEN timely_response = 'Yes' THEN 1 END) as timely_response_count,
    MIN(date_received) as earliest_complaint,
    MAX(date_received) as latest_complaint
FROM CONSUMER_COMPLAINTS
GROUP BY product
ORDER BY total_complaints DESC;

-- Summary view by date
CREATE OR REPLACE VIEW COMPLAINTS_BY_DATE AS
SELECT 
    DATE_TRUNC('day', date_received) as complaint_date,
    COUNT(*) as daily_complaints,
    COUNT(DISTINCT product) as unique_products,
    COUNT(DISTINCT company) as unique_companies
FROM CONSUMER_COMPLAINTS
GROUP BY complaint_date
ORDER BY complaint_date DESC;

-- Summary view by company
CREATE OR REPLACE VIEW COMPLAINTS_BY_COMPANY AS
SELECT 
    company,
    COUNT(*) as total_complaints,
    COUNT(DISTINCT product) as unique_products,
    COUNT(CASE WHEN consumer_disputed = 'Yes' THEN 1 END) as disputed_count,
    ROUND(COUNT(CASE WHEN timely_response = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) as timely_response_pct,
    MIN(date_received) as earliest_complaint,
    MAX(date_received) as latest_complaint
FROM CONSUMER_COMPLAINTS
GROUP BY company
ORDER BY total_complaints DESC;

-- Summary view by state
CREATE OR REPLACE VIEW COMPLAINTS_BY_STATE AS
SELECT 
    state,
    COUNT(*) as total_complaints,
    COUNT(DISTINCT company) as unique_companies,
    COUNT(DISTINCT product) as unique_products
FROM CONSUMER_COMPLAINTS
WHERE state IS NOT NULL
GROUP BY state
ORDER BY total_complaints DESC;

-- Data quality view
CREATE OR REPLACE VIEW DATA_QUALITY_METRICS AS
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT complaint_id) as unique_complaints,
    COUNT(*) - COUNT(DISTINCT complaint_id) as duplicate_complaints,
    COUNT(CASE WHEN complaint_id IS NULL THEN 1 END) as null_complaint_ids,
    COUNT(CASE WHEN date_received IS NULL THEN 1 END) as null_dates,
    COUNT(CASE WHEN product IS NULL THEN 1 END) as null_products,
    COUNT(CASE WHEN company IS NULL THEN 1 END) as null_companies,
    MIN(date_received) as earliest_date,
    MAX(date_received) as latest_date,
    MIN(load_timestamp) as first_load,
    MAX(load_timestamp) as last_load,
    DATEDIFF('day', MIN(date_received), MAX(date_received)) as date_range_days
FROM CONSUMER_COMPLAINTS;

-- ============================================================================
-- 7. VERIFY SETUP
-- ============================================================================

-- Show created objects
SHOW DATABASES LIKE 'CONSUMER_DATA';
SHOW SCHEMAS IN DATABASE CONSUMER_DATA;
SHOW TABLES IN SCHEMA CONSUMER_DATA.PUBLIC;
SHOW VIEWS IN SCHEMA CONSUMER_DATA.PUBLIC;

-- Test permissions (will fail if permissions not set correctly)
SELECT 'Setup verification: All permissions granted successfully!' as status;

-- Show grants
SHOW GRANTS TO ROLE ETL_ROLE;

-- ============================================================================
-- 8. SAMPLE QUERIES (for testing after data load)
-- ============================================================================

-- Count total records
-- SELECT COUNT(*) FROM CONSUMER_COMPLAINTS;

-- View sample data
-- SELECT * FROM CONSUMER_COMPLAINTS LIMIT 10;

-- View summaries
-- SELECT * FROM COMPLAINTS_BY_PRODUCT;
-- SELECT * FROM COMPLAINTS_BY_DATE LIMIT 30;
-- SELECT * FROM COMPLAINTS_BY_COMPANY LIMIT 20;
-- SELECT * FROM DATA_QUALITY_METRICS;

-- ============================================================================
-- 9. OPTIONAL OPTIMIZATIONS
-- ============================================================================

-- Add clustering key for better query performance (run after data is loaded)
-- ALTER TABLE CONSUMER_COMPLAINTS CLUSTER BY (date_received);

-- Set data retention (7 years for compliance)
-- ALTER TABLE CONSUMER_COMPLAINTS SET DATA_RETENTION_TIME_IN_DAYS = 2555;

-- ============================================================================
-- SETUP COMPLETE
-- ============================================================================

SELECT '✅ Snowflake setup complete! You can now configure Airflow connection.' as message;

