# Consumer Complaint Pipeline

An Apache Airflow ETL pipeline that extracts consumer complaint data from the [CFPB (Consumer Financial Protection Bureau) API](https://cfpb.github.io/api/ccdb/api.html) and loads it into Snowflake for analysis.

## 🎯 Overview

This pipeline provides a production-ready, automated solution for:

- **Extracting** consumer complaint data from the CFPB public API
- **Transforming** and validating the data for quality assurance
- **Loading** the data into Snowflake data warehouse
- **Monitoring** data quality and pipeline health

## 🏗️ Architecture

```
CFPB API → Airflow DAG → Data Transformation → Snowflake Data Warehouse
```

### Components

- **DAG**: `consumer_complaints_etl` - Main orchestration pipeline
- **API Client**: `cfpb_api_client.py` - Handles CFPB API interactions
- **Data Loader**: `snowflake_loader.py` - Manages Snowflake operations
- **Tests**: Comprehensive test suite for DAG validation

## 📋 Prerequisites

- Apache Airflow (using Astronomer Runtime 3.1-1)
- Snowflake account with appropriate permissions
- Python 3.8+

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd consumer_complaint_pipeline
```

### 2. Install Dependencies

Dependencies will be automatically installed when building the Docker image. The required packages are defined in `requirements.txt`:

- `apache-airflow-providers-snowflake>=5.0.0`
- `pandas>=2.0.0`
- `requests>=2.31.0`
- `snowflake-connector-python>=3.0.0`
- `python-dotenv>=1.0.0`

### 3. Configure Snowflake Connection in Airflow

Navigate to Airflow UI and create a new connection:

**Connection Details:**

- **Connection ID**: `snowflake_default`
- **Connection Type**: `Snowflake`
- **Account**: Your Snowflake account identifier (e.g., `abc12345.us-east-1`)
- **User**: Your Snowflake username
- **Password**: Your Snowflake password
- **Role**: Your Snowflake role (e.g., `ACCOUNTADMIN`, `SYSADMIN`)
- **Warehouse**: Your compute warehouse (e.g., `COMPUTE_WH`)
- **Database**: Target database (e.g., `CONSUMER_DATA`)
- **Schema**: Target schema (e.g., `PUBLIC`)

#### Using Airflow UI

1. Go to **Admin → Connections**
2. Click the **+** button to add a new connection
3. Fill in the details above
4. Click **Save**

#### Using Airflow CLI

```bash
airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login 'YOUR_USERNAME' \
    --conn-password 'YOUR_PASSWORD' \
    --conn-host 'YOUR_ACCOUNT.snowflakecomputing.com' \
    --conn-extra '{"account": "YOUR_ACCOUNT", "warehouse": "COMPUTE_WH", "database": "CONSUMER_DATA", "region": "us-east-1", "role": "SYSADMIN"}'
```

### 4. Configure Airflow Variables (Optional)

You can customize the pipeline behavior using Airflow Variables:

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `cfpb_lookback_days` | Number of days to look back for complaints | `1` |
| `cfpb_max_records` | Maximum records to fetch per run | `None` (unlimited) |
| `snowflake_database` | Target Snowflake database | `CONSUMER_DATA` |
| `snowflake_schema` | Target Snowflake schema | `PUBLIC` |
| `snowflake_warehouse` | Snowflake compute warehouse | `COMPUTE_WH` |

#### Using Airflow UI

1. Go to **Admin → Variables**
2. Click the **+** button
3. Add variables as needed

#### Using Airflow CLI

```bash
airflow variables set cfpb_lookback_days 7
airflow variables set snowflake_database CONSUMER_DATA
airflow variables set snowflake_schema PUBLIC
```

### 5. Prepare Snowflake Database

Run these commands in your Snowflake worksheet to prepare the database:

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS CONSUMER_DATA;
CREATE SCHEMA IF NOT EXISTS CONSUMER_DATA.PUBLIC;

-- Use the database and schema
USE DATABASE CONSUMER_DATA;
USE SCHEMA PUBLIC;

-- Grant necessary permissions (adjust role as needed)
GRANT USAGE ON DATABASE CONSUMER_DATA TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA CONSUMER_DATA.PUBLIC TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA CONSUMER_DATA.PUBLIC TO ROLE SYSADMIN;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CONSUMER_DATA.PUBLIC TO ROLE SYSADMIN;

-- The table will be automatically created by the DAG
-- But you can create it manually if desired:
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
);
```

### 6. Deploy and Run

If using Astronomer:

```bash
astro dev start
```

Otherwise, ensure Airflow is running and:

1. Navigate to the Airflow UI (typically <http://localhost:8080>)
2. Find the `consumer_complaints_etl` DAG
3. Toggle it **ON**
4. Click **Trigger DAG** to run manually, or wait for the daily schedule

## 📊 Data Schema

### Snowflake Table: `CONSUMER_COMPLAINTS`

| Column | Type | Description |
|--------|------|-------------|
| `complaint_id` | VARCHAR(50) | Unique complaint identifier (Primary Key) |
| `date_received` | DATE | Date complaint was received |
| `product` | VARCHAR(255) | Financial product type |
| `sub_product` | VARCHAR(255) | Product subcategory |
| `issue` | VARCHAR(500) | Main issue category |
| `sub_issue` | VARCHAR(500) | Issue subcategory |
| `company` | VARCHAR(500) | Company name |
| `state` | VARCHAR(2) | US state code |
| `zip_code` | VARCHAR(10) | ZIP code |
| `tags` | VARCHAR(255) | Additional tags |
| `consumer_consent_provided` | VARCHAR(100) | Consent status |
| `submitted_via` | VARCHAR(100) | Submission channel |
| `company_response_to_consumer` | VARCHAR(255) | Response type |
| `timely_response` | VARCHAR(10) | Timeliness indicator |
| `consumer_disputed` | VARCHAR(10) | Dispute status |
| `complaint_what_happened` | TEXT | Complaint narrative |
| `company_public_response` | TEXT | Company's public response |
| `created_date` | TIMESTAMP | Date sent to company |
| `updated_date` | TIMESTAMP | Last update date |
| `load_timestamp` | TIMESTAMP | ETL load timestamp |

## 🔄 Pipeline Details

### DAG: `consumer_complaints_etl`

- **Schedule**: Daily (`@daily`)
- **Catchup**: Disabled
- **Max Active Runs**: 1
- **Retries**: 3 attempts
- **Retry Delay**: 5 minutes

### Pipeline Tasks

1. **extract_complaints**: Fetch data from CFPB API
2. **transform_complaints**: Validate and transform data
3. **create_snowflake_table**: Ensure table exists in Snowflake
4. **load_to_snowflake**: Load data into Snowflake
5. **validate_data_quality**: Run data quality checks

### Task Dependencies

```
extract_complaints → transform_complaints → load_to_snowflake → validate_data_quality
                                         ↗
              create_snowflake_table ────
```

## 🧪 Testing

Run the test suite:

```bash
pytest tests/dags/test_consumer_complaints_etl.py -v
```

Or if using Astronomer:

```bash
astro dev pytest tests/dags/test_consumer_complaints_etl.py
```

## 📈 Monitoring

### Data Quality Checks

The pipeline includes automated data quality validation:

- ✅ Row count verification
- ✅ Duplicate complaint ID detection
- ✅ Null value validation for critical fields
- ✅ Load statistics tracking

### Logging

All tasks include comprehensive logging:

- API request/response details
- Transformation statistics
- Load performance metrics
- Error details with full stack traces

### Airflow UI Monitoring

Monitor your pipeline in the Airflow UI:

- **DAG Graph View**: Visualize task dependencies
- **Task Logs**: View detailed execution logs
- **XCom**: Inspect data passed between tasks
- **Task Duration**: Track performance over time

## 🔧 Troubleshooting

### Common Issues

#### 1. Snowflake Connection Failed

```
Error: snowflake.connector.errors.DatabaseError
```

**Solution**: Verify your Snowflake connection credentials in Airflow connections.

#### 2. CFPB API Rate Limiting

```
Error: HTTP 429 Too Many Requests
```

**Solution**: The client includes automatic retry logic. If persistent, reduce `cfpb_max_records` or increase `cfpb_lookback_days`.

#### 3. Table Already Exists Error

```
Error: Table already exists
```

**Solution**: The pipeline handles this automatically. If you see this error, check table permissions.

#### 4. Import Errors

```
Error: ModuleNotFoundError: No module named 'cfpb_api_client'
```

**Solution**: Ensure the `include/` directory is properly configured in your Airflow environment.

## 🏗️ Project Structure

```
consumer_complaint_pipeline/
├── dags/
│   ├── exampledag.py                      # Example DAG
│   └── consumer_complaints_etl.py         # Main ETL pipeline
├── include/
│   ├── cfpb_api_client.py                 # CFPB API client utility
│   └── snowflake_loader.py                # Snowflake loader utility
├── tests/
│   └── dags/
│       ├── test_dag_example.py
│       └── test_consumer_complaints_etl.py # Pipeline tests
├── plugins/                                # Custom Airflow plugins
├── requirements.txt                        # Python dependencies
├── packages.txt                            # System packages
├── Dockerfile                              # Container definition
├── airflow_settings.yaml                   # Airflow configuration
└── README.md                               # This file
```

## 🤝 Best Practices Implemented

This pipeline follows industry best practices:

- ✅ **Modular Design**: Separated concerns (API, data loading, orchestration)
- ✅ **Error Handling**: Comprehensive try-catch blocks with proper logging
- ✅ **Retry Logic**: Automatic retries with exponential backoff
- ✅ **Data Validation**: Quality checks at multiple stages
- ✅ **Documentation**: Extensive docstrings and comments
- ✅ **Type Hints**: Full type annotations for better code clarity
- ✅ **Testing**: Comprehensive test suite
- ✅ **Configuration Management**: Externalized configuration via Airflow Variables
- ✅ **Idempotency**: Pipeline can be safely re-run
- ✅ **Logging**: Detailed logging at each step
- ✅ **Connection Management**: Proper resource cleanup
- ✅ **TaskFlow API**: Modern Airflow patterns

## 📚 Resources

- [CFPB API Documentation](https://cfpb.github.io/api/ccdb/api.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [Astronomer Documentation](https://www.astronomer.io/docs/)

## 📝 License

This project is licensed under the terms specified in the LICENSE file.

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add/update tests
5. Submit a pull request

## 📧 Support

For issues or questions:

- Check the troubleshooting section above
- Review Airflow task logs in the UI
- Consult the CFPB API documentation
