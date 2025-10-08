# Quick Start Guide

Get your Consumer Complaint Pipeline up and running in 5 minutes!

## 🚀 5-Minute Setup

### Step 1: Setup Snowflake (2 minutes)

Run the provided SQL script in your Snowflake worksheet:

```sql
-- Copy and run: snowflake_setup.sql
```

This creates:

- Database: `CONSUMER_DATA`
- Schema: `PUBLIC`
- Table: `CONSUMER_COMPLAINTS`
- Warehouse: `COMPUTE_WH`
- Role: `ETL_ROLE` with appropriate permissions

### Step 2: Start Airflow (1 minute)

```bash
cd consumer_complaint_pipeline
astro dev start
```

Wait for Airflow to start (usually 30-60 seconds).

### Step 3: Configure Snowflake Connection (1 minute)

1. Open Airflow UI: <http://localhost:8080>
2. Go to **Admin → Connections**
3. Click **+** to add connection
4. Fill in:
   - **Connection Id**: `snowflake_default`
   - **Connection Type**: `Snowflake`
   - **Account**: `your_account` (e.g., `abc12345.us-east-1`)
   - **Login**: `your_username`
   - **Password**: `your_password`
   - **Extra**:

     ```json
     {
       "account": "your_account",
       "warehouse": "COMPUTE_WH",
       "database": "CONSUMER_DATA",
       "role": "SYSADMIN"
     }
     ```

5. Click **Test** then **Save**

### Step 4: Run the Pipeline (1 minute)

1. In Airflow UI, find the `consumer_complaints_etl` DAG
2. Toggle it **ON** (slider on the left)
3. Click the **▶️ Play** button
4. Select **Trigger DAG**
5. Watch it run! 🎉

### Step 5: Verify Data (30 seconds)

Run in Snowflake:

```sql
USE DATABASE CONSUMER_DATA;
USE SCHEMA PUBLIC;

-- Check data
SELECT COUNT(*) FROM CONSUMER_COMPLAINTS;

-- View sample
SELECT * FROM CONSUMER_COMPLAINTS LIMIT 10;
```

## ✅ What You Just Built

A production-ready ETL pipeline that:

- ✅ Extracts data from CFPB API daily
- ✅ Transforms and validates the data
- ✅ Loads it into Snowflake
- ✅ Runs data quality checks
- ✅ Includes comprehensive error handling
- ✅ Has full test coverage

## 📋 What Was Created

### Files Created

```
consumer_complaint_pipeline/
├── dags/
│   └── consumer_complaints_etl.py          # Main ETL DAG
├── include/
│   ├── __init__.py                         # Package init
│   ├── cfpb_api_client.py                  # API client
│   └── snowflake_loader.py                 # Data loader
├── tests/
│   └── dags/
│       └── test_consumer_complaints_etl.py # Tests
├── requirements.txt                         # Updated dependencies
├── README.md                                # Full documentation
├── SETUP_GUIDE.md                          # Detailed setup guide
├── QUICKSTART.md                           # This file
└── snowflake_setup.sql                     # Snowflake setup script
```

### DAG Tasks

1. **extract_complaints**: Fetch from CFPB API
2. **transform_complaints**: Clean and validate
3. **create_snowflake_table**: Ensure table exists
4. **load_to_snowflake**: Load data
5. **validate_data_quality**: Run checks

### Snowflake Objects

- **Database**: `CONSUMER_DATA`
- **Table**: `CONSUMER_COMPLAINTS` (19 columns)
- **Views**:
  - `COMPLAINTS_BY_PRODUCT`
  - `COMPLAINTS_BY_DATE`
  - `COMPLAINTS_BY_COMPANY`
  - `COMPLAINTS_BY_STATE`
  - `DATA_QUALITY_METRICS`

## 🎛️ Configuration Options

### Airflow Variables (Optional)

Customize behavior via **Admin → Variables**:

| Variable | Default | Description |
|----------|---------|-------------|
| `cfpb_lookback_days` | `1` | Days of data to fetch |
| `cfpb_max_records` | unlimited | Max records per run |
| `snowflake_database` | `CONSUMER_DATA` | Target database |
| `snowflake_schema` | `PUBLIC` | Target schema |
| `snowflake_warehouse` | `COMPUTE_WH` | Compute warehouse |

### DAG Schedule

Default: Daily at midnight

To change, edit `consumer_complaints_etl.py`:

```python
schedule="@daily"  # Change to @hourly, @weekly, etc.
```

## 📊 Monitoring

### In Airflow UI

- **Graph View**: Visual task dependencies
- **Task Logs**: Detailed execution logs
- **Task Duration**: Performance metrics

### In Snowflake

```sql
-- Data quality dashboard
SELECT * FROM DATA_QUALITY_METRICS;

-- Top companies by complaints
SELECT * FROM COMPLAINTS_BY_COMPANY LIMIT 10;

-- Recent trends
SELECT * FROM COMPLAINTS_BY_DATE LIMIT 30;
```

## 🔧 Common Customizations

### Change Schedule to Weekly

```python
schedule="0 0 * * 0"  # Every Sunday at midnight
```

### Fetch Last 7 Days

```bash
airflow variables set cfpb_lookback_days 7
```

### Email Alerts

In `consumer_complaints_etl.py`:

```python
default_args={
    "email": ["your-email@example.com"],
    "email_on_failure": True,
}
```

## 📚 Next Steps

1. **Read Full Documentation**: [README.md](README.md)
2. **Detailed Setup**: [SETUP_GUIDE.md](SETUP_GUIDE.md)
3. **Run Tests**: `astro dev pytest tests/dags/`
4. **Create Dashboards**: Use Snowflake views for BI tools
5. **Set Up Monitoring**: Configure alerts and notifications

## ❓ Need Help?

1. Check [SETUP_GUIDE.md](SETUP_GUIDE.md) troubleshooting section
2. Review Airflow task logs
3. Verify Snowflake permissions
4. Test API: <https://cfpb.github.io/api/ccdb/api.html>

## 🎉 You're All Set

Your pipeline is now:

- ✅ Running daily
- ✅ Loading fresh data
- ✅ Monitoring quality
- ✅ Ready for analysis

Happy data engineering! 🚀
