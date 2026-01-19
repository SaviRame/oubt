# Day 17: Athena & Serverless Analytics - Hands-on Plan

## Overview

This hands-on plan covers implementing serverless analytics with Amazon Athena, dimensional modeling with SCD Type 2 in Redshift, and creating governed QuickSight dashboards. The focus is on building a self-service analytics environment with proper governance guardrails.

## Learning Objectives

- Implement serverless querying with Athena using Presto/Trino
- Create and optimize external tables with proper partitioning
- Deploy a dimensional model in Redshift with SCD Type 2
- Design QuickSight dashboards with governance indicators
- Implement self-service analytics with guardrails

---

## 1. Technical Content

### 1.1 Serverless Querying with Presto/Trino

**Concepts:**
- Athena is built on Presto/Trino, a distributed SQL query engine
- Serverless architecture eliminates infrastructure management
- Pay-per-query pricing model based on data scanned
- Standard SQL with ANSI SQL support

**Implementation Steps:**
1. Create Athena workgroup with query result location
2. Configure query timeout and data usage limits
3. Set up query logging for monitoring

```sql
-- Example Athena query
SELECT 
    COUNT(*) as trip_count,
    DATE(tpep_pickup_datetime) as pickup_date,
    borough
FROM taxi_trips
GROUP BY DATE(tpep_pickup_datetime), borough
ORDER BY pickup_date;
```

### 1.2 Creating External Tables

**Concepts:**
- External tables reference data stored in S3 without moving it
- Schema-on-read approach allows flexibility
- Support for multiple formats (Parquet, ORC, JSON, CSV, etc.)
- Integration with Glue Data Catalog for metadata management

**Implementation Steps:**
1. Create database in Glue Data Catalog
2. Define table schema with proper data types
3. Specify S3 location and data format
4. Configure partitioning if applicable

```sql
-- Example: Create external table for taxi trips
CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips (
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    ratecode_id SMALLINT,
    store_and_fwd_flag STRING,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://your-bucket/taxi-data/trips/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### 1.3 Partitioning Strategies

**Concepts:**
- Partitioning divides tables into manageable segments based on column values
- Reduces amount of data scanned and improves query performance
- Common partition keys: date, region, category
- Partition projection can eliminate need for Glue crawler

**Implementation Strategies:**
1. **Date-based partitioning**: Most common for time-series data
2. **Hierarchical partitioning**: Multiple partition keys (e.g., year/month/day)
3. **Geographic partitioning**: By region, country, etc.

```sql
-- Example: Create partitioned table
CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips_partitioned (
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    -- other columns
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://your-bucket/taxi-data/partitioned/';

-- Add partitions
ALTER TABLE taxi_trips_partitioned ADD PARTITION (year='2025', month='08', day='01')
LOCATION 's3://your-bucket/taxi-data/partitioned/year=2025/month=08/day=01/';
```

### 1.4 Query Optimization and Cost Management

**Optimization Techniques:**
1. **Partition pruning**: Ensure queries filter on partition keys
2. **Column pruning**: Select only needed columns
3. **Predicate pushdown**: Apply filters early in query execution
4. **Compression**: Use columnar formats like Parquet or ORC
5. **File sizing**: Optimal file size (128-512MB in S3)

**Cost Management Strategies:**
1. **Data compression**: Reduce bytes scanned
2. **Partitioning**: Limit data scanned
3. **Columnar formats**: More efficient than row-based
4. **Query result caching**: Avoid repeated scans
5. **Set data usage controls**: Limit bytes scanned per query

```sql
-- Optimized query example
SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    borough,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare
FROM taxi_trips
WHERE year = '2025' AND month = '08'  -- Partition pruning
GROUP BY DATE(tpep_pickup_datetime), borough
ORDER BY pickup_date, borough;
```

### 1.5 Integration with Glue Catalog

**Concepts:**
- Glue Data Catalog stores metadata for data sources
- Centralized metadata repository for Athena, Redshift Spectrum, EMR
- Schema versioning and evolution support
- Automatic schema detection with crawlers

**Implementation Steps:**
1. Create Glue database
2. Set up Glue crawler to discover schemas
3. Configure crawler to run on schedule
4. Use Glue ETL for data transformation if needed

```python
# Example: Using Boto3 to create Glue database
import boto3

glue_client = boto3.client('glue')

response = glue_client.create_database(
    DatabaseInput={
        'Name': 'taxi_analytics',
        'Description': 'Database for NYC taxi analytics'
    }
)
```

### 1.6 Query Performance Tuning

**Performance Tuning Techniques:**
1. **Use appropriate file formats**: Parquet, ORC for analytics
2. **Optimal file sizes**: 128-512MB per file
3. **Partitioning strategy**: Balance between too many and too few partitions
4. **Column statistics**: Let Athena collect statistics
5. **Query structure**: Avoid SELECT *, use LIMIT for testing
6. **Convert data types**: Use smallest appropriate types (INT vs BIGINT)

**Advanced Techniques:**
1. **Materialized views**: For frequently accessed aggregations
2. **Athena ML**: Use machine learning functions within queries
3. **Federated queries**: Query across multiple data sources

### 1.7 Columnar Format Optimization

**Best Practices:**
1. **Use Parquet or ORC**: Columnar formats with compression
2. **Sort data**: By frequently filtered columns
3. **Optimal compression**: Snappy for speed, Zstandard for compression ratio
4. **Data type optimization**: Use smallest appropriate types
5. **String encoding**: Dictionary encoding for low-cardinality strings

```sql
-- Example: Creating optimized Parquet files with PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

spark = SparkSession.builder \
    .appName("Optimize Taxi Data") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Read data
df = spark.read.parquet("s3://your-bucket/raw-taxi-data/")

# Add partition columns
df = df.withColumn("year", year(col("tpep_pickup_datetime")))
df = df.withColumn("month", month(col("tpep_pickup_datetime")))
df = df.withColumn("day", dayofmonth(col("tpep_pickup_datetime")))

# Sort by pickup datetime and zone
df = df.orderBy("tpep_pickup_datetime", "pulocationid")

# Write partitioned Parquet
df.write.partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .parquet("s3://your-bucket/optimized-taxi-data/")
```

### 1.8 AWS QuickSight for Visualization

**QuickSight Concepts:**
- Cloud-based business intelligence service
- Serverless visualization with SPICE engine
- Integration with Athena, Redshift, and other data sources
- Dashboard sharing with fine-grained access control

**Implementation Steps:**
1. Create QuickSight account with proper permissions
2. Set up data sources (Athena, Redshift)
3. Create datasets with proper relationships
4. Design visualizations and dashboards
5. Configure sharing and permissions

---

## 2. Self-Service with Guardrails

### 2.1 Catalog with Certified Datasets

**Implementation:**
1. **Data Certification Process**:
   - Define certification criteria (quality, completeness, timeliness)
   - Establish stewardship roles and responsibilities
   - Create certification workflow (request → review → approve/certify)

2. **Metadata Management**:
   - Tag certified datasets in Glue Data Catalog
   - Document data lineage and quality metrics
   - Maintain data dictionary with business definitions

```sql
-- Example: Adding metadata to Glue table
ALTER TABLE taxi_trips SET TBLPROPERTIES (
    'certification_status' = 'certified',
    'certification_date' = '2025-08-01',
    'data_steward' = 'data-team@example.com',
    'quality_score' = '99.2',
    'refresh_frequency' = 'daily',
    'owner' = 'Operations Team'
);
```

### 2.2 Pre-built Queries/Views

**Implementation:**
1. **Create Certified Views**:
   - Develop standard business views
   - Implement row-level security where needed
   - Document view purpose and usage guidelines

2. **View Examples**:
   - Daily trip metrics by zone
   - Monthly revenue trends
   - Vendor performance comparisons
   - Quality score dashboards

```sql
-- Example: Certified view for daily trip metrics
CREATE OR REPLACE VIEW daily_trip_metrics AS
SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    borough,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare,
    SUM(total_amount) as total_revenue,
    '99.2% complete' as quality_score,
    'Owned by Operations Team' as owner
FROM taxi_trips
WHERE total_amount > 0 AND trip_distance > 0
GROUP BY DATE(tpep_pickup_datetime), borough
WITH CHECK OPTION;
```

### 2.3 Query Cost Limits

**Implementation:**
1. **Workgroup Configuration**:
   - Create separate workgroups for different user types
   - Set data usage controls per workgroup
   - Configure query timeout settings

2. **Monitoring and Alerting**:
   - Set up CloudWatch metrics for query monitoring
   - Create alerts for expensive queries
   - Implement query review process for high-cost queries

```python
# Example: Setting up Athena workgroup with usage controls
import boto3

athena_client = boto3.client('athena')

response = athena_client.create_work_group(
    Name='self-service-analytics',
    Configuration={
        'ResultConfiguration': {
            'OutputLocation': 's3://your-bucket/athena-results/'
        },
        'EnforceWorkGroupConfiguration': True,
        'PublishCloudWatchMetricsEnabled': True,
        'BytesScannedCutoffPerQuery': 1073741824,  # 1 GB limit
        'RequesterPaysEnabled': False
    },
    Description='Self-service analytics with usage controls'
)
```

---

## 3. Hands-on Implementation

### 3.1 Enhance Existing Redshift Dimensional Model

**Prerequisites:**
- Redshift cluster provisioned with existing Day 16 tables
- S3 bucket with taxi data
- IAM role with appropriate permissions

**Implementation Steps:**

#### Step 1: Enhance Existing Dimension Tables

The Day 16 implementation already includes the core dimensional model with:
- `mdm.zone_dim` - Zone dimension table
- `mdm.vendor_dim` - Vendor dimension table  
- `analytics.fact_taxi_trips` - Fact table for taxi trips

For Day 17, we'll enhance this model with:
1. Adding date dimension for time-based analysis
2. Adding SCD Type 2 capabilities to existing dimensions
3. Creating governance-enabled analytics views

```sql
-- Create date dimension (if it doesn't exist)
CREATE TABLE IF NOT EXISTS mdm.date_dim (
    date_sk BIGINT PRIMARY KEY,
    date_actual DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    record_created_at TIMESTAMP DEFAULT GETDATE()
);

-- Add date_sk foreign key columns to fact table if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fact_taxi_trips_pickup_date_sk_fkey') THEN
        ALTER TABLE analytics.fact_taxi_trips ADD COLUMN pickup_date_sk BIGINT;
        ALTER TABLE analytics.fact_taxi_trips ADD CONSTRAINT fact_taxi_trips_pickup_date_sk_fkey 
            FOREIGN KEY (pickup_date_sk) REFERENCES mdm.date_dim(date_sk);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fact_taxi_trips_dropoff_date_sk_fkey') THEN
        ALTER TABLE analytics.fact_taxi_trips ADD COLUMN dropoff_date_sk BIGINT;
        ALTER TABLE analytics.fact_taxi_trips ADD CONSTRAINT fact_taxi_trips_dropoff_date_sk_fkey 
            FOREIGN KEY (dropoff_date_sk) REFERENCES mdm.date_dim(date_sk);
    END IF;
END $$;
```

#### Step 2: Implement SCD Type 2 Procedures

```sql
-- Create procedure to handle SCD Type 2 for zones
CREATE OR REPLACE PROCEDURE mdm.apply_zone_scd2()
AS $$
DECLARE
    current_timestamp TIMESTAMP := GETDATE();
BEGIN
    -- Expire current records that have changed
    UPDATE mdm.zone_dim
    SET expiration_date = current_timestamp,
        is_current = FALSE,
        record_updated_at = current_timestamp
    WHERE is_current = TRUE
    AND zone_id IN (
        SELECT DISTINCT zone_id
        FROM staging.stg_taxi_zones
        WHERE NOT EXISTS (
            SELECT 1
            FROM mdm.zone_dim zd
            WHERE zd.is_current = TRUE
            AND zd.zone_id = staging.stg_taxi_zones.zone_id
            AND zd.borough = staging.stg_taxi_zones.borough
            AND zd.zone_name = staging.stg_taxi_zones.zone_name
            AND zd.service_zone = staging.stg_taxi_zones.service_zone
        )
    );
    
    -- Insert new or changed records
    INSERT INTO mdm.zone_dim (
        zone_id, borough, zone_name, service_zone,
        effective_date, expiration_date, is_current,
        record_created_at
    )
    SELECT 
        zone_id, borough, zone_name, service_zone,
        current_timestamp, NULL, TRUE,
        current_timestamp
    FROM staging.stg_taxi_zones
    WHERE NOT EXISTS (
        SELECT 1
        FROM mdm.zone_dim zd
        WHERE zd.is_current = TRUE
        AND zd.zone_id = staging.stg_taxi_zones.zone_id
    );
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- Create procedure to handle SCD Type 2 for vendors
CREATE OR REPLACE PROCEDURE mdm.apply_vendor_scd2()
AS $$
DECLARE
    current_timestamp TIMESTAMP := GETDATE();
BEGIN
    -- Expire current records that have changed
    UPDATE mdm.vendor_dim
    SET expiration_date = current_timestamp,
        is_current = FALSE,
        record_updated_at = current_timestamp
    WHERE is_current = TRUE
    AND vendor_id IN (
        SELECT DISTINCT vendor_id
        FROM staging.stg_vendors
        WHERE NOT EXISTS (
            SELECT 1
            FROM mdm.vendor_dim vd
            WHERE vd.is_current = TRUE
            AND vd.vendor_id = staging.stg_vendors.vendor_id
            AND vd.vendor_name = staging.stg_vendors.vendor_name
            AND vd.company_name = staging.stg_vendors.company_name
            AND vd.address = staging.stg_vendors.address
            AND vd.city = staging.stg_vendors.city
            AND vd.state = staging.stg_vendors.state
            AND vd.phone = staging.stg_vendors.phone
            AND vd.license_number = staging.stg_vendors.license_number
        )
    );
    
    -- Insert new or changed records
    INSERT INTO mdm.vendor_dim (
        vendor_id, vendor_name, company_name, address, city, state, phone, license_number,
        effective_date, expiration_date, is_current,
        record_created_at
    )
    SELECT 
        vendor_id, vendor_name, company_name, address, city, state, phone, license_number,
        current_timestamp, NULL, TRUE,
        current_timestamp
    FROM staging.stg_vendors
    WHERE NOT EXISTS (
        SELECT 1
        FROM mdm.vendor_dim vd
        WHERE vd.is_current = TRUE
        AND vd.vendor_id = staging.stg_vendors.vendor_id
    );
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;
```

#### Step 3: Create Fact Table

```sql
-- Create fact table for taxi trips
CREATE TABLE analytics.fact_taxi_trips (
    trip_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    vendor_sk BIGINT NOT NULL,
    pickup_zone_sk BIGINT NOT NULL,
    dropoff_zone_sk BIGINT NOT NULL,
    pickup_date_sk BIGINT NOT NULL,
    dropoff_date_sk BIGINT NOT NULL,
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    trip_duration_minutes DECIMAL(10,2),
    passenger_count SMALLINT,
    trip_distance DECIMAL(10,2),
    ratecode_id SMALLINT,
    store_and_fwd_flag VARCHAR(1),
    payment_type SMALLINT,
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    record_created_at TIMESTAMP DEFAULT GETDATE(),
    FOREIGN KEY (vendor_sk) REFERENCES mdm.vendor_dim(zone_sk),
    FOREIGN KEY (pickup_zone_sk) REFERENCES mdm.zone_dim(zone_sk),
    FOREIGN KEY (dropoff_zone_sk) REFERENCES mdm.zone_dim(zone_sk),
    FOREIGN KEY (pickup_date_sk) REFERENCES mdm.date_dim(date_sk),
    FOREIGN KEY (dropoff_date_sk) REFERENCES mdm.date_dim(date_sk)
);

-- Create indexes for performance
CREATE INDEX idx_fact_taxi_trips_pickup_date ON analytics.fact_taxi_trips (pickup_date_sk);
CREATE INDEX idx_fact_taxi_trips_pickup_zone ON analytics.fact_taxi_trips (pickup_zone_sk);
CREATE INDEX idx_fact_taxi_trips_dropoff_zone ON analytics.fact_taxi_trips (dropoff_zone_sk);
CREATE INDEX idx_fact_taxi_trips_vendor ON analytics.fact_taxi_trips (vendor_sk);
```

#### Step 4: Load Data into Fact Table

```sql
-- Create procedure to load fact table
CREATE OR REPLACE PROCEDURE analytics.load_fact_taxi_trips()
AS $$
DECLARE
    load_count INTEGER;
    reject_count INTEGER;
    total_count INTEGER;
BEGIN
    -- Get total count from staging
    SELECT COUNT(*) INTO total_count FROM staging.stg_taxi_trips;
    
    -- Insert valid records into fact table
    INSERT INTO analytics.fact_taxi_trips (
        vendor_sk, pickup_zone_sk, dropoff_zone_sk, 
        pickup_date_sk, dropoff_date_sk,
        tpep_pickup_datetime, tpep_dropoff_datetime,
        trip_duration_minutes, passenger_count, trip_distance,
        ratecode_id, store_and_fwd_flag, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge, airport_fee
    )
    SELECT 
        vd.zone_sk, 
        pz.zone_sk, 
        dz.zone_sk,
        pd.date_sk,
        dd.date_sk,
        t.tpep_pickup_datetime,
        t.tpep_dropoff_datetime,
        DATEDIFF(second, t.tpep_pickup_datetime, t.tpep_dropoff_datetime)/60.0,
        t.passenger_count,
        t.trip_distance,
        t.ratecode_id,
        t.store_and_fwd_flag,
        t.payment_type,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee
    FROM staging.stg_taxi_trips t
    JOIN mdm.vendor_dim vd ON t.vendorid = vd.vendor_id AND vd.is_current = TRUE
    JOIN mdm.zone_dim pz ON t.pulocationid = pz.zone_id AND pz.is_current = TRUE
    JOIN mdm.zone_dim dz ON t.dolocationid = dz.zone_id AND dz.is_current = TRUE
    JOIN mdm.date_dim pd ON DATE(t.tpep_pickup_datetime) = pd.date_actual
    JOIN mdm.date_dim dd ON DATE(t.tpep_dropoff_datetime) = dd.date_actual
    WHERE t.tpep_pickup_datetime <= t.tpep_dropoff_datetime
      AND t.total_amount > 0
      AND t.trip_distance > 0
      AND t.passenger_count BETWEEN 0 AND 6;
    
    -- Get count of loaded records
    GET DIAGNOSTICS load_count = ROW_COUNT;
    
    -- Calculate rejected count
    reject_count := total_count - load_count;
    
    -- Log loading statistics
    RAISE NOTICE 'Fact table load completed: % loaded, % rejected, % total', 
                load_count, reject_count, total_count;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;
```

### 3.2 Create Analytics Dashboard in QuickSight with Governance Indicators

#### Step 1: Set up QuickSight Data Sources

1. **Create Athena Data Source**:
   - Connect QuickSight to Athena
   - Select appropriate workgroup
   - Configure S3 result bucket access

2. **Create Redshift Data Source**:
   - Connect QuickSight to Redshift cluster
   - Provide connection details
   - Configure VPC if needed

#### Step 2: Create Datasets

1. **Create Taxi Trips Dataset**:
   - Use Athena as source
   - Select the taxi_trips table
   - Apply any necessary transformations
   - Configure SPICE refresh schedule

2. **Create Redshift Analytics Dataset**:
   - Use Redshift as source
   - Select fact and dimension tables
   - Define relationships between tables
   - Configure SPICE refresh schedule

#### Step 3: Design Dashboard with Governance Indicators

**Dashboard Components:**

1. **Header Section**:
   - Title: "NYC Taxi Analytics Dashboard"
   - Date range filter
   - Quality score indicator: "99.2% Complete"
   - Ownership label: "Owned by Operations Team"

2. **Key Metrics Section**:
   - Total trips
   - Total revenue
   - Average trip distance
   - Average fare amount

3. **Time Series Analysis**:
   - Daily trip count trend
   - Daily revenue trend
   - Comparison with previous period

4. **Geographic Analysis**:
   - Trips by borough (bar chart)
   - Pickup zones heatmap (if coordinates available)
   - Top pickup/dropoff zones

5. **Performance Analysis**:
   - Vendor performance comparison
   - Trip duration distribution
   - Payment type breakdown

6. **Governance Section**:
   - Data freshness indicator
   - Last refresh timestamp
   - Data quality score trend
   - Certification status

#### Step 4: Implement Quality Score Indicators

**Quality Score Calculation**:
- Completeness: Percentage of non-null values
- Validity: Percentage of values within expected ranges
- Timeliness: How recent is the data
- Consistency: Cross-field validation

```sql
-- Example: Quality score calculation view
CREATE OR REPLACE VIEW data_quality_metrics AS
SELECT 
    'taxi_trips' as table_name,
    'completeness' as metric_name,
    ROUND(AVG(CASE WHEN vendor_id IS NOT NULL 
                   AND tpep_pickup_datetime IS NOT NULL
                   AND tpep_dropoff_datetime IS NOT NULL
                   AND passenger_count IS NOT NULL
                   AND trip_distance IS NOT NULL
                   AND total_amount IS NOT NULL
                   THEN 1 ELSE 0 END) * 100, 1) as metric_value,
    'percent' as metric_unit,
    GETDATE() as calculated_at
FROM taxi_trips
UNION ALL
SELECT 
    'taxi_trips' as table_name,
    'validity' as metric_name,
    ROUND(AVG(CASE WHEN total_amount > 0 
                   AND trip_distance > 0
                   AND passenger_count BETWEEN 0 AND 6
                   AND tpep_pickup_datetime <= tpep_dropoff_datetime
                   THEN 1 ELSE 0 END) * 100, 1) as metric_value,
    'percent' as metric_unit,
    GETDATE() as calculated_at
FROM taxi_trips;
```

#### Step 5: Add Ownership Labels

**Implementation**:
1. **Metadata Management**:
   - Tag datasets with ownership information
   - Document data stewards and contact information
   - Define approval workflows for changes

2. **Visual Indicators**:
   - Add ownership labels to dashboard
   - Include contact information for data questions
   - Display certification status

```sql
-- Example: Ownership metadata view
CREATE OR REPLACE VIEW dataset_ownership AS
SELECT 
    'taxi_trips' as table_name,
    'Operations Team' as owner,
    'ops-team@example.com' as contact_email,
    'Daily' as refresh_frequency,
    'certified' as certification_status,
    '2025-08-01' as certification_date
UNION ALL
SELECT 
    'fact_taxi_trips' as table_name,
    'Analytics Team' as owner,
    'analytics-team@example.com' as contact_email,
    'Daily' as refresh_frequency,
    'certified' as certification_status,
    '2025-08-01' as certification_date;
```

---

## 4. Implementation Sequence

### Phase 1: Environment Setup (Day 1)
1. Create S3 buckets for data storage
2. Set up Athena workgroups with usage controls
3. Configure Glue Data Catalog
4. Set up Redshift cluster
5. Create IAM roles with appropriate permissions

### Phase 2: Data Foundation (Day 2)
1. Create external tables in Athena
2. Implement partitioning strategy
3. Set up Glue crawlers
4. Optimize data formats
5. Create data quality checks

### Phase 3: Dimensional Model (Day 3)
1. Create dimension tables with SCD Type 2 support
2. Implement SCD procedures
3. Create fact table
4. Develop fact loading procedure
5. Test dimensional model with sample data

### Phase 4: Analytics Layer (Day 4)
1. Create certified views in Athena
2. Build pre-built queries for common use cases
3. Implement data quality metrics
4. Set up ownership metadata
5. Create governance guardrails

### Phase 5: Visualization (Day 5)
1. Set up QuickSight account and permissions
2. Create data sources and datasets
3. Design dashboard layout
4. Add governance indicators
5. Configure sharing and access controls

---

## 5. Success Criteria

### Technical Success Criteria
- [ ] Athena queries complete within expected timeframes
- [ ] Redshift dimensional model properly implements SCD Type 2
- [ ] QuickSight dashboard loads in under 10 seconds
- [ ] Data quality score is 95% or higher
- [ ] All queries stay within cost limits

### Business Success Criteria
- [ ] Users can self-serve analytics without IT intervention
- [ ] Governance guardrails prevent unauthorized data access
- [ ] Dashboard provides actionable insights
- [ ] Data ownership is clearly documented
- [ ] Solution scales to handle increased data volumes

---

## 6. Troubleshooting Guide

### Common Issues and Solutions

1. **Athena Query Performance Issues**
   - **Problem**: Queries are slow or timing out
   - **Solution**: Check partitioning, reduce data scanned, optimize file formats

2. **SCD Type 2 Implementation Errors**
   - **Problem**: Duplicate records or incorrect history
   - **Solution**: Verify effective/expiration date logic, check for missing updates

3. **QuickSight Data Refresh Failures**
   - **Problem**: SPICE refresh fails or takes too long
   - **Solution**: Check dataset permissions, reduce data volume, optimize queries

4. **Permission Issues**
   - **Problem**: Access denied errors when accessing data
   - **Solution**: Verify IAM roles, bucket policies, and table permissions

5. **Cost Overruns**
   - **Problem**: Unexpected charges from Athena or QuickSight
   - **Solution**: Implement usage controls, monitor CloudWatch metrics, set up alerts

---

## 7. Next Steps

1. **Advanced Analytics**: Implement ML models for trip prediction
2. **Real-time Analytics**: Add streaming data capabilities
3. **Advanced Governance**: Implement data lineage and impact analysis
4. **Performance Optimization**: Fine-tune query performance and cost
5. **User Training**: Train business users on self-service analytics

---

## 8. Resources

- [Amazon Athena Documentation](https://docs.aws.amazon.com/athena/)
- [Amazon Redshift Documentation](https://docs.aws.amazon.com/redshift/)
- [Amazon QuickSight Documentation](https://docs.aws.amazon.com/quicksight/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Athena Best Practices](https://docs.aws.amazon.com/athena/latest/ug/best-practices.html)
- [QuickSight Dashboard Design](https://docs.aws.amazon.com/quicksight/latest/user/designing-dashboards.html)