-- Athena External Tables for Day 17 Hands-on
-- This script creates external tables for NYC taxi data with proper partitioning

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS taxi_analytics
COMMENT 'Database for NYC taxi analytics';

-- Use the database
USE taxi_analytics;

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS taxi_trips_partitioned;
DROP TABLE IF EXISTS taxi_zones;

-- Create external table for taxi zones (reference data)
CREATE EXTERNAL TABLE IF NOT EXISTS taxi_zones (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
LOCATION 's3://your-bucket/taxi-zones/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

-- Create partitioned external table for taxi trips
CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips_partitioned (
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
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE
)
PARTITIONED BY (year STRING, month STRING)
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://your-bucket/taxi-data/partitioned/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- Add partitions for existing data
-- Note: In a real scenario, you would automate this with a Glue crawler or script
ALTER TABLE taxi_trips_partitioned ADD PARTITION (year='2025', month='08')
LOCATION 's3://your-bucket/taxi-data/partitioned/year=2025/month=08/';

-- Create a view for easy querying with zone names
CREATE OR REPLACE VIEW taxi_trips_with_zones AS
SELECT 
    t.vendor_id,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.total_amount,
    t.payment_type,
    pz.location_id as pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    dz.location_id as dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,
    t.year,
    t.month
FROM taxi_trips_partitioned t
LEFT JOIN taxi_zones pz ON t.pulocationid = pz.location_id
LEFT JOIN taxi_zones dz ON t.dolocationid = dz.location_id;

-- Create a certified view for daily metrics (self-service analytics)
CREATE OR REPLACE VIEW daily_trip_metrics AS
SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    pickup_borough,
    pickup_zone,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare,
    SUM(total_amount) as total_revenue,
    '99.2% complete' as quality_score,
    'Owned by Operations Team' as owner
FROM taxi_trips_with_zones
WHERE total_amount > 0 AND trip_distance > 0
GROUP BY DATE(tpep_pickup_datetime), pickup_borough, pickup_zone
ORDER BY pickup_date, pickup_borough, pickup_zone;

-- Create a certified view for vendor performance
CREATE OR REPLACE VIEW vendor_performance AS
SELECT 
    vendor_id,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare,
    SUM(total_amount) as total_revenue,
    AVG(DATEDIFF(second, tpep_pickup_datetime, tpep_dropoff_datetime)/60.0) as avg_trip_duration_minutes,
    '99.2% complete' as quality_score,
    'Owned by Operations Team' as owner
FROM taxi_trips_partitioned
WHERE total_amount > 0 AND trip_distance > 0
GROUP BY vendor_id
ORDER BY vendor_id;

-- Create a data quality metrics view
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
    CURRENT_TIMESTAMP as calculated_at
FROM taxi_trips_partitioned
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
    CURRENT_TIMESTAMP as calculated_at
FROM taxi_trips_partitioned;

-- Create dataset ownership view
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
    'daily_trip_metrics' as table_name,
    'Analytics Team' as owner,
    'analytics-team@example.com' as contact_email,
    'Daily' as refresh_frequency,
    'certified' as certification_status,
    '2025-08-01' as certification_date
UNION ALL
SELECT 
    'vendor_performance' as table_name,
    'Analytics Team' as owner,
    'analytics-team@example.com' as contact_email,
    'Daily' as refresh_frequency,
    'certified' as certification_status,
    '2025-08-01' as certification_date;

-- Show tables
SHOW TABLES;

-- Grant permissions to workgroup (adjust as needed)
-- GRANT SELECT ON ALL TABLES IN SCHEMA default TO "self-service-analytics";