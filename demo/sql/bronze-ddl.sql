-- Athena DDL for bronze layer tables

-- Database
CREATE DATABASE bronze LOCATION 's3://week-4-oubt/bronze';

-- Zone Table
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.zone (
    locationid INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://week-4-oubt/bronze/mdm/zone/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Rate Code Table
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.rate_code (
    ratecodeid INT,
    description STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://week-4-oubt/bronze/rate_code/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Payment Type Table
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.payment_type (
    payment_type INT,
    description STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://week-4-oubt/bronze/mdm/payment_type/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Vendor Table
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.vendor (
    vendor_id INT,
    vendor_name STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://week-4-oubt/bronze/mdm/vendor/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Yellow Tripdata Table (Parquet)
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.trips(
    vendorid               INT,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        DOUBLE,
    trip_distance          DOUBLE,
    ratecodeid             DOUBLE,
    store_and_fwd_flag     STRING,
    pulocationid           INT,
    dolocationid           INT,
    payment_type           BIGINT,
    fare_amount            DOUBLE,
    extra                  DOUBLE,
    mta_tax                DOUBLE,
    tip_amount             DOUBLE,
    tolls_amount           DOUBLE,
    improvement_surcharge  DOUBLE,
    total_amount           DOUBLE,
    congestion_surcharge   DOUBLE,
    airport_fee            DOUBLE,
    cbd_congestion_fee     DOUBLE
)
PARTITIONED BY (ingestion_date STRING)
STORED AS PARQUET
LOCATION 's3://week-4-oubt/bronze/trips/';




-- Partition management

-- Repair partitions (automatically discover and add partitions)
MSCK REPAIR TABLE bronze.zone;
MSCK REPAIR TABLE bronze.rate_code;
MSCK REPAIR TABLE bronze.payment_type;
MSCK REPAIR TABLE bronze.vendor;
MSCK REPAIR TABLE bronze.trips;
