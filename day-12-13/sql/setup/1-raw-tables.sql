-- =============================================================================
-- Author: Savitha
-- Owner: data engineering team
-- Purpose: Register raw NYC taxi datasets in Athena (external tables over S3)
-- Dependencies:
--   - s3://day-7-glue-etl-quality/landing-data/taxi_zone_lookup.csv
--   - s3://day-7-glue-etl-quality/landing-data/transactions/yellow_tripdata_2025-08.parquet
-- =============================================================================

CREATE DATABASE IF NOT EXISTS raw;

-- --- Zones lookup (CSV)
DROP TABLE IF EXISTS raw.taxi_zone_lookup;

CREATE EXTERNAL TABLE raw.taxi_zone_lookup (
  locationid    int,
  borough       string,
  zone          string,
  service_zone  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '\"',
  'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://day-7-glue-etl-quality/landing-data/zone/'
TBLPROPERTIES ('skip.header.line.count'='1');


-- --- Yellow tripdata (Parquet)
DROP TABLE IF EXISTS raw.yellow_tripdata_2025_08;

CREATE EXTERNAL TABLE raw.yellow_tripdata_2025_08 (
  vendorid               int,
  tpep_pickup_datetime   timestamp,
  tpep_dropoff_datetime  timestamp,
  passenger_count        int,
  trip_distance          double,
  ratecodeid             int,
  store_and_fwd_flag     string,
  pulocationid           int,
  dolocationid           int,
  payment_type           int,
  fare_amount            double,
  extra                  double,
  mta_tax                double,
  tip_amount             double,
  tolls_amount           double,
  improvement_surcharge  double,
  total_amount           double,
  congestion_surcharge   double,
  airport_fee            double
)
STORED AS PARQUET
LOCATION 's3://day-7-glue-etl-quality/landing-data/transactions/';
