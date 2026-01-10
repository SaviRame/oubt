-- =============================================================================
-- Author: savihta
-- Owner: data engineering team
-- Purpose: Create an analytics-ready taxi trips table enriched with pickup/dropoff zone attributes.
-- Dependencies:
--   - raw.yellow_tripdata_2025_08
--   - raw.taxi_zone_lookup
-- Quality Expectations:
--   - pickup/dropoff timestamps present and dropoff >= pickup
--   - pulocationid and dolocationid not null
--   - total_amount >= 0 and trip_distance >= 0
-- Output:
--   - analytics.taxi_trip_enriched stored at s3://day11-12-sql-glue/curated/taxi_trip_enriched/
-- =============================================================================

CREATE DATABASE IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.taxi_trip_enriched;

CREATE TABLE analytics.taxi_trip_enriched
WITH (
  format = 'PARQUET',
  external_location = 's3://day11-12-sql-glue/curated/taxi_trip_enriched/',
  partitioned_by = ARRAY['pickup_date']
) AS
WITH base_trips AS (
  SELECT
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    CAST(date(tpep_pickup_datetime) AS date) AS pickup_date
  FROM raw.yellow_tripdata_2025_08
),
validated_trips AS (
  SELECT *
  FROM base_trips
  WHERE tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime >= tpep_pickup_datetime
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND trip_distance >= 0
    AND total_amount >= 0
),
zone_enriched AS (
  SELECT
    t.*,
    pu.borough      AS pickup_borough,
    pu.zone         AS pickup_zone,
    pu.service_zone AS pickup_service_zone,
    do.borough      AS dropoff_borough,
    do.zone         AS dropoff_zone,
    do.service_zone AS dropoff_service_zone,
    (date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0) AS trip_duration_minutes
  FROM validated_trips t
  LEFT JOIN raw.taxi_zone_lookup pu ON t.pulocationid = pu.locationid
  LEFT JOIN raw.taxi_zone_lookup do ON t.dolocationid = do.locationid
)
SELECT
  vendorid,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  trip_duration_minutes,
  pulocationid,
  dolocationid,
  pickup_borough,
  pickup_zone,
  pickup_service_zone,
  dropoff_borough,
  dropoff_zone,
  dropoff_service_zone,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  total_amount,
  pickup_date         
FROM zone_enriched;


SELECT COUNT(*) FROM analytics.taxi_trip_enriched;

SELECT COUNT(*) FROM analytics.daily_pickup_zone_metrics;

