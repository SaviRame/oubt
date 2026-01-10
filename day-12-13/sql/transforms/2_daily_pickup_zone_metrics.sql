-- =============================================================================
-- Author: savitha
-- Owner: data engineering team
-- Purpose: Daily taxi KPIs by pickup zone for reporting and dashboards.
-- Dependencies:
--   - analytics.taxi_trip_enriched
-- Quality Expectations:
--   - (pickup_date, pickup_zone) should be unique
-- Output:
--   - analytics.daily_pickup_zone_metrics stored at s3://day11-12-sql-glue/curated/daily_pickup_zone_metrics/
-- =============================================================================

CREATE DATABASE IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.daily_pickup_zone_metrics;

CREATE TABLE analytics.daily_pickup_zone_metrics
WITH (
  format = 'PARQUET',
  external_location = 's3://day11-12-sql-glue/curated/daily_pickup_zone_metrics/',
  partitioned_by = ARRAY['pickup_date']
) AS
SELECT
  pickup_borough,
  pickup_zone,
  COUNT(*)                   AS trip_count,
  AVG(trip_distance)         AS avg_trip_distance,
  AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
  AVG(total_amount)          AS avg_total_amount,
  SUM(total_amount)          AS sum_total_amount,
  pickup_date
FROM analytics.taxi_trip_enriched
GROUP BY 1,2,pickup_date;

SHOW PARTITIONS analytics.taxi_trip_enriched;