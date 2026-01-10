SELECT 'pickup_date_null' AS test_name, COUNT(*) AS failing_rows
FROM analytics.daily_pickup_zone_metrics
WHERE pickup_date IS NULL

UNION ALL
SELECT 'pickup_zone_null' AS test_name, COUNT(*) AS failing_rows
FROM analytics.daily_pickup_zone_metrics
WHERE pickup_zone IS NULL

UNION ALL
SELECT 'nonpositive_trip_count' AS test_name, COUNT(*) AS failing_rows
FROM analytics.daily_pickup_zone_metrics
WHERE trip_count <= 0

UNION ALL
SELECT 'duplicate_daily_zone_key' AS test_name, COUNT(*) AS failing_rows
FROM (
  SELECT pickup_date, pickup_zone, COUNT(*) AS c
  FROM analytics.daily_pickup_zone_metrics
  GROUP BY 1,2
  HAVING COUNT(*) > 1
) d;
