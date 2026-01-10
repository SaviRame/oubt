-- Tests return counts. Any failing_rows > 0 should fail the job.

SELECT 'pickup_zone_null' AS test_name, COUNT(*) AS failing_rows
FROM analytics.taxi_trip_enriched
WHERE pickup_zone IS NULL

UNION ALL
SELECT 'dropoff_before_pickup' AS test_name, COUNT(*) AS failing_rows
FROM analytics.taxi_trip_enriched
WHERE tpep_dropoff_datetime < tpep_pickup_datetime

UNION ALL
SELECT 'negative_total_amount' AS test_name, COUNT(*) AS failing_rows
FROM analytics.taxi_trip_enriched
WHERE total_amount < 0

UNION ALL
SELECT 'negative_trip_distance' AS test_name, COUNT(*) AS failing_rows
FROM analytics.taxi_trip_enriched
WHERE trip_distance < 0;
