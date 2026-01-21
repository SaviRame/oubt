CREATE SCHEMA IF NOT EXISTS quality;

CREATE OR REPLACE VIEW quality.stg_taxi_trips_nulls AS
SELECT 'vendorid' AS column_name, COUNT(*)::BIGINT AS null_count
FROM staging.stg_taxi_trips WHERE vendorid IS NULL
UNION ALL
SELECT 'tpep_pickup_datetime', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE tpep_pickup_datetime IS NULL
UNION ALL
SELECT 'tpep_dropoff_datetime', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE tpep_dropoff_datetime IS NULL
UNION ALL
SELECT 'passenger_count', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE passenger_count IS NULL
UNION ALL
SELECT 'trip_distance', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE trip_distance IS NULL
UNION ALL
SELECT 'ratecodeid', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE ratecodeid IS NULL
UNION ALL
SELECT 'store_and_fwd_flag', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE store_and_fwd_flag IS NULL
UNION ALL
SELECT 'pulocationid', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE pulocationid IS NULL
UNION ALL
SELECT 'dolocationid', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE dolocationid IS NULL
UNION ALL
SELECT 'payment_type', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE payment_type IS NULL
UNION ALL
SELECT 'fare_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE fare_amount IS NULL
UNION ALL
SELECT 'extra', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE extra IS NULL
UNION ALL
SELECT 'mta_tax', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE mta_tax IS NULL
UNION ALL
SELECT 'tip_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE tip_amount IS NULL
UNION ALL
SELECT 'tolls_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE tolls_amount IS NULL
UNION ALL
SELECT 'improvement_surcharge', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE improvement_surcharge IS NULL
UNION ALL
SELECT 'total_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE total_amount IS NULL
UNION ALL
SELECT 'congestion_surcharge', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE congestion_surcharge IS NULL
UNION ALL
SELECT 'airport_fee', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE airport_fee IS NULL
UNION ALL
SELECT 'cbd_congestion_fee', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE cbd_congestion_fee IS NULL;

CREATE OR REPLACE VIEW quality.stg_taxi_trips_negative_values AS
SELECT 'passenger_count' AS column_name, COUNT(*)::BIGINT AS negative_count
FROM staging.stg_taxi_trips WHERE passenger_count < 0
UNION ALL
SELECT 'trip_distance', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE trip_distance < 0
UNION ALL
SELECT 'ratecodeid', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE ratecodeid < 0
UNION ALL
SELECT 'pulocationid', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE pulocationid < 0
UNION ALL
SELECT 'dolocationid', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE dolocationid < 0
UNION ALL
SELECT 'payment_type', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE payment_type < 0
UNION ALL
SELECT 'fare_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE fare_amount < 0
UNION ALL
SELECT 'extra', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE extra < 0
UNION ALL
SELECT 'mta_tax', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE mta_tax < 0
UNION ALL
SELECT 'tip_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE tip_amount < 0
UNION ALL
SELECT 'tolls_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE tolls_amount < 0
UNION ALL
SELECT 'improvement_surcharge', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE improvement_surcharge < 0
UNION ALL
SELECT 'total_amount', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE total_amount < 0
UNION ALL
SELECT 'congestion_surcharge', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE congestion_surcharge < 0
UNION ALL
SELECT 'airport_fee', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE airport_fee < 0
UNION ALL
SELECT 'cbd_congestion_fee', COUNT(*)::BIGINT
FROM staging.stg_taxi_trips WHERE cbd_congestion_fee < 0;
