1. apply the following validation

```
WHERE tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime >= tpep_pickup_datetime
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND trip_distance >= 0
    AND total_amount >= 0
```

filter out records that do not meet the above criteria.

2. derive following columns

trip_duration_minutes
trip_day_of_week
fare_per_mile
speed_mph
tip_percentage


3. enrichment with master dim tables from master database. Add only gk key
enrichment with zone lookup data for pickup and dropoff locations.
vendor (you need to use vendor_xf table as well)
payment
rate_code

for now join based on the latest value from scd 2 dim tables.


4. write the output to silver layer in delta format. partitioned by trip start date. overwrite the output partition if already exist dynamically.

5. create external table if not already exist. 

6. USE sql as much as possible.




I want to build a curated table for trips data. the table should be build from silver.trips_processed. you can find the silver table definition in 



Implement a gold layer fact table from silver.trips_processed.

  Requirements:

  1. Create gold.fact_trips 
  Dims (group by)

  - trip_start_date
  - pickup_zone_gk
  - dropoff_zone_gk
  - vendor_gk
  - payment_type_gk
  - rate_code_gk

  Metrics

  - trip_count = count(*)
  - total_revenue = sum(total_amount)
  - total_fare = sum(fare_amount)
  - total_tips = sum(tip_amount)
  - total_distance = sum(trip_distance)
  - total_duration_minutes = sum(trip_duration_minutes)
  - avg_distance = avg(trip_distance)
  - avg_duration_minutes = avg(trip_duration_minutes)
  - avg_fare_per_mile = avg(fare_per_mile)
  - avg_speed_mph = avg(speed_mph)
  - avg_tip_percentage = avg(tip_percentage)

  2. Write  table as Delta to s3://week-4-oubt/gold/... partitioned by trip_start_date, using overwrite with dynamic partitioning.
  3. Use Spark/Glue style consistent with demo/src/trips_processed.py (job args, Glue context, etc.)
  4. use sql as much as possible
  5. 


