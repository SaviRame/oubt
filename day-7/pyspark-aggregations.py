"""
Day 7 Task 7.3: Aggregation Transformations (Silver → Gold)

This script creates a single multi-dimensional fact table for analytics and reporting
from the cleaned trip data in the Silver zone.

Output Table: trip_metrics
- Dimensions: trip_date, trip_hour, PULocationID, payment_method
- Additive Metrics (sum_*): Safe for rollup aggregations
- Average Metrics (avg_*): Pre-computed for convenience at base granularity

Important: Day 7 focuses on TRANSFORMATIONS ONLY - no validation or quality checks.
All records are aggregated without filtering. Day 8 will analyze quality of aggregations.

Input: s3a://day-7-spark-glue/silver/trips_cleaned/ (Delta format)
Output: s3a://day-7-spark-glue/gold/trip_metrics/ (Delta format, partitioned by trip_date)
"""

import sys

from pyspark.sql.functions import (
    avg,
    count,
    current_timestamp,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from spark_session import create_spark_session

# ============================================
# CONFIGURATION
# ============================================
S3_BUCKET = "day-7-spark-glue"
SILVER_TRIPS_PATH = f"s3a://{S3_BUCKET}/silver/trips_cleaned/"
GOLD_PATH = f"s3a://{S3_BUCKET}/gold"

# Single output path for consolidated fact table
GOLD_TRIP_METRICS_PATH = f"{GOLD_PATH}/trip_metrics/"


def read_silver_trips(spark, path):
    """
    Read cleaned trip data from silver zone (Delta format).

    This is the output from Task 7.1a (trip transformations).
    """
    print("\n=== Reading Silver Trip Data (Delta Format) ===")
    print(f"Path: {path}")

    try:
        df = spark.read.format("delta").load(path)
        record_count = df.count()
        print(f"Records Read: {record_count:,}")
        print(f"Columns: {len(df.columns)}")
        return df, record_count
    except Exception as e:
        print(f"Error reading silver trip data: {e}")
        raise


def create_trip_metrics(trips_df):
    """
    Create multi-dimensional trip metrics fact table.

    This consolidates the previous 4 separate aggregations into a single
    fact table with multiple dimensions, enabling flexible analytics.

    Dimensions:
    - trip_date: Date of pickup (partition key)
    - trip_hour: Hour of pickup (0-23)
    - PULocationID: Pickup zone ID
    - payment_method: Decoded payment type

    Additive Metrics (sum_*):
    - Safe for rollup aggregations across any dimension combination
    - Use these when aggregating to higher levels

    Average Metrics (avg_*):
    - Pre-computed for convenience at base granularity
    - WARNING: Do NOT sum these when rolling up - recalculate from sum_* / trip_count

    Query Examples:
    - Zone analysis: GROUP BY PULocationID
    - Hourly patterns: GROUP BY trip_hour
    - Daily summary: GROUP BY trip_date
    - Payment analysis: GROUP BY payment_method
    - Cross-dimensional: GROUP BY trip_date, trip_hour, payment_method
    """
    print("\n=== Creating trip_metrics Fact Table ===")
    print("Dimensions: trip_date, trip_hour, PULocationID, payment_method")

    trip_metrics = trips_df.groupBy(
        "trip_date",
        "trip_hour",
        "PULocationID",
        "payment_method",
    ).agg(
        # Count - additive
        count("*").alias("trip_count"),
        # Additive metrics (sum_*) - safe for rollup
        spark_round(spark_sum("total_amount"), 2).alias("sum_revenue"),
        spark_round(spark_sum("fare_amount"), 2).alias("sum_fare"),
        spark_round(spark_sum("tip_amount"), 2).alias("sum_tips"),
        spark_round(spark_sum("trip_distance"), 2).alias("sum_distance"),
        spark_round(spark_sum("trip_duration_minutes"), 2).alias("sum_duration_min"),
        spark_round(spark_sum("tip_percentage"), 2).alias("sum_tip_pct"),
        # Average metrics (avg_*) - convenience at base level only
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip"),
        spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        spark_round(avg("speed_mph"), 2).alias("avg_speed_mph"),
    )

    # Add processing timestamp
    trip_metrics = trip_metrics.withColumn("aggregated_at", current_timestamp())

    record_count = trip_metrics.count()
    print(f"  Dimension Combinations: {record_count:,}")
    print("\n  Additive Metrics (sum_*): sum_revenue, sum_fare, sum_tips,")
    print("                            sum_distance, sum_duration_min, sum_tip_pct")
    print("\n  Average Metrics (avg_*): avg_fare, avg_tip, avg_tip_pct,")
    print("                           avg_distance, avg_duration_min, avg_speed_mph")

    return trip_metrics, record_count


def write_trip_metrics(df, path):
    """
    Write trip metrics to gold zone as Delta format, partitioned by trip_date.

    Partitioning by trip_date enables:
    - Efficient time-range queries
    - Incremental updates by date
    - Partition pruning for better performance
    """
    print("\n=== Writing trip_metrics (Delta Format) ===")
    print(f"Path: {path}")
    print("Partition Key: trip_date")

    try:
        df.write.format("delta").mode("overwrite").partitionBy("trip_date").option(
            "overwriteSchema", "true"
        ).save(path)
        print("Write completed successfully")
    except Exception as e:
        print(f"Error writing trip_metrics: {e}")
        raise


def print_summary(input_count, output_count):
    """Print aggregation transformation summary."""
    print("\n" + "=" * 70)
    print("=== AGGREGATION TRANSFORMATION SUMMARY ===")
    print("=" * 70)
    print(f"\nInput: {SILVER_TRIPS_PATH}")
    print(f"Records Processed: {input_count:,}")

    print("\n" + "-" * 70)
    print("=== OUTPUT: trip_metrics (Consolidated Fact Table) ===")
    print(f"Location: {GOLD_TRIP_METRICS_PATH}")
    print("Format: Delta Lake")
    print("Partition Key: trip_date")
    print(f"Dimension Combinations: {output_count:,}")

    print("\n  Dimensions:")
    print("    - trip_date: Date of pickup")
    print("    - trip_hour: Hour of pickup (0-23)")
    print("    - PULocationID: Pickup zone ID")
    print("    - payment_method: Decoded payment type")

    print("\n  Additive Metrics (safe for rollup):")
    print("    - trip_count: Number of trips")
    print("    - sum_revenue: Total revenue (total_amount)")
    print("    - sum_fare: Total fare amount")
    print("    - sum_tips: Total tip amount")
    print("    - sum_distance: Total distance traveled")
    print("    - sum_duration_min: Total trip duration in minutes")
    print("    - sum_tip_pct: Sum of tip percentages")

    print("\n  Average Metrics (base level only - do NOT rollup):")
    print("    - avg_fare: Average fare amount")
    print("    - avg_tip: Average tip amount")
    print("    - avg_tip_pct: Average tip percentage")
    print("    - avg_distance: Average trip distance")
    print("    - avg_duration_min: Average trip duration")
    print("    - avg_speed_mph: Average speed")

    print("\n" + "-" * 70)
    print("\n  Query Examples:")
    print("    -- Zone analysis (replaces trips_per_zone)")
    print("    SELECT PULocationID, SUM(trip_count), SUM(sum_revenue)")
    print("    FROM trip_metrics GROUP BY PULocationID;")
    print()
    print("    -- Hourly patterns (replaces hourly_metrics)")
    print("    SELECT trip_hour, SUM(trip_count), SUM(sum_revenue)")
    print("    FROM trip_metrics GROUP BY trip_hour ORDER BY trip_hour;")
    print()
    print("    -- Daily summary (replaces daily_summary)")
    print("    SELECT trip_date, SUM(trip_count), SUM(sum_revenue)")
    print("    FROM trip_metrics GROUP BY trip_date ORDER BY trip_date;")
    print()
    print("    -- Payment analysis (replaces payment_analysis)")
    print("    SELECT payment_method, SUM(trip_count), SUM(sum_revenue)")
    print("    FROM trip_metrics GROUP BY payment_method;")
    print()
    print("    -- Cross-dimensional (NEW capability)")
    print("    SELECT trip_date, trip_hour, payment_method, SUM(trip_count)")
    print("    FROM trip_metrics WHERE PULocationID = 132")
    print("    GROUP BY trip_date, trip_hour, payment_method;")

    print("\n" + "-" * 70)
    print("\nNote: No filtering performed. All records aggregated.")
    print("      Day 8 will analyze quality of aggregations.")
    print("=" * 70)


def main():
    """Main execution function."""
    print("\n" + "=" * 70)
    print("Day 7 Task 7.3: Aggregation Transformations (Silver → Gold)")
    print("Consolidated Multi-Dimensional Fact Table")
    print("=" * 70)

    # Initialize Spark
    spark = create_spark_session("AggregationTransformations")

    try:
        # Step 1: Read cleaned trips from silver zone (Delta format)
        trips_df, input_count = read_silver_trips(spark, SILVER_TRIPS_PATH)

        # Cache the DataFrame for aggregation
        trips_df.cache()
        print("\nDataFrame cached for aggregation")

        # Step 2: Create consolidated trip_metrics fact table
        trip_metrics, output_count = create_trip_metrics(trips_df)

        # Step 3: Write to gold zone as Delta format, partitioned by trip_date
        write_trip_metrics(trip_metrics, GOLD_TRIP_METRICS_PATH)

        # Unpersist cached DataFrame
        trips_df.unpersist()

        # Print summary
        print_summary(input_count, output_count)

        print("\nAggregation transformations completed successfully!")

    except Exception as e:
        print(f"\nError during aggregation: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
