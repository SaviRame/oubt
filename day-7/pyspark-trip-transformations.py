"""
Day 7 Task 7.1a: Trip Data Transformations (Bronze → Silver)

This script transforms raw NYC Yellow Taxi trip data from the Bronze zone
to the Silver zone by adding derived fields and decoding reference values.

Transformation Categories:
1. Time-based derived fields (duration, hour, day of week, date)
2. Trip efficiency metrics (speed, fare per mile/minute)
3. Tipping analysis (tip percentage)
4. Business flags (airport, rush hour, weekend)
5. Decoded reference values (payment method, vendor, rate type)

Important: Day 7 focuses on TRANSFORMATIONS ONLY - no validation or quality checks.
All quality checks happen in Day 8.

Input: s3a://day-7-spark-glue/bronze/yellow_tripdata/
Output: s3a://day-7-spark-glue/silver/trips_cleaned/
"""

import sys

from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofweek,
    hour,
    lit,
    to_date,
    when,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import DoubleType
from spark_session import create_spark_session

# ============================================
# CONFIGURATION
# ============================================
S3_BUCKET = "day-7-spark-glue"
BRONZE_TRIPS_PATH = f"s3a://{S3_BUCKET}/bronze/yellow_tripdata/"
SILVER_TRIPS_PATH = f"s3a://{S3_BUCKET}/silver/trips_cleaned/"


def read_bronze_data(spark, path):
    """Read parquet data from bronze zone and convert timestamp_ntz to timestamp."""
    print("\n=== Reading Bronze Data ===")
    print(f"Path: {path}")

    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"Records Read: {record_count:,}")
        print(f"Columns: {len(df.columns)}")

        # Convert timestamp_ntz columns to regular timestamp for Glue compatibility
        # Spark 3.4+ reads timestamps as timestamp_ntz which requires Delta reader v3
        # AWS Glue crawlers don't support Delta reader v3, so we cast to timestamp
        from pyspark.sql.types import TimestampNTZType, TimestampType

        for field in df.schema.fields:
            if isinstance(field.dataType, TimestampNTZType):
                print(f"  Converting {field.name} from timestamp_ntz to timestamp")
                df = df.withColumn(field.name, col(field.name).cast(TimestampType()))

        return df
    except Exception as e:
        print(f"Error reading bronze data: {e}")
        raise


def add_time_based_fields(df):
    """
    Add time-based derived fields.

    Fields added:
    - trip_duration_minutes: Duration in minutes (dropoff - pickup)
    - trip_hour: Hour of pickup (0-23)
    - trip_day_of_week: Day of week (1=Sunday, 7=Saturday)
    - trip_date: Date of pickup
    """
    print("\n=== Adding Time-Based Fields ===")

    df = (
        df.withColumn(
            "trip_duration_minutes",
            spark_round(
                (
                    col("tpep_dropoff_datetime").cast("timestamp").cast("long")
                    - col("tpep_pickup_datetime").cast("timestamp").cast("long")
                )
                / 60.0,
                2,
            ),
        )
        .withColumn("trip_hour", hour("tpep_pickup_datetime"))
        .withColumn("trip_day_of_week", dayofweek("tpep_pickup_datetime"))
        .withColumn("trip_date", to_date("tpep_pickup_datetime"))
    )

    print("  Added: trip_duration_minutes, trip_hour, trip_day_of_week, trip_date")
    return df


def add_efficiency_metrics(df):
    """
    Add trip efficiency metrics with null handling for division by zero.

    Fields added:
    - speed_mph: Average speed (trip_distance / duration_hours)
    - fare_per_mile: Fare amount per mile
    - fare_per_minute: Fare amount per minute
    """
    print("\n=== Adding Efficiency Metrics ===")

    # Speed in MPH (distance / hours)
    # Handle null when duration is 0 or negative
    df = df.withColumn(
        "speed_mph",
        when(
            col("trip_duration_minutes") > 0,
            spark_round(
                col("trip_distance") / (col("trip_duration_minutes") / 60.0), 2
            ),
        ).otherwise(lit(None).cast(DoubleType())),
    )

    # Fare per mile
    # Handle null when distance is 0 or negative
    df = df.withColumn(
        "fare_per_mile",
        when(
            col("trip_distance") > 0,
            spark_round(col("fare_amount") / col("trip_distance"), 2),
        ).otherwise(lit(None).cast(DoubleType())),
    )

    # Fare per minute
    # Handle null when duration is 0 or negative
    df = df.withColumn(
        "fare_per_minute",
        when(
            col("trip_duration_minutes") > 0,
            spark_round(col("fare_amount") / col("trip_duration_minutes"), 2),
        ).otherwise(lit(None).cast(DoubleType())),
    )

    print("  Added: speed_mph, fare_per_mile, fare_per_minute")
    return df


def add_tipping_analysis(df):
    """
    Add tipping analysis fields with null handling.

    Fields added:
    - tip_percentage: Tip as percentage of fare amount
    """
    print("\n=== Adding Tipping Analysis ===")

    df = df.withColumn(
        "tip_percentage",
        when(
            col("fare_amount") > 0,
            spark_round(col("tip_amount") / col("fare_amount") * 100, 2),
        ).otherwise(lit(None).cast(DoubleType())),
    )

    print("  Added: tip_percentage")
    return df


def add_business_flags(df):
    """
    Add business-relevant boolean flags.

    Fields added:
    - is_airport_trip: True if RatecodeID is 2 (JFK) or 3 (Newark)
    - is_rush_hour: True if trip_hour is 7-9 AM or 5-7 PM
    - is_weekend: True if day of week is Sunday (1) or Saturday (7)
    """
    print("\n=== Adding Business Flags ===")

    # Airport trip flag (JFK=2, Newark=3)
    df = df.withColumn("is_airport_trip", col("RatecodeID").isin([2, 3]))

    # Rush hour flag (7-9 AM and 5-7 PM)
    df = df.withColumn("is_rush_hour", col("trip_hour").isin([7, 8, 9, 17, 18, 19]))

    # Weekend flag (Sunday=1, Saturday=7 in Spark's dayofweek)
    df = df.withColumn("is_weekend", col("trip_day_of_week").isin([1, 7]))

    print("  Added: is_airport_trip, is_rush_hour, is_weekend")
    return df


def decode_reference_values(df):
    """
    Decode numeric reference values to human-readable strings.

    Fields added:
    - payment_method: Decoded from payment_type
    - vendor_name: Decoded from VendorID
    - rate_type: Decoded from RatecodeID
    """
    print("\n=== Decoding Reference Values ===")

    # Decode payment_type to payment_method
    # 0=Flex Fare, 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute, 5=Unknown, 6=Voided
    df = df.withColumn(
        "payment_method",
        when(col("payment_type") == 0, "Flex Fare")
        .when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided")
        .otherwise("Unknown"),
    )

    # Decode VendorID to vendor_name
    # 1=Creative Mobile Technologies, 2=Curb Mobility, 6=Myle Technologies, 7=Helix
    df = df.withColumn(
        "vendor_name",
        when(col("VendorID") == 1, "Creative Mobile Technologies")
        .when(col("VendorID") == 2, "Curb Mobility")
        .when(col("VendorID") == 6, "Myle Technologies")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown"),
    )

    # Decode RatecodeID to rate_type
    # 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group Ride
    df = df.withColumn(
        "rate_type",
        when(col("RatecodeID") == 1, "Standard")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau/Westchester")
        .when(col("RatecodeID") == 5, "Negotiated")
        .when(col("RatecodeID") == 6, "Group Ride")
        .otherwise("Unknown"),
    )

    print("  Added: payment_method, vendor_name, rate_type")
    return df


def write_silver_data(df, path):
    """Write transformed data to silver zone as Delta format."""
    print("\n=== Writing Silver Data (Delta Format) ===")
    print(f"Path: {path}")

    try:
        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())

        # Write to silver zone using Delta format
        # Delta provides ACID transactions, time travel, and schema evolution
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)

        # Get record count for summary
        record_count = df.count()
        print(f"Records Written: {record_count:,}")
        print("Format: Delta Lake")
        return record_count
    except Exception as e:
        print(f"Error writing silver data: {e}")
        raise


def print_summary(input_count, output_count, original_columns, new_columns):
    """Print transformation summary."""
    added_columns = [c for c in new_columns if c not in original_columns]

    print("\n" + "=" * 60)
    print("=== TRIP DATA TRANSFORMATION SUMMARY ===")
    print("=" * 60)
    print(f"\nInput: {BRONZE_TRIPS_PATH}")
    print(f"Output: {SILVER_TRIPS_PATH}")
    print(f"\nRecords Read: {input_count:,}")
    print(f"Records Written: {output_count:,} (all records preserved)")
    print(f"\nColumns Added ({len(added_columns)} new fields):")

    # Group columns by category
    time_cols = ["trip_duration_minutes", "trip_hour", "trip_day_of_week", "trip_date"]
    efficiency_cols = [
        "speed_mph",
        "fare_per_mile",
        "fare_per_minute",
        "tip_percentage",
    ]
    flag_cols = ["is_airport_trip", "is_rush_hour", "is_weekend"]
    decoded_cols = ["payment_method", "vendor_name", "rate_type"]
    other_cols = ["processed_at"]

    print(f"  Time-based: {', '.join([c for c in time_cols if c in added_columns])}")
    print(
        f"  Efficiency: {', '.join([c for c in efficiency_cols if c in added_columns])}"
    )
    print(f"  Flags: {', '.join([c for c in flag_cols if c in added_columns])}")
    print(f"  Decoded: {', '.join([c for c in decoded_cols if c in added_columns])}")
    print(f"  Metadata: {', '.join([c for c in other_cols if c in added_columns])}")

    print("\nNote: No validation performed. All quality checks happen in Day 8.")
    print("=" * 60)


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("Day 7 Task 7.1a: Trip Data Transformations (Bronze → Silver)")
    print("=" * 60)

    # Initialize Spark
    spark = create_spark_session("TripDataTransformations")

    try:
        # Step 1: Read bronze data
        trips_df = read_bronze_data(spark, BRONZE_TRIPS_PATH)
        input_count = trips_df.count()
        original_columns = trips_df.columns.copy()

        # Step 2: Add time-based derived fields
        trips_df = add_time_based_fields(trips_df)

        # Step 3: Add trip efficiency metrics
        trips_df = add_efficiency_metrics(trips_df)

        # Step 4: Add tipping analysis
        trips_df = add_tipping_analysis(trips_df)

        # Step 5: Add business flags
        trips_df = add_business_flags(trips_df)

        # Step 6: Decode reference values to human-readable
        trips_df = decode_reference_values(trips_df)

        # Step 7: Write ALL trip records to silver zone (no filtering)
        output_count = write_silver_data(trips_df, SILVER_TRIPS_PATH)

        # Print summary
        print_summary(input_count, output_count, original_columns, trips_df.columns)

        print("\nTransformation completed successfully!")

    except Exception as e:
        print(f"\nError during transformation: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
