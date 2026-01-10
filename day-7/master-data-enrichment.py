"""
Day 7 Task 7.2: Master Data Enrichment (Trip Data with Zone Information)

This script enriches trip data with zone information using broadcast joins.
It joins the cleaned trip data from silver zone with the golden zone master
records from gold zone to add pickup and dropoff zone details.

Enrichment Strategy:
1. Load cleaned trips from silver zone (Task 7.1a output) - Delta format
2. Load golden zone records from gold zone (Task 7.1b output) - Delta format
3. Use broadcast joins for the small zones table (~265 rows)
4. LEFT JOIN to preserve all trips (unmatched zones get NULL values)

Important: Day 7 focuses on TRANSFORMATIONS ONLY - no validation flags.
Unmatched zones will have NULL values. Day 8 will analyze these for
referential integrity reporting.

Input: s3a://day-7-spark-glue/silver/trips_cleaned/ (Delta)
Input: s3a://day-7-spark-glue/gold/taxi_zones_master/ (Delta)
Output: s3a://day-7-spark-glue/silver/trips_enriched/ (Delta)
"""

import sys

from pyspark.sql.functions import (
    broadcast,
    col,
    current_timestamp,
)
from spark_session import create_spark_session

# ============================================
# CONFIGURATION
# ============================================
S3_BUCKET = "day-7-spark-glue"
SILVER_TRIPS_PATH = f"s3a://{S3_BUCKET}/silver/trips_cleaned/"
GOLD_ZONES_MASTER_PATH = f"s3a://{S3_BUCKET}/gold/taxi_zones_master/"
SILVER_ENRICHED_PATH = f"s3a://{S3_BUCKET}/silver/trips_enriched/"


def read_silver_trips(spark, path):
    """
    Read cleaned trip data from silver zone (Delta format).

    This is the output from Task 7.1a (trip transformations).
    """
    print("\n=== Reading Silver Trip Data (Delta) ===")
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


def read_gold_zones_master(spark, path):
    """
    Read golden zone master records from gold zone (Delta format).

    This is the output from Task 7.1b (zone transformations).
    Only current records (is_current = True) are used for enrichment.
    """
    print("\n=== Reading Gold Zone Master Data (Delta) ===")
    print(f"Path: {path}")

    try:
        df = spark.read.format("delta").load(path)
        total_count = df.count()
        print(f"Total Records: {total_count:,}")

        # Filter to current records only
        df_current = df.filter(col("is_current") == True)
        current_count = df_current.count()
        print(f"Current Records (is_current=True): {current_count:,}")

        return df_current, current_count
    except Exception as e:
        print(f"Error reading gold zone master data: {e}")
        raise


def enrich_with_pickup_zone(trips_df, zones_df):
    """
    Enrich trips with pickup zone information using broadcast join.

    Fields added:
    - pickup_zone_name: Zone name for pickup location
    - pickup_borough: Borough for pickup location
    - pickup_service_zone: Service zone for pickup location
    - pickup_zone_type: Zone type classification for pickup
    - pickup_is_airport: Boolean flag if pickup is at airport

    Uses LEFT JOIN to preserve all trips. Unmatched zones get NULL values.
    """
    print("\n=== Enriching with Pickup Zone Info ===")

    # Prepare pickup zone columns with aliases
    pickup_zones = zones_df.select(
        col("LocationID").alias("pu_loc_id"),
        col("Zone").alias("pickup_zone_name"),
        col("Borough").alias("pickup_borough"),
        col("service_zone").alias("pickup_service_zone"),
        col("zone_type").alias("pickup_zone_type"),
        col("is_airport").alias("pickup_is_airport"),
    )

    # Broadcast join (zones table is small ~265 rows)
    enriched_df = trips_df.join(
        broadcast(pickup_zones),
        trips_df.PULocationID == col("pu_loc_id"),
        "left",
    ).drop("pu_loc_id")

    print("  Added: pickup_zone_name, pickup_borough, pickup_service_zone,")
    print("         pickup_zone_type, pickup_is_airport")

    return enriched_df


def enrich_with_dropoff_zone(trips_df, zones_df):
    """
    Enrich trips with dropoff zone information using broadcast join.

    Fields added:
    - dropoff_zone_name: Zone name for dropoff location
    - dropoff_borough: Borough for dropoff location
    - dropoff_service_zone: Service zone for dropoff location
    - dropoff_zone_type: Zone type classification for dropoff
    - dropoff_is_airport: Boolean flag if dropoff is at airport

    Uses LEFT JOIN to preserve all trips. Unmatched zones get NULL values.
    """
    print("\n=== Enriching with Dropoff Zone Info ===")

    # Prepare dropoff zone columns with aliases
    dropoff_zones = zones_df.select(
        col("LocationID").alias("do_loc_id"),
        col("Zone").alias("dropoff_zone_name"),
        col("Borough").alias("dropoff_borough"),
        col("service_zone").alias("dropoff_service_zone"),
        col("zone_type").alias("dropoff_zone_type"),
        col("is_airport").alias("dropoff_is_airport"),
    )

    # Broadcast join (zones table is small ~265 rows)
    enriched_df = trips_df.join(
        broadcast(dropoff_zones),
        trips_df.DOLocationID == col("do_loc_id"),
        "left",
    ).drop("do_loc_id")

    print("  Added: dropoff_zone_name, dropoff_borough, dropoff_service_zone,")
    print("         dropoff_zone_type, dropoff_is_airport")

    return enriched_df


def write_enriched_data(df, path):
    """Write enriched trip data to silver zone as Delta format."""
    print("\n=== Writing Enriched Trip Data (Delta) ===")
    print(f"Path: {path}")

    try:
        # Add enrichment timestamp
        df = df.withColumn("enriched_at", current_timestamp())

        # Write to silver zone in Delta format
        # Use overwriteSchema to handle schema changes (e.g., timestamp_ntz to timestamp)
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)

        # Get record count for summary
        record_count = df.count()
        print(f"Records Written: {record_count:,}")
        return record_count
    except Exception as e:
        print(f"Error writing enriched data: {e}")
        raise


def print_summary(trips_count, zones_count, output_count):
    """Print enrichment transformation summary."""
    print("\n" + "=" * 60)
    print("=== ENRICHMENT TRANSFORMATION SUMMARY ===")
    print("=" * 60)
    print(f"\nInput: {SILVER_TRIPS_PATH}")
    print(f"Master: {GOLD_ZONES_MASTER_PATH}")
    print(f"Output: {SILVER_ENRICHED_PATH}")

    print(f"\nTrips Read: {trips_count:,} (all records)")
    print(f"Zone Master Records: {zones_count:,} (current golden records)")

    print("\nColumns Added:")
    print("  Pickup Zone Info:")
    print("    - pickup_zone_name")
    print("    - pickup_borough")
    print("    - pickup_service_zone")
    print("    - pickup_zone_type")
    print("    - pickup_is_airport")
    print("")
    print("  Dropoff Zone Info:")
    print("    - dropoff_zone_name")
    print("    - dropoff_borough")
    print("    - dropoff_service_zone")
    print("    - dropoff_zone_type")
    print("    - dropoff_is_airport")
    print("")
    print("  Metadata:")
    print("    - enriched_at")

    print(f"\nRecords Written: {output_count:,} (all records preserved)")

    print("\nNote: No validation performed. Unmatched zones have NULL values.")
    print("      Day 8 will analyze NULLs for referential integrity reporting.")
    print("=" * 60)


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("Day 7 Task 7.2: Master Data Enrichment")
    print("(Trip Data with Zone Information)")
    print("=" * 60)

    # Initialize Spark session
    spark = create_spark_session("MasterDataEnrichment")

    try:
        # Step 1: Read cleaned trips from silver zone (Task 7.1a output)
        trips_df, trips_count = read_silver_trips(spark, SILVER_TRIPS_PATH)

        # Step 2: Read golden zone records from gold zone (Task 7.1b output)
        zones_df, zones_count = read_gold_zones_master(spark, GOLD_ZONES_MASTER_PATH)

        # Step 3: Enrich trips with pickup zone info (broadcast join)
        trips_enriched = enrich_with_pickup_zone(trips_df, zones_df)

        # Step 4: Enrich trips with dropoff zone info (broadcast join)
        trips_enriched = enrich_with_dropoff_zone(trips_enriched, zones_df)

        # Step 5: Write ALL enriched records to silver zone
        # Note: LEFT JOIN preserves all trips, unmatched zones have NULL values
        output_count = write_enriched_data(trips_enriched, SILVER_ENRICHED_PATH)

        # Print summary
        print_summary(trips_count, zones_count, output_count)

        print("\nEnrichment completed successfully!")

    except Exception as e:
        print(f"\nError during enrichment: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
