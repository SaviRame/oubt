"""
Day 7 Task 7.1b: Zone Data Transformations (Bronze → Silver → Gold/Master)

This script transforms NYC Taxi Zone reference data through the data lake zones:
- Bronze → Silver: Read CSV, add derived fields (zone_type, is_airport), write Delta
- Silver → Gold: Add MDM governance fields for golden records, write Delta

Transformation Categories:
1. Silver Layer: Zone type classification and airport flag
2. Gold Layer: MDM governance fields (golden_record_id, versioning, stewardship)

Important: Day 7 focuses on TRANSFORMATIONS ONLY - no validation or quality checks.
All quality checks happen in Day 8.

Input: s3a://day-7-spark-glue/bronze/taxi_zones/ (CSV format)
Silver Output: s3a://day-7-spark-glue/silver/zones_cleaned/ (Delta format)
Gold Output: s3a://day-7-spark-glue/gold/taxi_zones_master/ (Delta format)
"""

import sys

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    monotonically_increasing_id,
    when,
)
from spark_session import create_spark_session

# ============================================
# CONFIGURATION
# ============================================
S3_BUCKET = "day-7-spark-glue"
BRONZE_ZONES_PATH = f"s3a://{S3_BUCKET}/bronze/taxi_zones/"
SILVER_ZONES_PATH = f"s3a://{S3_BUCKET}/silver/zones_cleaned/"
GOLD_ZONES_MASTER_PATH = f"s3a://{S3_BUCKET}/gold/taxi_zones_master/"


def read_bronze_csv_data(spark, path):
    """
    Read zone data from bronze zone as CSV.

    Reads CSV format from the bronze zone with header and schema inference.
    """
    print("\n=== Reading Bronze Zone Data (CSV) ===")
    print(f"Path: {path}")

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        record_count = df.count()
        print("Format: CSV")
        print(f"Records Read: {record_count:,}")
        print(f"Columns: {len(df.columns)}")
        print(
            f"Schema: {', '.join([f'{c.name}:{c.dataType.simpleString()}' for c in df.schema.fields])}"
        )
        return df
    except Exception as csv_error:
        print(f"CSV read failed: {csv_error}")
        raise RuntimeError(f"Could not read CSV data from {path}")


def add_zone_type_classification(df):
    """
    Add zone type classification based on service_zone.

    Fields added:
    - zone_type: Derived classification from service_zone
        - "Yellow Zone" → "Core Manhattan"
        - "Boro Zone" → "Outer Borough"
        - "Airports" → "Airport"
        - "EWR" → "Newark Airport"
        - Otherwise → "Other"
    """
    print("\n=== Adding Zone Type Classification ===")

    df = df.withColumn(
        "zone_type",
        when(col("service_zone") == "Yellow Zone", "Core Manhattan")
        .when(col("service_zone") == "Boro Zone", "Outer Borough")
        .when(col("service_zone") == "Airports", "Airport")
        .when(col("service_zone") == "EWR", "Newark Airport")
        .otherwise("Other"),
    )

    print("  Added: zone_type (derived from service_zone)")
    return df


def add_airport_flag(df):
    """
    Add boolean flag indicating if zone is an airport.

    Fields added:
    - is_airport: True if service_zone is "Airports" or "EWR"
    """
    print("\n=== Adding Airport Flag ===")

    df = df.withColumn(
        "is_airport",
        col("service_zone").isin(["Airports", "EWR"]),
    )

    print("  Added: is_airport (True for Airports and EWR)")
    return df


def add_processing_timestamp(df):
    """
    Add processing timestamp for silver layer.

    Fields added:
    - processed_at: Current timestamp when transformation ran
    """
    print("\n=== Adding Processing Timestamp ===")

    df = df.withColumn("processed_at", current_timestamp())

    print("  Added: processed_at")
    return df


def add_mdm_governance_fields(df):
    """
    Add Master Data Management (MDM) governance fields for golden records.

    Fields added:
    - golden_record_id: Unique identifier for the golden record
    - effective_from: Timestamp when this version became effective
    - effective_to: Timestamp when this version expires (9999-12-31 for current)
    - is_current: Boolean flag indicating if this is the current version
    - version: Version number of the record
    - source_system: Origin system of the data
    - data_steward: Team responsible for data quality
    """
    print("\n=== Adding MDM Governance Fields ===")

    df = (
        df.withColumn("golden_record_id", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
        .withColumn("version", lit(1))
        .withColumn("source_system", lit("NYC TLC"))
        .withColumn("data_steward", lit("MDM Team"))
    )

    print("  Added: golden_record_id, effective_from, effective_to,")
    print("         is_current, version, source_system, data_steward")
    return df


def write_silver_delta(df, path):
    """Write transformed data to silver zone as Delta format."""
    print("\n=== Writing Silver Zone Data (Delta) ===")
    print(f"Path: {path}")

    try:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
        record_count = df.count()
        print("Format: Delta")
        print(f"Records Written: {record_count:,}")
        print(f"Columns: {len(df.columns)}")
        return record_count
    except Exception as e:
        print(f"Error writing silver Delta data: {e}")
        raise


def write_gold_delta(df, path):
    """Write master data to gold zone as Delta format."""
    print("\n=== Writing Gold Zone Data (Delta) ===")
    print(f"Path: {path}")

    try:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
        record_count = df.count()
        print("Format: Delta")
        print(f"Records Written: {record_count:,}")
        print(f"Columns: {len(df.columns)}")
        return record_count
    except Exception as e:
        print(f"Error writing gold Delta data: {e}")
        raise


def print_summary(record_count, silver_columns, gold_columns, original_columns):
    """Print transformation summary."""
    silver_added = [c for c in silver_columns if c not in original_columns]
    gold_added = [c for c in gold_columns if c not in silver_columns]

    print("\n" + "=" * 60)
    print("=== ZONE DATA TRANSFORMATION SUMMARY ===")
    print("=" * 60)
    print(f"\nInput: {BRONZE_ZONES_PATH}")
    print(f"Silver Output: {SILVER_ZONES_PATH}")
    print(f"Gold Output: {GOLD_ZONES_MASTER_PATH}")
    print(f"\nRecords: {record_count:,} zones")

    print(f"\nSilver Columns Added ({len(silver_added)} fields):")
    print(f"  {', '.join(silver_added)}")

    print(f"\nGold Columns Added ({len(gold_added)} fields):")
    # Format gold columns nicely across multiple lines
    gold_line1 = ["golden_record_id", "effective_from", "effective_to"]
    gold_line2 = ["is_current", "version", "source_system", "data_steward"]
    print(f"  {', '.join([c for c in gold_line1 if c in gold_added])},")
    print(f"  {', '.join([c for c in gold_line2 if c in gold_added])}")

    print("\nNote: No validation performed. All quality checks happen in Day 8.")
    print("=" * 60)


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("Day 7 Task 7.1b: Zone Data Transformations")
    print("(Bronze CSV → Silver Delta → Gold Delta)")
    print("=" * 60)

    # Initialize Spark with Delta Lake support
    spark = create_spark_session("ZoneDataTransformations")

    try:
        # Step 1: Read bronze zone data (CSV format)
        zones_df = read_bronze_csv_data(spark, BRONZE_ZONES_PATH)
        original_columns = zones_df.columns.copy()
        record_count = zones_df.count()

        # ============================================
        # SILVER LAYER TRANSFORMATIONS
        # ============================================
        print("\n" + "-" * 40)
        print("SILVER LAYER TRANSFORMATIONS")
        print("-" * 40)

        # Step 2: Add zone type classification
        zones_df = add_zone_type_classification(zones_df)

        # Step 3: Add airport flag
        zones_df = add_airport_flag(zones_df)

        # Step 4: Add processing timestamp
        zones_df = add_processing_timestamp(zones_df)

        # Step 5: Write to silver zone (Delta format)
        silver_columns = zones_df.columns.copy()
        write_silver_delta(zones_df, SILVER_ZONES_PATH)

        # ============================================
        # GOLD LAYER TRANSFORMATIONS (Master Data)
        # ============================================
        print("\n" + "-" * 40)
        print("GOLD LAYER TRANSFORMATIONS (Master Data)")
        print("-" * 40)

        # Step 6: Add MDM governance fields for golden records
        zones_master_df = add_mdm_governance_fields(zones_df)

        # Step 7: Write to gold zone (Delta format)
        gold_columns = zones_master_df.columns.copy()
        write_gold_delta(zones_master_df, GOLD_ZONES_MASTER_PATH)

        # Print summary
        print_summary(record_count, silver_columns, gold_columns, original_columns)

        print("\nTransformation completed successfully!")
        print("Output format: Delta Lake (supports ACID transactions, time travel)")

    except Exception as e:
        print(f"\nError during transformation: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
