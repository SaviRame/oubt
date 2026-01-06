"""
Day 7 Task 7.5: AWS Glue Catalog & Crawler Setup with Lineage

This script sets up AWS Glue Data Catalog infrastructure for the NYC Taxi data lake:
1. Creates a Glue database for the NYC Taxi data
2. Creates crawlers for each data zone (Bronze, Silver, Gold)
3. Adds governance and lineage metadata to all tables

Crawlers are scheduled to run once daily:
- bronze_crawler: 6 AM UTC
- silver_crawler: 7 AM UTC
- gold_crawler: 8 AM UTC

Prerequisites:
- AWS credentials configured with Glue permissions
- S3 bucket with data in bronze/silver/gold zones
- IAM role for Glue service (GlueServiceRole)
"""

import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ============================================
# CONFIGURATION
# ============================================
S3_BUCKET = "day-7-spark-glue"
DATABASE_NAME = "nyc_taxi_db"
GLUE_ROLE = "GlueServiceRole"  # IAM role for Glue
AWS_REGION = "us-east-1"  # Default AWS region

# Crawler configurations - scheduled to run once daily
# Note: For Delta Lake, we must specify individual table paths (not parent directories)
CRAWLERS = [
    {
        "name": "bronze_crawler",
        "delta_tables": [
            f"s3://{S3_BUCKET}/bronze/yellow_tripdata/",
            f"s3://{S3_BUCKET}/bronze/taxi_zones/",
        ],
        "prefix": "bronze_",
        "schedule": "cron(0 6 * * ? *)",  # Daily at 6 AM UTC
        "description": "Crawls Bronze zone Delta tables",
    },
    {
        "name": "silver_crawler",
        "delta_tables": [
            f"s3://{S3_BUCKET}/silver/trips_cleaned/",
            f"s3://{S3_BUCKET}/silver/zones_cleaned/",
            f"s3://{S3_BUCKET}/silver/trips_enriched/",
        ],
        "prefix": "silver_",
        "schedule": "cron(0 7 * * ? *)",  # Daily at 7 AM UTC
        "description": "Crawls Silver zone Delta tables",
    },
    {
        "name": "gold_crawler",
        "delta_tables": [
            f"s3://{S3_BUCKET}/gold/taxi_zones_master/",
            f"s3://{S3_BUCKET}/gold/trip_metrics/",
        ],
        "prefix": "gold_",
        "schedule": "cron(0 8 * * ? *)",  # Daily at 8 AM UTC
        "description": "Crawls Gold zone Delta tables",
    },
]

# Table metadata with governance and lineage information
TABLE_METADATA = {
    "bronze_yellow_tripdata": {
        "data_owner": "NYC TLC",
        "domain": "Transportation",
        "classification": "Internal",
        "pii_flag": "false",
        "retention_days": "365",
        "data_zone": "bronze",
        "source_system": "NYC TLC Website",
        "derived_from": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        "source_tables": "",
        "transformation_job": "",
        "downstream_tables": "silver_trips_cleaned",
        "lineage_updated": "",
    },
    "bronze_taxi_zones": {
        "data_owner": "NYC TLC",
        "domain": "Transportation",
        "classification": "Reference Data",
        "pii_flag": "false",
        "retention_days": "indefinite",
        "data_zone": "bronze",
        "source_system": "NYC TLC Website",
        "derived_from": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        "source_tables": "",
        "transformation_job": "",
        "downstream_tables": "silver_zones_cleaned,gold_taxi_zones_master",
        "lineage_updated": "",
    },
    "silver_trips_cleaned": {
        "data_owner": "Data Engineering",
        "domain": "Transportation",
        "classification": "Internal",
        "pii_flag": "false",
        "retention_days": "365",
        "data_zone": "silver",
        "derived_from": "",
        "source_tables": "bronze_yellow_tripdata",
        "transformation_job": "pyspark-trip-transformations",
        "downstream_tables": "silver_trips_enriched,gold_trip_metrics",
        "lineage_updated": "",
    },
    "silver_zones_cleaned": {
        "data_owner": "Data Engineering",
        "domain": "Transportation",
        "classification": "Reference Data",
        "pii_flag": "false",
        "retention_days": "indefinite",
        "data_zone": "silver",
        "derived_from": "",
        "source_tables": "bronze_taxi_zones",
        "transformation_job": "pyspark-taxi-transformations",
        "downstream_tables": "gold_taxi_zones_master",
        "lineage_updated": "",
    },
    "silver_trips_enriched": {
        "data_owner": "Data Engineering",
        "domain": "Transportation",
        "classification": "Internal",
        "pii_flag": "false",
        "retention_days": "365",
        "data_zone": "silver",
        "derived_from": "",
        "source_tables": "silver_trips_cleaned,gold_taxi_zones_master",
        "transformation_job": "master-data-enrichment",
        "downstream_tables": "",
        "lineage_updated": "",
    },
    "gold_taxi_zones_master": {
        "data_owner": "MDM Team",
        "domain": "Transportation",
        "classification": "Master Data",
        "pii_flag": "false",
        "retention_days": "indefinite",
        "data_zone": "gold",
        "is_golden_record": "true",
        "data_steward": "MDM Team",
        "derived_from": "",
        "source_tables": "silver_zones_cleaned",
        "transformation_job": "pyspark-taxi-transformations",
        "downstream_tables": "silver_trips_enriched",
        "lineage_updated": "",
    },
    "gold_trip_metrics": {
        "data_owner": "Analytics Team",
        "domain": "Transportation",
        "classification": "Analytics",
        "pii_flag": "false",
        "retention_days": "365",
        "data_zone": "gold",
        "is_fact_table": "true",
        "partition_key": "trip_date",
        "derived_from": "",
        "source_tables": "silver_trips_cleaned",
        "transformation_job": "pyspark-aggregations",
        "downstream_tables": "",
        "lineage_updated": "",
    },
}


# ============================================
# GLUE CLIENT INITIALIZATION
# ============================================
def create_glue_client():
    """Create and return a boto3 Glue client."""
    return boto3.client("glue", region_name=AWS_REGION)


def create_iam_client():
    """Create and return a boto3 IAM client."""
    return boto3.client("iam", region_name=AWS_REGION)


# ============================================
# IAM ROLE CREATION
# ============================================
def create_glue_service_role(iam_client):
    """Create the IAM role required for AWS Glue crawlers."""
    print("\n" + "=" * 60)
    print("Creating IAM Role for Glue Service")
    print("=" * 60)

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        try:
            iam_client.get_role(RoleName=GLUE_ROLE)
            print(f"  IAM Role '{GLUE_ROLE}' already exists")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise

        print(f"  Creating IAM role: {GLUE_ROLE}")
        iam_client.create_role(
            RoleName=GLUE_ROLE,
            AssumeRolePolicyDocument=str(trust_policy).replace("'", '"'),
            Description="IAM role for AWS Glue crawlers",
            Tags=[
                {"Key": "Project", "Value": "MDM Training"},
                {"Key": "CreatedBy", "Value": "glue-catalog-crawler-setup.py"},
            ],
        )

        policies_to_attach = [
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
            "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        ]

        for policy_arn in policies_to_attach:
            iam_client.attach_role_policy(RoleName=GLUE_ROLE, PolicyArn=policy_arn)

        print(f"  IAM Role '{GLUE_ROLE}' created successfully!")
        print("  Waiting 10 seconds for IAM role propagation...")
        time.sleep(10)
        return True

    except ClientError as e:
        print(f"  Error creating IAM role: {e}")
        return False


# ============================================
# STEP 1: CREATE DATABASE
# ============================================
def create_database(glue_client):
    """Create the Glue database for NYC Taxi data."""
    print("\n" + "=" * 60)
    print("Step 1: Database Creation")
    print("=" * 60)

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": "NYC Taxi Trip Data for MDM Training",
                "LocationUri": f"s3://{S3_BUCKET}/",
                "Parameters": {
                    "created_by": "glue-catalog-crawler-setup.py",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            }
        )
        print(f"  Database: {DATABASE_NAME}")
        print("  Status: Created")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"  Database: {DATABASE_NAME}")
            print("  Status: Already exists")
            return True
        else:
            print(f"  Error creating database: {e}")
            raise


# ============================================
# STEP 2: CREATE CRAWLERS (with Delta Lake support)
# ============================================
def create_crawlers(glue_client):
    """Create Glue crawlers for each data zone with Delta Lake support."""
    print("\n" + "=" * 60)
    print("Step 2: Crawler Creation (Delta Lake)")
    print("=" * 60)

    created_crawlers = []

    # Delta Lake crawler configuration
    delta_config = {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
            "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"},
        },
        "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
    }

    for crawler in CRAWLERS:
        try:
            glue_client.create_crawler(
                Name=crawler["name"],
                Role=GLUE_ROLE,
                DatabaseName=DATABASE_NAME,
                Description=crawler["description"],
                Targets={
                    # Use DeltaTargets with specific table paths for Delta Lake
                    "DeltaTargets": [
                        {
                            "DeltaTables": crawler["delta_tables"],
                            "WriteManifest": False,
                            "CreateNativeDeltaTable": True,
                        }
                    ]
                },
                TablePrefix=crawler["prefix"],
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
                Schedule=crawler["schedule"],
                Configuration=str(delta_config).replace("'", '"'),
            )
            print(f"  - {crawler['name']}: {len(crawler['delta_tables'])} Delta tables")
            print(f"    Schedule: {crawler['schedule']} (once daily)")
            created_crawlers.append(crawler["name"])

        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                print(f"  - {crawler['name']}: Already exists")
                created_crawlers.append(crawler["name"])
            else:
                print(f"  Error creating crawler {crawler['name']}: {e}")
                raise

    print(f"\n  Status: {len(created_crawlers)} crawlers ready")
    print("  Note: Crawlers will run on their scheduled times (once daily)")
    return created_crawlers


# ============================================
# STEP 3: ADD GOVERNANCE + LINEAGE METADATA
# ============================================
def add_governance_lineage_metadata(glue_client):
    """Add governance and lineage metadata to all tables."""
    print("\n" + "=" * 60)
    print("Step 3: Governance + Lineage Metadata")
    print("=" * 60)

    updated_tables = []
    not_found_tables = []

    for table_name, properties in TABLE_METADATA.items():
        try:
            response = glue_client.get_table(
                DatabaseName=DATABASE_NAME, Name=table_name
            )
            table = response["Table"]

            properties["lineage_updated"] = datetime.now(timezone.utc).isoformat()

            current_params = table.get("Parameters", {})
            current_params.update(properties)

            table_input = {
                "Name": table_name,
                "Parameters": current_params,
                "StorageDescriptor": table["StorageDescriptor"],
            }

            if "TableType" in table:
                table_input["TableType"] = table["TableType"]
            if "PartitionKeys" in table:
                table_input["PartitionKeys"] = table["PartitionKeys"]

            glue_client.update_table(DatabaseName=DATABASE_NAME, TableInput=table_input)

            print(f"  Updated: {table_name}")
            updated_tables.append(table_name)

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                print(f"  Not found: {table_name} (run crawler first)")
                not_found_tables.append(table_name)
            else:
                print(f"  Error updating {table_name}: {e}")

    print(f"\n  Tables updated: {len(updated_tables)}")
    print(f"  Tables not found: {len(not_found_tables)}")

    return updated_tables, not_found_tables


# ============================================
# STEP 4: VERIFY CRAWLER SCHEDULES
# ============================================
def verify_crawler_schedules(glue_client):
    """Verify that crawlers are scheduled correctly."""
    print("\n" + "=" * 60)
    print("Step 4: Crawler Schedules")
    print("=" * 60)

    for crawler in CRAWLERS:
        try:
            response = glue_client.get_crawler(Name=crawler["name"])
            schedule = (
                response["Crawler"]
                .get("Schedule", {})
                .get("ScheduleExpression", "Not scheduled")
            )
            state = response["Crawler"]["State"]

            # Parse cron for human-readable format
            if "cron(0 6" in schedule:
                schedule_desc = "Daily at 6 AM UTC"
            elif "cron(0 7" in schedule:
                schedule_desc = "Daily at 7 AM UTC"
            elif "cron(0 8" in schedule:
                schedule_desc = "Daily at 8 AM UTC"
            else:
                schedule_desc = schedule

            print(f"  - {crawler['name']}: {schedule_desc} (State: {state})")

        except ClientError as e:
            print(f"  - {crawler['name']}: Error - {e}")


# ============================================
# MAIN EXECUTION
# ============================================
def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("Day 7 Task 7.5: AWS Glue Catalog & Crawler Setup")
    print("=" * 60)

    glue_client = create_glue_client()
    iam_client = create_iam_client()

    try:
        # Step 0: Create IAM role
        if not create_glue_service_role(iam_client):
            print("\n  Cannot proceed without IAM role. Exiting.")
            sys.exit(1)

        # Step 1: Create database
        create_database(glue_client)

        # Step 2: Create crawlers (with Delta Lake support, scheduled daily)
        crawler_names = create_crawlers(glue_client)

        # Step 3: Add metadata to existing tables (if any)
        if crawler_names:
            add_governance_lineage_metadata(glue_client)

        # Step 4: Verify schedules
        if crawler_names:
            verify_crawler_schedules(glue_client)

        # Summary
        print("\n" + "=" * 60)
        print("=== SETUP COMPLETE ===")
        print("=" * 60)
        print("\nCrawlers are scheduled to run once daily:")
        print("  - bronze_crawler: 6 AM UTC")
        print("  - silver_crawler: 7 AM UTC")
        print("  - gold_crawler: 8 AM UTC")
        print("\nTo run a crawler manually:")
        print("  aws glue start-crawler --name bronze_crawler")

    except Exception as e:
        print(f"\nError during setup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
