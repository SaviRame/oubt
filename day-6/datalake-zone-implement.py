import boto3


def create_data_lake_structure(bucket_name: str):
    """Create data lake zone structure in S3."""
    s3 = boto3.client("s3")

    # Create the bucket
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket: {bucket_name}")

    # Define zone structure
    zones = {
        "bronze": ["yellow_tripdata/", "taxi_zones/"],
        "silver": ["trips_cleaned/", "zones_standardized/"],
        "gold": ["daily_metrics/", "zone_analytics/"],
        "master": ["vendors/", "payment_types/"],
    }

    # Create folder markers
    for zone, folders in zones.items():
        for folder in folders:
            key = f"{zone}/{folder}"
            s3.put_object(Bucket=bucket_name, Key=key)
            print(f"Created: s3://{bucket_name}/{key}")


# Usage
create_data_lake_structure('day-6-datalake-nyc-data')
