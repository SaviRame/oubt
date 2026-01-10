import os
import deltalake
import pandas as pd


def convert_parquet_to_delta(
    parquet_path: str, bucket_name: str, delta_table_name: str
):
    """
    Convert Parquet data to Delta Lake format and store in S3.
    """
    # Read Parquet data into Pandas DataFrame
    print(f"Reading Parquet data from {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    print(f"Data loaded with shape: {df.shape}")

    # Define S3 path for Delta table
    delta_path = f"s3://{bucket_name}/bronze/{delta_table_name}/"
    print(f"Target Delta table path: {delta_path}")

    # Write DataFrame as Delta table to S3
    print("Writing DataFrame as Delta table...")
    deltalake.write_deltalake(delta_path, df)
    print(f"Delta table created at {delta_path}")

    # Verify Delta table creation
    print("Verifying Delta table...")
    delta_table = deltalake.DeltaTable(delta_path)
    print(f"Schema: {delta_table.schema()}")
    print(f"Version: {delta_table.version()}")

    # Test reading the Delta table
    print("Testing read of Delta table...")
    df_read = delta_table.to_pandas()
    print(f"Read back data with shape: {df_read.shape}")
    print(
        "Data integrity check: shapes match"
        if df.shape == df_read.shape
        else "Data integrity issue: shapes do not match"
    )


if __name__ == "__main__":
    # Configuration
    parquet_path = "data/yellow_tripdata_2025-08.parquet"
    bucket_name = "day-6-datalake-nyc-data"
    delta_table_name = "yellow_tripdata"

    # Ensure AWS credentials are set (via environment or config)
    if not os.getenv("AWS_ACCESS_KEY_ID"):
        print(
            "Warning: AWS_ACCESS_KEY_ID not set. Ensure AWS credentials are configured."
        )

    convert_parquet_to_delta(parquet_path, bucket_name, delta_table_name)
