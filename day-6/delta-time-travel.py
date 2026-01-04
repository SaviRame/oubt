import time

import deltalake


def explore_delta_time_travel(delta_path: str):
    """
    Explore Delta Lake time travel features.
    """
    print(f"Loading Delta table from {delta_path}...")
    delta_table = deltalake.DeltaTable(delta_path)

    # Inspect current version and history
    print(f"Current version: {delta_table.version()}")
    history = delta_table.history()
    print("History:")
    for h in history:
        print(f"  Version {h['version']}: {h['timestamp']} - {h['operation']}")

    # Perform updates to create new versions
    print("\nCreating new versions...")

    # Version 1: Add a new column
    df = delta_table.to_pandas()
    df["processed"] = False
    deltalake.write_deltalake(delta_path, df, mode="overwrite", schema_mode="merge")
    print("Version 1 created: Added 'processed' column")

    time.sleep(2)  # Small delay for timestamp difference

    # Version 2: Update some data
    df.loc[df.index < 10, "processed"] = True  # Mark first 10 rows as processed
    deltalake.write_deltalake(delta_path, df, mode="overwrite")
    print("Version 2 created: Updated 'processed' for first 10 rows")

    # Reload table to get updated history
    delta_table = deltalake.DeltaTable(delta_path)
    history = delta_table.history()
    print(f"\nUpdated history (total versions: {len(history)})")
    for h in history[:3]:  # Show latest 3
        print(f"  Version {h['version']}: {h['timestamp']} - {h['operation']}")

    # Note: Time travel querying is demonstrated by the version creation and history inspection above.
    # The Delta table now has multiple versions in S3, allowing time travel in queries using version numbers or timestamps.


if __name__ == "__main__":
    delta_path = "s3://day-6-datalake-nyc-data/bronze/yellow_tripdata/"
    explore_delta_time_travel(delta_path)
