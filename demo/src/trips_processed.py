import sys

from awsglue.utils import getResolvedOptions
from glue_utils import init_glue_job
from pyspark.sql import functions as F


def get_glue_args(argv):
    return getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "bronze_db",
            "master_db",
            "silver_db",
            "ingestion_date",
            "output_path",
        ],
    )


def parse_args(argv):
    args = get_glue_args(argv)
    bronze_table = f"{args['bronze_db']}.trips"
    zone_table = f"{args['master_db']}.zone_master"
    payment_table = f"{args['master_db']}.payment_type_ref"
    rate_code_table = f"{args['master_db']}.rate_code_ref"
    vendor_xf_table = f"{args['master_db']}.xref_vendor"
    silver_table = f"{args['silver_db']}.trips_processed"
    ingestion_date = args["ingestion_date"]
    output_path = args["output_path"]
    silver_path = f"{output_path}/trips_processed"
    return (
        args,
        bronze_table,
        zone_table,
        payment_table,
        rate_code_table,
        vendor_xf_table,
        silver_table,
        ingestion_date,
        silver_path,
    )


def init_spark(args):
    spark, glue_context, job = init_glue_job(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark, glue_context, job


def read_bronze(glue_context, bronze_table, ingestion_date):
    """Read from Glue Data Catalog, filtering by ingestion_date."""
    if "." not in bronze_table:
        raise ValueError(f"Expected database.table format, got: {bronze_table}")
    bronze_db, bronze_name = bronze_table.split(".", 1)
    df = glue_context.create_dynamic_frame.from_catalog(
        database=bronze_db, table_name=bronze_name
    ).toDF()
    return df.filter(F.col("ingestion_date") == F.lit(ingestion_date))


def build_trips_sql(
    spark,
    trips_df,
    zone_table,
    payment_table,
    rate_code_table,
    vendor_xf_table,
):
    trips_df.createOrReplaceTempView("trips_src")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW zone_current AS
        SELECT location_id, zone_gk
        FROM {zone_table}
        WHERE is_current
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW payment_current AS
        SELECT payment_type, payment_type_gk
        FROM {payment_table}
        WHERE is_current
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW rate_code_current AS
        SELECT ratecode_id, rate_code_gk
        FROM {rate_code_table}
        WHERE is_current
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW vendor_xf_current AS
        SELECT vendor_id, vendor_gk
        FROM {vendor_xf_table}
        WHERE is_current
        """
    )

    return spark.sql(
        """
        WITH validated AS (
            SELECT *
            FROM trips_src
            WHERE tpep_pickup_datetime IS NOT NULL
                AND tpep_dropoff_datetime IS NOT NULL
                AND tpep_dropoff_datetime >= tpep_pickup_datetime
                AND pulocationid IS NOT NULL
                AND dolocationid IS NOT NULL
                AND trip_distance >= 0
                AND total_amount >= 0
        ),
        derived AS (
            SELECT
                v.*,
                CAST(v.tpep_pickup_datetime AS DATE) AS trip_start_date,
                ROUND(
                    (
                        UNIX_TIMESTAMP(v.tpep_dropoff_datetime)
                        - UNIX_TIMESTAMP(v.tpep_pickup_datetime)
                    ) / 60.0,
                    2
                ) AS trip_duration_minutes,
                DAYOFWEEK(v.tpep_pickup_datetime) AS trip_day_of_week,
                CASE
                    WHEN v.trip_distance > 0
                        THEN ROUND(v.fare_amount / v.trip_distance, 2)
                    ELSE CAST(NULL AS DOUBLE)
                END AS fare_per_mile,
                CASE
                    WHEN (
                        UNIX_TIMESTAMP(v.tpep_dropoff_datetime)
                        - UNIX_TIMESTAMP(v.tpep_pickup_datetime)
                    ) > 0
                        THEN ROUND(
                            v.trip_distance
                            / (
                                (
                                    UNIX_TIMESTAMP(v.tpep_dropoff_datetime)
                                    - UNIX_TIMESTAMP(v.tpep_pickup_datetime)
                                ) / 3600.0
                            ),
                            2
                        )
                    ELSE CAST(NULL AS DOUBLE)
                END AS speed_mph,
                CASE
                    WHEN v.fare_amount > 0
                        THEN ROUND(v.tip_amount / v.fare_amount * 100, 2)
                    ELSE CAST(NULL AS DOUBLE)
                END AS tip_percentage
            FROM validated v
        )
        SELECT
            d.*,
            pu.zone_gk AS pickup_zone_gk,
            do.zone_gk AS dropoff_zone_gk,
            vx.vendor_gk AS vendor_gk,
            pay.payment_type_gk AS payment_type_gk,
            rc.rate_code_gk AS rate_code_gk
        FROM derived d
        LEFT JOIN zone_current pu ON d.pulocationid = pu.location_id
        LEFT JOIN zone_current do ON d.dolocationid = do.location_id
        LEFT JOIN vendor_xf_current vx ON d.vendorid = vx.vendor_id
        LEFT JOIN payment_current pay ON d.payment_type = pay.payment_type
        LEFT JOIN rate_code_current rc ON d.ratecodeid = rc.ratecode_id
        """
    )


def write_silver(trips_df, silver_table, silver_path):
    (
        trips_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("trip_start_date")
        .option("path", silver_path)
        .saveAsTable(silver_table)
    )


def main():
    (
        args,
        bronze_table,
        zone_table,
        payment_table,
        rate_code_table,
        vendor_xf_table,
        silver_table,
        ingestion_date,
        silver_path,
    ) = parse_args(sys.argv)
    spark, glue_context, job = init_spark(args)

    trips_df = read_bronze(glue_context, bronze_table, ingestion_date)
    if trips_df.limit(1).count() == 0:
        job.commit()
        return

    enriched_df = build_trips_sql(
        spark,
        trips_df,
        zone_table,
        payment_table,
        rate_code_table,
        vendor_xf_table,
    )
    write_silver(enriched_df, silver_table, silver_path)

    job.commit()


if __name__ == "__main__":
    main()
