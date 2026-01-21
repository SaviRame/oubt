import sys

from awsglue.utils import getResolvedOptions
from glue_utils import init_glue_job


def get_glue_args(argv):
    return getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "silver_db",
            "gold_db",
            "output_path",
            "start_date",
            "end_date",
        ],
    )


def parse_args(argv):
    args = get_glue_args(argv)
    silver_table = f"{args['silver_db']}.trips_processed"
    gold_table = f"{args['gold_db']}.fact_trips"
    output_path = args["output_path"].rstrip("/")
    gold_path = f"{output_path}/fact_trips"
    start_date = args["start_date"]
    end_date = args["end_date"]
    return args, silver_table, gold_table, gold_path, start_date, end_date


def init_spark(args):
    spark, glue_context, job = init_glue_job(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark, glue_context, job


def build_fact_sql(spark, silver_table, start_date, end_date):
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW trips_processed AS
        SELECT *
        FROM {silver_table}
        """
    )

    return spark.sql(
        f"""
        SELECT
            CAST(trip_start_date AS DATE) AS trip_start_date,
            CAST(pickup_zone_gk AS BIGINT) AS pickup_zone_gk,
            CAST(dropoff_zone_gk AS BIGINT) AS dropoff_zone_gk,
            CAST(vendor_gk AS BIGINT) AS vendor_gk,
            CAST(payment_type_gk AS BIGINT) AS payment_type_gk,
            CAST(rate_code_gk AS BIGINT) AS rate_code_gk,
            CAST(COUNT(*) AS BIGINT) AS trip_count,
            CAST(SUM(total_amount) AS DOUBLE) AS total_revenue,
            CAST(SUM(fare_amount) AS DOUBLE) AS total_fare,
            CAST(SUM(tip_amount) AS DOUBLE) AS total_tips,
            CAST(SUM(trip_distance) AS DOUBLE) AS total_distance,
            CAST(SUM(trip_duration_minutes) AS DOUBLE) AS total_duration_minutes,
            CAST(AVG(trip_distance) AS DOUBLE) AS avg_distance,
            CAST(AVG(trip_duration_minutes) AS DOUBLE) AS avg_duration_minutes,
            CAST(AVG(fare_per_mile) AS DOUBLE) AS avg_fare_per_mile,
            CAST(AVG(speed_mph) AS DOUBLE) AS avg_speed_mph,
            CAST(AVG(tip_percentage) AS DOUBLE) AS avg_tip_percentage
        FROM trips_processed
        WHERE trip_start_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY
            trip_start_date,
            pickup_zone_gk,
            dropoff_zone_gk,
            vendor_gk,
            payment_type_gk,
            rate_code_gk
        """
    )


def write_gold(spark, fact_df, gold_table, gold_path):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            trip_start_date DATE,
            pickup_zone_gk LONG,
            dropoff_zone_gk LONG,
            vendor_gk LONG,
            payment_type_gk LONG,
            rate_code_gk LONG,
            trip_count BIGINT,
            total_revenue DOUBLE,
            total_fare DOUBLE,
            total_tips DOUBLE,
            total_distance DOUBLE,
            total_duration_minutes DOUBLE,
            avg_distance DOUBLE,
            avg_duration_minutes DOUBLE,
            avg_fare_per_mile DOUBLE,
            avg_speed_mph DOUBLE,
            avg_tip_percentage DOUBLE
        )
        USING DELTA
        PARTITIONED BY (trip_start_date)
        LOCATION '{gold_path}'
        """
    )

    fact_df.write.mode("overwrite").insertInto(gold_table)


def main():
    args, silver_table, gold_table, gold_path, start_date, end_date = parse_args(
        sys.argv
    )
    spark, glue_context, job = init_spark(args)

    fact_df = build_fact_sql(spark, silver_table, start_date, end_date)
    write_gold(spark, fact_df, gold_table, gold_path)

    job.commit()


if __name__ == "__main__":
    main()
