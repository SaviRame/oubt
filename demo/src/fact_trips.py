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
    output_path = args["output_path"]
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
            trip_start_date,
            pickup_zone_gk,
            dropoff_zone_gk,
            vendor_gk,
            payment_type_gk,
            rate_code_gk,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            SUM(fare_amount) AS total_fare,
            SUM(tip_amount) AS total_tips,
            SUM(trip_distance) AS total_distance,
            SUM(trip_duration_minutes) AS total_duration_minutes,
            AVG(trip_distance) AS avg_distance,
            AVG(trip_duration_minutes) AS avg_duration_minutes,
            AVG(fare_per_mile) AS avg_fare_per_mile,
            AVG(speed_mph) AS avg_speed_mph,
            AVG(tip_percentage) AS avg_tip_percentage
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


def write_gold(fact_df, gold_table, gold_path):
    (
        fact_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("trip_start_date")
        .option("path", gold_path)
        .saveAsTable(gold_table)
    )


def main():
    args, silver_table, gold_table, gold_path, start_date, end_date = parse_args(
        sys.argv
    )
    spark, glue_context, job = init_spark(args)

    fact_df = build_fact_sql(spark, silver_table, start_date, end_date)
    write_gold(fact_df, gold_table, gold_path)

    job.commit()


if __name__ == "__main__":
    main()
