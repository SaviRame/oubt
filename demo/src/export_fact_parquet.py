import sys

from awsglue.utils import getResolvedOptions
from glue_utils import init_glue_job
from pyspark.sql import functions as F


def get_glue_args(argv):
    args = getResolvedOptions(argv, ["JOB_NAME", "gold_db", "output_path"])
    if "--start_date" in argv:
        args["start_date"] = getResolvedOptions(argv, ["start_date"])["start_date"]
    else:
        args["start_date"] = None
    if "--end_date" in argv:
        args["end_date"] = getResolvedOptions(argv, ["end_date"])["end_date"]
    else:
        args["end_date"] = None
    return args


def parse_args(argv):
    args = get_glue_args(argv)
    gold_db = args["gold_db"]
    output_path = args["output_path"].rstrip("/")
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    return args, gold_db, output_path, start_date, end_date


def init_spark(args):
    spark, glue_context, job = init_glue_job(args["JOB_NAME"], args)
    return spark, glue_context, job


def export_fact_trips(spark, gold_table, output_path, start_date, end_date):
    df = spark.read.table(gold_table)
    if start_date and end_date and "trip_start_date" in df.columns:
        df = df.filter(
            (F.col("trip_start_date") >= F.lit(start_date))
            & (F.col("trip_start_date") <= F.lit(end_date))
        )
    df.write.mode("overwrite").parquet(output_path)


def main():
    args, gold_db, output_path, start_date, end_date = parse_args(sys.argv)
    spark, glue_context, job = init_spark(args)

    export_fact_trips(
        spark,
        f"{gold_db}.fact_trips",
        f"{output_path}/fact_trips",
        start_date,
        end_date,
    )

    job.commit()


if __name__ == "__main__":
    main()
