import sys

from awsglue.utils import getResolvedOptions
from glue_utils import init_glue_job
from pyspark.sql import functions as F


MASTER_TABLES = [
    "dim_vendor",
    "zone_master",
    "payment_type_ref",
    "rate_code_ref",
]


def get_glue_args(argv):
    args = getResolvedOptions(argv, ["JOB_NAME", "master_db", "output_path"])
    if "--current_only" in argv:
        args["current_only"] = getResolvedOptions(argv, ["current_only"])["current_only"]
    else:
        args["current_only"] = "true"
    return args


def parse_args(argv):
    args = get_glue_args(argv)
    master_db = args["master_db"]
    output_path = args["output_path"].rstrip("/")
    current_only = args["current_only"].lower() == "true"
    return args, master_db, output_path, current_only


def init_spark(args):
    spark, glue_context, job = init_glue_job(args["JOB_NAME"], args)
    return spark, glue_context, job


def export_table(spark, full_table_name, output_path, current_only):
    df = spark.read.table(full_table_name)
    if current_only and "is_current" in df.columns:
        df = df.filter(F.col("is_current") == F.lit(True))
    df.write.mode("overwrite").parquet(output_path)


def main():
    args, master_db, output_path, current_only = parse_args(sys.argv)
    spark, glue_context, job = init_spark(args)

    for table in MASTER_TABLES:
        export_table(
            spark,
            f"{master_db}.{table}",
            f"{output_path}/{table}",
            current_only,
        )

    job.commit()


if __name__ == "__main__":
    main()
