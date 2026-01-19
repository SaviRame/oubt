import sys

from glue_utils import get_glue_args, init_glue_job
from pyspark.sql import functions as F


# -----------------------------
# Normalization rules
# -----------------------------
def normalized_vendor_name(col: F.Column) -> F.Column:
    """
    normalized_name:
      - lowercase
      - trim
      - replace punctuation with spaces
      - collapse whitespace
      - remove common legal suffixes (LLC, Inc, Ltd, Corp, etc.)
    """
    x = F.lower(F.trim(col))
    x = F.regexp_replace(x, r"[^a-z0-9]+", " ")
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)

    x = F.regexp_replace(
        x,
        r"\b(llc|l\.l\.c|inc|incorporated|ltd|limited|corp|corporation|co|company)\b",
        "",
    )
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    return x


def parse_args(argv):
    args = get_glue_args(argv)
    bronze_table = f"{args['bronze_db']}.vendor"
    silver_table = f"{args['silver_db']}.vendor"
    ingestion_date = args["ingestion_date"]
    return args, bronze_table, silver_table, ingestion_date


def init_spark(args):
    spark, glueContext, job = init_glue_job(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark, glueContext, job


def read_bronze(glueContext, bronze_table, ingestion_date):
    """
    Read from Glue Data Catalog, filtering by ingestion_date.
    This ensures we only process data for the specific date.
    """
    if "." not in bronze_table:
        raise ValueError(f"Expected database.table format, got: {bronze_table}")
    bronze_db, bronze_name = bronze_table.split(".", 1)

    df = (
        glueContext.create_dynamic_frame.from_catalog(
            database=bronze_db, table_name=bronze_name
        )
        .toDF()
    )
    return df.filter(F.col("ingestion_date") == F.lit(ingestion_date))


def transform_vendor(df):
    df = df.select(
        F.col("vendor_id").cast("int").alias("vendor_id"),
        F.when(F.trim(F.col("vendor_name")) == "", F.lit(None))
        .otherwise(F.trim(F.col("vendor_name")))
        .alias("vendor_name"),
        F.col("ingestion_date").cast("date").alias("ingestion_date"),
    )

    df = df.withColumn("normalized_name", normalized_vendor_name(F.col("vendor_name")))
    return df.dropDuplicates(["vendor_id"])


def write_silver(df, spark, silver_table):
    """
    Create silver table if missing and overwrite only the processed partition.
    """
    if "." not in silver_table:
        raise ValueError(f"Expected database.table format, got: {silver_table}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            vendor_id INT,
            vendor_name STRING,
            normalized_name STRING,
            ingestion_date DATE
        )
        USING DELTA
        PARTITIONED BY (ingestion_date)
        """
    )

    (
        df.select("vendor_id", "vendor_name", "normalized_name", "ingestion_date")
        .write.format("delta")
        .mode("overwrite")
        .insertInto(silver_table)
    )


def main():
    args, bronze_table, silver_table, ingestion_date = parse_args(sys.argv)
    spark, glueContext, job = init_spark(args)
    df = read_bronze(glueContext, bronze_table, ingestion_date)
    df = transform_vendor(df)
    write_silver(df, spark, silver_table)
    job.commit()


if __name__ == "__main__":
    main()
