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
            "ingestion_date",
            "output_path",
        ],
    )


def parse_args(argv):
    args = get_glue_args(argv)
    bronze_table = f"{args['bronze_db']}.rate_code"
    master_table = f"{args['master_db']}.rate_code_ref"
    ingestion_date = args["ingestion_date"]
    output_path = args["output_path"]
    dim_path = f"{output_path}/rate_code_ref"
    return args, bronze_table, master_table, ingestion_date, dim_path


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


def clean_string(col: F.Column) -> F.Column:
    x = F.trim(col)
    x = F.regexp_replace(x, r"\s+", " ")
    return F.when(x == "", F.lit(None)).otherwise(x)


def transform_bronze(df):
    return df.select(
        F.col("ratecodeid").cast("int").alias("ratecode_id"),
        clean_string(F.col("description")).alias("description"),
    )


def build_new_dim_sql(spark, rate_code_df):
    rate_code_df.createOrReplaceTempView("rate_code_src")
    return spark.sql(
        """
        SELECT
            ABS(XXHASH64(CAST(ratecode_id AS STRING))) AS rate_code_gk,
            ratecode_id,
            description,
            SHA2(
                CONCAT_WS('|',
                    CAST(ratecode_id AS STRING),
                    COALESCE(description, '')
                ),
                256
            ) AS record_hash
        FROM rate_code_src
        """
    )


def merge_scd2_rate_code(spark, new_dim, table, path, ingestion_date):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            rate_code_gk LONG,
            ratecode_id INT,
            description STRING,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            change_reason STRING,
            record_hash STRING
        )
        USING DELTA
        LOCATION '{path}'
        """
    )

    new_dim.createOrReplaceTempView("new_dim")

    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING (
            SELECT n.rate_code_gk, n.ratecode_id, n.description, n.record_hash,
                   DATE '{ingestion_date}' AS valid_from,
                   CAST(NULL AS DATE) AS valid_to,
                   true AS is_current,
                   CASE WHEN e.rate_code_gk IS NULL THEN 'NEW' ELSE 'UPDATE' END AS change_reason,
                   1 AS _action
            FROM new_dim n
            LEFT JOIN {table} e ON n.rate_code_gk = e.rate_code_gk AND e.is_current
            WHERE e.rate_code_gk IS NULL OR e.record_hash != n.record_hash

            UNION ALL

            SELECT e.rate_code_gk, e.ratecode_id, e.description, e.record_hash,
                   e.valid_from,
                   DATE_SUB(DATE '{ingestion_date}', 1) AS valid_to,
                   false AS is_current,
                   e.change_reason,
                   0 AS _action
            FROM new_dim n
            JOIN {table} e ON n.rate_code_gk = e.rate_code_gk AND e.is_current
            WHERE e.record_hash != n.record_hash
        ) AS s
        ON t.rate_code_gk = s.rate_code_gk AND t.is_current AND s._action = 0
        WHEN MATCHED THEN
            UPDATE SET valid_to = s.valid_to, is_current = false
        WHEN NOT MATCHED THEN
            INSERT (rate_code_gk, ratecode_id, description, valid_from, valid_to, is_current, change_reason, record_hash)
            VALUES (s.rate_code_gk, s.ratecode_id, s.description, s.valid_from, s.valid_to, s.is_current, s.change_reason, s.record_hash)
        """
    )


def main():
    args, bronze_table, master_table, ingestion_date, dim_path = parse_args(sys.argv)
    spark, glue_context, job = init_spark(args)

    bronze_df = read_bronze(glue_context, bronze_table, ingestion_date)
    rate_code_df = transform_bronze(bronze_df)

    if rate_code_df.limit(1).count() == 0:
        job.commit()
        return

    new_dim = build_new_dim_sql(spark, rate_code_df)
    merge_scd2_rate_code(spark, new_dim, master_table, dim_path, ingestion_date)

    job.commit()


if __name__ == "__main__":
    main()
