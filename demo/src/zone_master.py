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
    bronze_table = f"{args['bronze_db']}.zone"
    master_table = f"{args['master_db']}.zone_master"
    ingestion_date = args["ingestion_date"]
    output_path = args["output_path"]
    dim_path = f"{output_path}/zone_master"
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
        F.col("locationid").cast("int").alias("location_id"),
        clean_string(F.col("borough")).alias("borough"),
        clean_string(F.col("zone")).alias("zone"),
        clean_string(F.col("service_zone")).alias("service_zone"),
    )


def build_new_dim_sql(spark, zone_df):
    zone_df.createOrReplaceTempView("zone_src")
    return spark.sql(
        """
        SELECT
            ABS(XXHASH64(CAST(location_id AS STRING))) AS zone_gk,
            location_id,
            borough,
            zone,
            service_zone,
            SHA2(
                CONCAT_WS('|',
                    CAST(location_id AS STRING),
                    COALESCE(borough, ''),
                    COALESCE(zone, ''),
                    COALESCE(service_zone, '')
                ),
                256
            ) AS record_hash
        FROM zone_src
        """
    )


def merge_scd2_zone(spark, new_dim, table, path, ingestion_date):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            zone_gk LONG,
            location_id INT,
            borough STRING,
            zone STRING,
            service_zone STRING,
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
            SELECT n.zone_gk, n.location_id, n.borough, n.zone, n.service_zone, n.record_hash,
                   DATE '{ingestion_date}' AS valid_from,
                   CAST(NULL AS DATE) AS valid_to,
                   true AS is_current,
                   CASE WHEN e.zone_gk IS NULL THEN 'NEW' ELSE 'UPDATE' END AS change_reason,
                   1 AS _action
            FROM new_dim n
            LEFT JOIN {table} e ON n.zone_gk = e.zone_gk AND e.is_current
            WHERE e.zone_gk IS NULL OR e.record_hash != n.record_hash

            UNION ALL

            SELECT e.zone_gk, e.location_id, e.borough, e.zone, e.service_zone, e.record_hash,
                   e.valid_from,
                   DATE_SUB(DATE '{ingestion_date}', 1) AS valid_to,
                   false AS is_current,
                   e.change_reason,
                   0 AS _action
            FROM new_dim n
            JOIN {table} e ON n.zone_gk = e.zone_gk AND e.is_current
            WHERE e.record_hash != n.record_hash
        ) AS s
        ON t.zone_gk = s.zone_gk AND t.is_current AND s._action = 0
        WHEN MATCHED THEN
            UPDATE SET valid_to = s.valid_to, is_current = false
        WHEN NOT MATCHED THEN
            INSERT (zone_gk, location_id, borough, zone, service_zone, valid_from, valid_to, is_current, change_reason, record_hash)
            VALUES (s.zone_gk, s.location_id, s.borough, s.zone, s.service_zone, s.valid_from, s.valid_to, s.is_current, s.change_reason, s.record_hash)
        """
    )


def main():
    args, bronze_table, master_table, ingestion_date, dim_path = parse_args(sys.argv)
    spark, glue_context, job = init_spark(args)

    bronze_df = read_bronze(glue_context, bronze_table, ingestion_date)
    zone_df = transform_bronze(bronze_df)

    if zone_df.limit(1).count() == 0:
        job.commit()
        return

    new_dim = build_new_dim_sql(spark, zone_df)
    merge_scd2_zone(spark, new_dim, master_table, dim_path, ingestion_date)

    job.commit()


if __name__ == "__main__":
    main()
