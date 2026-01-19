from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession


def get_glue_args(argv):
    return getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "bronze_db",
            "silver_db",
            "ingestion_date"  # YYYY-MM-DD
        ],
    )


def init_glue_job(job_name, args):
    # Ensure the Glue Data Catalog tables are visible to Spark SQL.
    spark = (
        SparkSession.builder.config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(job_name, args)
    return spark, glue_context, job
