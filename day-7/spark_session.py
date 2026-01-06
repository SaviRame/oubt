"""
Spark Session Utility for Day 7 PySpark Scripts

Creates a Spark session with Delta Lake and S3 support.

Usage:
    from spark_session import create_spark_session
    spark = create_spark_session("MyAppName")
"""

import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session with Delta Lake and S3 support.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession instance
    """
    # Package dependencies
    packages = (
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    spark = (
        SparkSession.builder.appName(app_name)
        # Package dependencies
        .config("spark.jars.packages", packages)
        # Adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Broadcast join threshold (10MB)
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # S3A filesystem
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .getOrCreate()
    )

    # Configure AWS credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if aws_access_key and aws_secret_key:
        hadoop_conf.set("fs.s3a.access.key", aws_access_key)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    else:
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        )

    spark.sparkContext.setLogLevel("WARN")

    return spark
