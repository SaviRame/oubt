import shutil
import sys
import tempfile
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# Mock AWS Glue modules before importing vendor_master
sys.modules["awsglue"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()
sys.modules["glue_utils"] = MagicMock()


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("test-vendor-master")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _vendor_schema():
    return StructType([
        StructField("vendor_id", IntegerType(), False),
        StructField("vendor_name", StringType(), True),
        StructField("ingestion_date", DateType(), True),
    ])


@pytest.fixture
def day1_vendors(spark):
    """Day 1 vendor data (2026-01-18): 4 new vendors."""
    from datetime import date
    data = [
        (1, "Creative Mobile Technologies, LLC", date(2026, 1, 18)),
        (2, "Curb Mobility, LLC", date(2026, 1, 18)),
        (6, "Myle Technologies Inc", date(2026, 1, 18)),
        (7, "Helix", date(2026, 1, 18)),
    ]
    return spark.createDataFrame(data, _vendor_schema())


@pytest.fixture
def day2_vendors(spark):
    """Day 2 vendor data (2026-01-19): 3 vendors - 1 existing match, 1 fuzzy match, 1 new."""
    from datetime import date
    data = [
        (7, "Helix Technologies", date(2026, 1, 19)),      # Matches existing Helix (exact after normalization)
        (8, "Creative Mobil", date(2026, 1, 19)),          # Fuzzy matches Creative Mobile
        (10, "Savitha Technologies", date(2026, 1, 19)),   # New vendor, no match
    ]
    return spark.createDataFrame(data, _vendor_schema())
