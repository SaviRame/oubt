"""
Integration tests for vendor_master.py

Tests verify the core matching and grouping logic:
- Day 1: New vendors each get their own match group
- Day 2: Vendors are correctly matched to existing masters

Note: Delta MERGE operations are tested in AWS Glue environment.
"""
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vendor_master import (
    build_recordlinkage_metrics,
    normalized_vendor_name,
)


class TestVendorMasterIntegration:
    """Integration tests for the vendor master pipeline."""

    def test_day1_4_vendors_get_4_groups(self, spark, day1_vendors):
        """
        Day 1: Process 4 new vendors with no existing masters.

        Each vendor should get its own negative match_group (no matches found).
        - vendor_id=1 -> match_group=-1
        - vendor_id=2 -> match_group=-2
        - vendor_id=6 -> match_group=-6
        - vendor_id=7 -> match_group=-7
        """
        # Add normalized names
        vendor_df = day1_vendors.withColumn(
            "normalized_name", normalized_vendor_name(F.col("vendor_name"))
        )

        # Run matching with no existing masters
        metrics_df, debug_df = build_recordlinkage_metrics(
            vendor_df, existing_masters=None, ingestion_date="2026-01-18"
        )

        # Verify 4 vendors, each with their own group
        assert len(metrics_df) == 4, f"Expected 4 records, got {len(metrics_df)}"

        groups = dict(zip(metrics_df["vendor_id"], metrics_df["match_group"]))
        assert groups[1] == -1, f"vendor_id=1 should have group -1, got {groups[1]}"
        assert groups[2] == -2, f"vendor_id=2 should have group -2, got {groups[2]}"
        assert groups[6] == -6, f"vendor_id=6 should have group -6, got {groups[6]}"
        assert groups[7] == -7, f"vendor_id=7 should have group -7, got {groups[7]}"

        # Verify 4 unique groups (each vendor is its own master)
        unique_groups = metrics_df["match_group"].nunique()
        assert unique_groups == 4, f"Expected 4 unique groups, got {unique_groups}"

    def test_day2_matching_against_existing_masters(self, spark, day1_vendors, day2_vendors):
        """
        Day 2: Process 3 vendors against 4 existing masters.

        Input vendors:
        - vendor_id=7 'Helix Technologies' -> normalizes to 'helix' -> matches master gk=7 (Helix)
        - vendor_id=8 'Creative Mobil' -> normalizes to 'creative mobil' -> fuzzy matches master gk=1
        - vendor_id=10 'Savitha Technologies' -> normalizes to 'savitha' -> no match, new group

        Expected:
        - vendor_id=7 -> match_group=7 (exact match after normalization)
        - vendor_id=8 -> match_group=1 (fuzzy match to Creative Mobile)
        - vendor_id=10 -> match_group=-10 (no match, new master)
        """
        # First run Day 1 to get existing masters
        day1_df = day1_vendors.withColumn(
            "normalized_name", normalized_vendor_name(F.col("vendor_name"))
        )
        day1_metrics, _ = build_recordlinkage_metrics(
            day1_df, existing_masters=None, ingestion_date="2026-01-18"
        )

        # Build existing_masters using explicit schema to avoid pandas->spark issues
        # Collect day1 data and create a proper Spark DataFrame
        day1_data = day1_df.select("vendor_id", "vendor_name", "normalized_name").collect()

        # Map vendor_id to vendor_gk (using absolute value of match_group)
        gk_map = {int(row["vendor_id"]): abs(int(row["match_group"]))
                  for _, row in day1_metrics.iterrows()}

        master_data = [
            (gk_map[row.vendor_id], row.vendor_name, row.normalized_name)
            for row in day1_data
        ]
        master_schema = StructType([
            StructField("vendor_gk", LongType(), False),
            StructField("canonical_name", StringType(), True),
            StructField("normalized_name", StringType(), True),
        ])
        existing_masters = spark.createDataFrame(master_data, master_schema)

        # Run Day 2 matching
        day2_df = day2_vendors.withColumn(
            "normalized_name", normalized_vendor_name(F.col("vendor_name"))
        )
        day2_metrics, debug_df = build_recordlinkage_metrics(
            day2_df, existing_masters=existing_masters, ingestion_date="2026-01-19"
        )

        # Verify groupings
        groups = dict(zip(day2_metrics["vendor_id"], day2_metrics["match_group"]))

        # vendor_id=7 'Helix Technologies' -> 'helix' should match existing 'Helix' -> 'helix'
        assert groups[7] == 7, f"vendor_id=7 should match gk=7 (Helix), got {groups[7]}"

        # vendor_id=8 'Creative Mobil' should fuzzy match 'Creative Mobile' (gk=1)
        assert groups[8] == 1, f"vendor_id=8 should match gk=1 (Creative Mobile), got {groups[8]}"

        # vendor_id=10 'Savitha Technologies' has no match -> new negative group
        assert groups[10] == -10, f"vendor_id=10 should be new master (-10), got {groups[10]}"

        # Verify confidence scores
        confidences = dict(zip(day2_metrics["vendor_id"], day2_metrics["match_confidence"]))

        # Helix Technologies -> helix matches Helix -> helix exactly (1.0)
        assert confidences[7] >= 0.99, f"vendor_id=7 should have high confidence, got {confidences[7]}"

        # Creative Mobil should have >= 0.75 confidence (threshold for match)
        assert confidences[8] >= 0.75, f"vendor_id=8 should have confidence >= 0.75, got {confidences[8]}"

    def test_normalization_removes_business_words(self, spark):
        """Verify normalization correctly removes business words for matching."""
        data = [
            ("Helix Technologies",),
            ("Creative Mobile Technologies, LLC",),
            ("Myle Technologies Inc",),
            ("Savitha Technologies",),
        ]
        df = spark.createDataFrame(data, ["name"])
        result = df.select(
            F.col("name"),
            normalized_vendor_name(F.col("name")).alias("normalized")
        ).collect()

        normalized = {row["name"]: row["normalized"] for row in result}

        assert normalized["Helix Technologies"] == "helix"
        assert normalized["Creative Mobile Technologies, LLC"] == "creative mobile"
        assert normalized["Myle Technologies Inc"] == "myle"
        assert normalized["Savitha Technologies"] == "savitha"
