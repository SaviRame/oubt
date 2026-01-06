#!/usr/bin/env python3
"""
Vendor Deduplication Pipeline for NYC Taxi TPEP Providers

This script demonstrates MDM (Master Data Management) deduplication techniques
for vendor master data, following Day 9-10 training objectives:
- Fuzzy matching algorithms (Jaro-Winkler, Levenshtein)
- Probabilistic matching with weighted scores
- Match confidence-based governance workflow

Status Classification (per MDM Governance):
- auto_merge (≥95%): High confidence - Auto-merge with audit log
- Steward_review (80-95%): Medium confidence - Flag for steward review
- manual_resolution_needed (<80%): Low confidence - Require manual resolution

To install: pip install recordlinkage pandas
"""

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import pandas as pd
import recordlinkage

# Output file path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCORECARD_FILE = os.path.join(SCRIPT_DIR, "vendor-quality-scorecard.md")


class MatchStatus(Enum):
    """MDM governance status for duplicate matches."""

    AUTO_MERGE = "auto_merge"
    STEWARD_REVIEW = "Steward_review"
    MANUAL_RESOLUTION = "manual_resolution_needed"


@dataclass
class MatchThresholds:
    """Configurable thresholds for match classification."""

    auto_merge: float = 0.95  # ≥95% confidence
    steward_review: float = 0.80  # 80-95% confidence
    # Below 80% = manual_resolution_needed


# Field weights for weighted scoring
FIELD_WEIGHTS = {
    "company_name_score": 0.30,  # Primary identifier
    "tax_id_score": 0.25,  # High confidence when matching
    "address_score": 0.15,  # Handles abbreviations
    "phone_score": 0.15,  # After normalization
    "contact_name_score": 0.10,  # Secondary identifier
    "license_number_score": 0.05,  # Regulatory identifier
}


def create_sample_vendor_data() -> pd.DataFrame:
    """
    Create sample TPEP vendor data with intentional duplicates.

    Based on NYC TLC Data Dictionary vendors:
    - 1 = Creative Mobile Technologies, LLC
    - 2 = Curb Mobility, LLC
    - 6 = Myle Technologies Inc
    - 7 = Helix

    Includes various duplicate scenarios for testing.
    """
    data = {
        "vendor_id": list(range(1, 19)),
        "company_name": [
            # Original vendors
            "Creative Mobile Technologies, LLC",  # 1 - Original
            "Curb Mobility, LLC",  # 2 - Original
            "Myle Technologies Inc",  # 3 - Original
            "Helix",  # 4 - Original
            # Duplicates with variations
            "Creative Mobile Technologies LLC",  # 5 - Missing comma (dup of 1)
            "Cretive Mobile Technologies, LLC",  # 6 - Typo (dup of 1)
            "Creative Mobile Tech, L.L.C.",  # 7 - Abbreviation (dup of 1)
            "Curb Mobility LLC",  # 8 - Missing comma (dup of 2)
            "Curb Mobility, L.L.C.",  # 9 - Different LLC format (dup of 2)
            "Myle Technologies, Inc.",  # 10 - Added comma (dup of 3)
            "Myle Tech Inc",  # 11 - Abbreviation (dup of 3)
            "Helix Transportation",  # 12 - Different name (maybe dup of 4?)
            "HELIX",  # 13 - All caps (dup of 4)
            # Additional vendors (not duplicates)
            "Metro Taxi Services, LLC",  # 14 - Unique
            "City Cab Corporation",  # 15 - Unique
            "Urban Transit Technologies",  # 16 - Unique
            # More duplicates
            "Curb Mobility",  # 17 - Missing LLC (dup of 2)
            "Creative Mobile Technologies",  # 18 - Missing LLC (dup of 1)
        ],
        "tax_id": [
            "12-3456789",  # 1
            "23-4567890",  # 2
            "34-5678901",  # 3
            "45-6789012",  # 4
            "12-3456789",  # 5 - Same as 1
            "12-3456789",  # 6 - Same as 1
            "12-3456789",  # 7 - Same as 1
            "23-4567890",  # 8 - Same as 2
            "23-4567890",  # 9 - Same as 2
            "34-5678901",  # 10 - Same as 3
            "",  # 11 - Missing (should still match 3)
            "45-6789999",  # 12 - Different from 4
            "45-6789012",  # 13 - Same as 4
            "56-7890123",  # 14 - Unique
            "67-8901234",  # 15 - Unique
            "78-9012345",  # 16 - Unique
            "23-4567890",  # 17 - Same as 2
            "",  # 18 - Missing
        ],
        "address": [
            "123 Main Street, Suite 100",  # 1
            "456 Broadway, Floor 5",  # 2
            "789 Tech Park Drive",  # 3
            "321 Innovation Way",  # 4
            "123 Main St, Suite 100",  # 5 - Abbreviated
            "123 Main Street Suite 100",  # 6 - Missing comma
            "123 Main Street, Ste 100",  # 7 - Abbreviated
            "456 Broadway, Fl 5",  # 8 - Abbreviated
            "456 Broadway Floor 5",  # 9 - Missing comma
            "789 Tech Park Dr",  # 10 - Abbreviated
            "789 Tech Park Drive",  # 11 - Same as 3
            "321 Innovation Way, Bldg A",  # 12 - Additional info
            "321 Innovation Way",  # 13 - Same as 4
            "555 Commerce Blvd",  # 14 - Unique
            "777 Downtown Ave",  # 15 - Unique
            "999 Silicon Valley Rd",  # 16 - Unique
            "456 Broadway",  # 17 - Partial
            "123 Main Street",  # 18 - Partial
        ],
        "city": [
            "New York",  # 1
            "New York",  # 2
            "Jersey City",  # 3
            "Brooklyn",  # 4
            "New York",  # 5
            "New York",  # 6
            "New York",  # 7
            "New York",  # 8
            "New York",  # 9
            "Jersey City",  # 10
            "Jersey City",  # 11
            "Brooklyn",  # 12
            "Brooklyn",  # 13
            "Manhattan",  # 14
            "Queens",  # 15
            "Hoboken",  # 16
            "New York",  # 17
            "New York",  # 18
        ],
        "state": [
            "NY",
            "NY",
            "NJ",
            "NY",
            "NY",
            "NY",
            "NY",
            "NY",
            "NY",
            "NJ",
            "NJ",
            "NY",
            "NY",
            "NY",
            "NY",
            "NJ",
            "NY",
            "NY",
        ],
        "phone": [
            "212-555-1234",  # 1
            "212-555-2345",  # 2
            "201-555-3456",  # 3
            "718-555-4567",  # 4
            "(212) 555-1234",  # 5 - Different format
            "2125551234",  # 6 - No separators
            "212.555.1234",  # 7 - Dots
            "212-555-2345",  # 8 - Same as 2
            "(212) 555-2345",  # 9 - Different format
            "201-555-3456",  # 10 - Same as 3
            "201.555.3456",  # 11 - Dots
            "718-555-9999",  # 12 - Different
            "718-555-4567",  # 13 - Same as 4
            "212-555-5555",  # 14 - Unique
            "718-555-6666",  # 15 - Unique
            "201-555-7777",  # 16 - Unique
            "212-555-2345",  # 17 - Same as 2
            "212-555-1234",  # 18 - Same as 1
        ],
        "contact_name": [
            "John Smith",  # 1
            "Jane Doe",  # 2
            "Robert Johnson",  # 3
            "Alice Williams",  # 4
            "John Smith",  # 5 - Same
            "Jon Smith",  # 6 - Typo
            "J. Smith",  # 7 - Abbreviated
            "Jane Doe",  # 8 - Same
            "J. Doe",  # 9 - Abbreviated
            "Robert Johnson",  # 10 - Same
            "Bob Johnson",  # 11 - Nickname
            "Alice Williams",  # 12 - Same
            "A. Williams",  # 13 - Abbreviated
            "Michael Brown",  # 14 - Unique
            "Sarah Davis",  # 15 - Unique
            "David Wilson",  # 16 - Unique
            "Jane D.",  # 17 - Abbreviated
            "John S.",  # 18 - Abbreviated
        ],
        "license_number": [
            "TPEP-001",  # 1
            "TPEP-002",  # 2
            "TPEP-006",  # 3
            "TPEP-007",  # 4
            "TPEP-001",  # 5 - Same
            "TPEP-001",  # 6 - Same
            "TPEP-001",  # 7 - Same
            "TPEP-002",  # 8 - Same
            "TPEP-002",  # 9 - Same
            "TPEP-006",  # 10 - Same
            "TPEP-006",  # 11 - Same
            "TPEP-007-A",  # 12 - Different
            "TPEP-007",  # 13 - Same
            "TPEP-014",  # 14 - Unique
            "TPEP-015",  # 15 - Unique
            "TPEP-016",  # 16 - Unique
            "TPEP-002",  # 17 - Same
            "",  # 18 - Missing
        ],
    }
    return pd.DataFrame(data)


def normalize_phone(phone: str) -> str:
    """Normalize phone number by removing non-digit characters."""
    if pd.isna(phone):
        return ""
    return re.sub(r"\D", "", str(phone))


def print_header(title: str, char: str = "=", width: int = 70) -> None:
    """Print a formatted header."""
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def print_dataframe(df: pd.DataFrame, title: str) -> None:
    """Print a dataframe with a nice header."""
    print_header(title)
    # Select key columns for display
    display_cols = ["vendor_id", "company_name", "tax_id", "city", "phone"]
    print(df[display_cols].to_string(index=False))


def calculate_weighted_score(scores: pd.Series) -> float:
    """
    Calculate weighted average score based on field weights.

    Args:
        scores: Series containing individual field scores

    Returns:
        Weighted average score (0.0 to 1.0)
    """
    total_weight = 0.0
    weighted_sum = 0.0

    for field, weight in FIELD_WEIGHTS.items():
        if field in scores.index:
            score = scores[field]
            # Handle missing values (treat as 0.5 - neutral)
            if pd.isna(score):
                score = 0.5
            weighted_sum += score * weight
            total_weight += weight

    if total_weight == 0:
        return 0.0

    return weighted_sum / total_weight


def classify_match(
    score: float, thresholds: Optional[MatchThresholds] = None
) -> MatchStatus:
    """
    Classify a match based on confidence score.

    Args:
        score: Weighted match score (0.0 to 1.0)
        thresholds: Optional custom thresholds

    Returns:
        MatchStatus enum value
    """
    if thresholds is None:
        thresholds = MatchThresholds()

    if score >= thresholds.auto_merge:
        return MatchStatus.AUTO_MERGE
    elif score >= thresholds.steward_review:
        return MatchStatus.STEWARD_REVIEW
    else:
        return MatchStatus.MANUAL_RESOLUTION


def get_status_symbol(status: MatchStatus) -> str:
    """Get display symbol for match status."""
    symbols = {
        MatchStatus.AUTO_MERGE: "✓ AUTO-MERGE",
        MatchStatus.STEWARD_REVIEW: "? STEWARD REVIEW",
        MatchStatus.MANUAL_RESOLUTION: "✗ MANUAL RESOLUTION",
    }
    return symbols.get(status, "?")


def print_comparison_result(
    df: pd.DataFrame, features: pd.DataFrame, pair: tuple
) -> tuple[float, MatchStatus]:
    """
    Print a detailed comparison between two vendor records.

    Args:
        df: Original vendor dataframe
        features: Comparison features dataframe
        pair: Tuple of (idx1, idx2) for the pair being compared

    Returns:
        Tuple of (weighted_score, status)
    """
    idx1, idx2 = pair
    rec1 = df.loc[idx1]
    rec2 = df.loc[idx2]
    scores_row = features.loc[pair]
    # Convert to Series if it's a DataFrame row
    if isinstance(scores_row, pd.DataFrame):
        scores = scores_row.iloc[0]
    else:
        scores = scores_row

    print(f"\n  Record {idx1} vs Record {idx2}")
    print(f"  {'-' * 60}")
    print(
        f"  {'Field':<18} {'Record ' + str(idx1):<20} {'Record ' + str(idx2):<20} {'Score':<8}"
    )
    print(f"  {'-' * 60}")

    # Display each field comparison
    field_mappings = [
        ("company_name", "company_name_score"),
        ("tax_id", "tax_id_score"),
        ("address", "address_score"),
        ("phone", "phone_score"),
        ("contact_name", "contact_name_score"),
        ("license_number", "license_number_score"),
    ]

    for field, score_col in field_mappings:
        val1 = str(rec1[field])[:18] if pd.notna(rec1[field]) else "(empty)"
        val2 = str(rec2[field])[:18] if pd.notna(rec2[field]) else "(empty)"
        score_val = scores[score_col] if score_col in scores.index else 0.0
        if pd.isna(score_val):
            score_str = "N/A"
        else:
            score_str = f"{float(score_val):.2f}"
        print(f"  {field:<18} {val1:<20} {val2:<20} {score_str:<8}")

    # Calculate weighted score
    weighted_score = calculate_weighted_score(scores)
    status = classify_match(weighted_score)

    print(f"  {'-' * 60}")
    print(f"  {'Weighted Score:':<58} {weighted_score:.2f}")
    print(f"  {'Status:':<58} {get_status_symbol(status)}")

    return weighted_score, status


def main():
    """Run the vendor deduplication demonstration."""

    print("\n" + "=" * 70)
    print("     VENDOR DEDUPLICATION - MDM Governance Demo")
    print("     NYC Taxi TPEP Provider Master Data Management")
    print("=" * 70)

    # Step 1: Create sample data
    df = create_sample_vendor_data()

    # Add normalized phone for comparison
    df["phone_normalized"] = df["phone"].apply(normalize_phone)

    print_dataframe(df, "STEP 1: Sample TPEP Vendor Data")

    print("\n  Vendor data includes intentional duplicates:")
    print("  - Records 1, 5, 6, 7, 18: Creative Mobile Technologies variations")
    print("  - Records 2, 8, 9, 17: Curb Mobility variations")
    print("  - Records 3, 10, 11: Myle Technologies variations")
    print("  - Records 4, 12, 13: Helix variations")

    # Step 2: Create candidate pairs using blocking
    print_header("STEP 2: Create Candidate Pairs (Blocking)")

    indexer = recordlinkage.Index()
    # Block on city to reduce comparisons
    indexer.block("city")

    candidate_pairs = indexer.index(df)

    total_possible = len(df) * (len(df) - 1) // 2
    print("\n  Blocking on 'city' field reduces comparisons:")
    print(f"  - Total records: {len(df)}")
    print(f"  - Without blocking: {total_possible} comparisons")
    print(f"  - With blocking: {len(candidate_pairs)} comparisons")
    reduction = 100 * (1 - len(candidate_pairs) / total_possible)
    print(f"  - Reduction: {reduction:.0f}%")

    # Step 3: Compare records
    print_header("STEP 3: Compare Records")

    compare = recordlinkage.Compare()

    # Company name - Jaro-Winkler (good for typos)
    compare.string(
        "company_name", "company_name", method="jarowinkler", label="company_name_score"
    )

    # Tax ID - Exact match (high confidence)
    compare.exact("tax_id", "tax_id", label="tax_id_score")

    # Address - Jaro-Winkler (handles abbreviations)
    compare.string("address", "address", method="jarowinkler", label="address_score")

    # Phone - Exact match on normalized
    compare.exact("phone_normalized", "phone_normalized", label="phone_score")

    # Contact name - Jaro-Winkler
    compare.string(
        "contact_name", "contact_name", method="jarowinkler", label="contact_name_score"
    )

    # License number - Exact match
    compare.exact("license_number", "license_number", label="license_number_score")

    features = compare.compute(candidate_pairs, df)

    print("\n  Comparison methods and weights:")
    print("  ┌────────────────────┬─────────────────┬────────┐")
    print("  │ Field              │ Method          │ Weight │")
    print("  ├────────────────────┼─────────────────┼────────┤")
    print("  │ company_name       │ Jaro-Winkler    │  30%   │")
    print("  │ tax_id             │ Exact Match     │  25%   │")
    print("  │ address            │ Jaro-Winkler    │  15%   │")
    print("  │ phone              │ Exact (normal.) │  15%   │")
    print("  │ contact_name       │ Jaro-Winkler    │  10%   │")
    print("  │ license_number     │ Exact Match     │   5%   │")
    print("  └────────────────────┴─────────────────┴────────┘")

    # Step 4: Detailed comparisons and classification
    print_header("STEP 4: Detailed Comparison Results")

    # Store results by status
    results_by_status: dict[MatchStatus, list] = {
        MatchStatus.AUTO_MERGE: [],
        MatchStatus.STEWARD_REVIEW: [],
        MatchStatus.MANUAL_RESOLUTION: [],
    }

    # Only show pairs with some similarity (score > 0.5)
    for pair in candidate_pairs:
        scores_row = features.loc[pair]
        # Convert to Series if it's a DataFrame row
        if isinstance(scores_row, pd.DataFrame):
            scores = scores_row.iloc[0]
        else:
            scores = scores_row
        weighted_score = calculate_weighted_score(scores)

        if weighted_score >= 0.5:  # Only show potentially related pairs
            score, status = print_comparison_result(df, features, pair)
            results_by_status[status].append((pair, score))

    # Step 5: Summary by status
    print_header("STEP 5: Summary - Matches by Status")

    print("\n  Classification Thresholds:")
    print("  ┌─────────────────────────────┬───────────────────────────────┐")
    print("  │ Status                      │ Confidence Score              │")
    print("  ├─────────────────────────────┼───────────────────────────────┤")
    print("  │ auto_merge                  │ ≥ 95% (High confidence)       │")
    print("  │ Steward_review              │ 80% - 95% (Medium confidence) │")
    print("  │ manual_resolution_needed    │ < 80% (Low confidence)        │")
    print("  └─────────────────────────────┴───────────────────────────────┘")

    # Print results by status
    for status in MatchStatus:
        matches = results_by_status[status]
        print(f"\n  {get_status_symbol(status)} ({len(matches)} pairs):")

        if matches:
            for pair, score in sorted(matches, key=lambda x: -x[1]):
                rec1 = df.loc[pair[0]]
                rec2 = df.loc[pair[1]]
                name1 = rec1["company_name"][:25]
                name2 = rec2["company_name"][:25]
                print(f"    Records {pair[0]:2d} & {pair[1]:2d} (Score: {score:.2f})")
                print(f"      → {name1}")
                print(f"      → {name2}")
        else:
            print("    (No matches in this category)")

    # Step 6: Steward Review Queue
    print_header("STEP 6: Steward Review Queue")

    steward_items = results_by_status[MatchStatus.STEWARD_REVIEW]
    if steward_items:
        print("\n  The following pairs require steward review:")
        print("  ┌─────┬─────────────────────────────────────────────────┬───────┐")
        print("  │ #   │ Vendor Pair                                     │ Score │")
        print("  ├─────┼─────────────────────────────────────────────────┼───────┤")
        for i, (pair, score) in enumerate(steward_items, 1):
            rec1 = df.loc[pair[0]]
            rec2 = df.loc[pair[1]]
            pair_desc = f"{rec1['company_name'][:20]} vs {rec2['company_name'][:20]}"
            print(f"  │ {i:<3} │ {pair_desc:<47} │ {score:.2f}  │")
        print("  └─────┴─────────────────────────────────────────────────┴───────┘")
        print("\n  Action Required: Data Steward must review and approve/reject merges")
    else:
        print("\n  No items pending steward review.")

    # Step 7: Key Concepts
    print_header("STEP 7: Key Concepts - MDM Matching & Governance")
    print(
        """
  1. BLOCKING STRATEGY
     - Groups records by common field (city) to reduce comparisons
     - Trade-off: May miss duplicates across different cities
     - Consider multiple blocking passes for comprehensive matching

  2. WEIGHTED SCORING
     - Different fields have different reliability for matching
     - Tax ID (25%): High weight - unique business identifier
     - Company Name (30%): Primary identifier, but prone to variations
     - Phone/Address (15% each): Supporting evidence

  3. MATCH CONFIDENCE & GOVERNANCE
     ┌─────────────────────────────────────────────────────────────┐
     │  Score ≥ 95%  →  auto_merge                                 │
     │                  High confidence, merge with audit log      │
     ├─────────────────────────────────────────────────────────────┤
     │  80% - 95%    →  Steward_review                             │
     │                  Medium confidence, requires human review   │
     ├─────────────────────────────────────────────────────────────┤
     │  Score < 80%  →  manual_resolution_needed                   │
     │                  Low confidence, complex investigation      │
     └─────────────────────────────────────────────────────────────┘

  4. MDM LIFECYCLE STATES
     - Proposed: New record submitted, awaiting review
     - Active: Approved golden record, in use
     - Deprecated: Marked for retirement
     - Retired: Archived for compliance

  5. SURVIVORSHIP RULES
     - When merging duplicates, determine which values survive:
       • Most complete record wins
       • Most recent update wins
       • Source system priority
       • Manual steward selection
"""
    )

    # Final statistics
    print_header("FINAL STATISTICS")
    total_matches = sum(len(v) for v in results_by_status.values())
    print(f"\n  Total candidate pairs evaluated: {len(candidate_pairs)}")
    print(f"  Pairs with similarity ≥ 50%: {total_matches}")
    print(f"  ├── auto_merge: {len(results_by_status[MatchStatus.AUTO_MERGE])}")
    print(f"  ├── Steward_review: {len(results_by_status[MatchStatus.STEWARD_REVIEW])}")
    print(
        f"  └── manual_resolution_needed: {len(results_by_status[MatchStatus.MANUAL_RESOLUTION])}"
    )
    print()

    # Step 8: Generate Quality Scorecard with Business Impact Mapping
    print_header("STEP 8: Generating Quality Scorecard")
    generate_quality_scorecard(df, results_by_status, len(candidate_pairs))
    print(f"\n  Scorecard written to: {SCORECARD_FILE}")


def generate_quality_scorecard(
    df: pd.DataFrame,
    results_by_status: dict[MatchStatus, list],
    total_pairs: int,
) -> None:
    """
    Generate a markdown quality scorecard with business impact mapping.

    Args:
        df: Original vendor dataframe
        results_by_status: Dictionary of match results grouped by status
        total_pairs: Total number of candidate pairs evaluated
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_records = len(df)
    total_matches = sum(len(v) for v in results_by_status.values())

    auto_merge_count = len(results_by_status[MatchStatus.AUTO_MERGE])
    steward_count = len(results_by_status[MatchStatus.STEWARD_REVIEW])
    manual_count = len(results_by_status[MatchStatus.MANUAL_RESOLUTION])

    # Calculate percentages
    if total_matches > 0:
        auto_pct = auto_merge_count / total_matches * 100
        steward_pct = steward_count / total_matches * 100
        manual_pct = manual_count / total_matches * 100
    else:
        auto_pct = steward_pct = manual_pct = 0

    # Calculate duplicate rate (estimated unique records with duplicates / total records)
    # Each pair represents 2 records that might be duplicates, but we count unique records involved
    records_with_duplicates = set()
    for status_matches in results_by_status.values():
        for pair, _ in status_matches:
            records_with_duplicates.add(pair[0])
            records_with_duplicates.add(pair[1])
    duplicate_rate = (
        len(records_with_duplicates) / total_records * 100 if total_records > 0 else 0
    )

    # Count data quality issues
    missing_tax_ids = df[df["tax_id"] == ""].shape[0]
    missing_licenses = df[df["license_number"] == ""].shape[0]

    # Determine overall health
    if auto_pct >= 70 and steward_pct <= 20:
        health_status = "✅ HEALTHY"
        health_desc = "High automation rate, low manual intervention needed"
    elif steward_pct <= 30 and manual_pct <= 20:
        health_status = "⚠️ NEEDS ATTENTION"
        health_desc = "Moderate steward workload, some data quality issues"
    else:
        health_status = "❌ CRITICAL"
        health_desc = (
            "High manual intervention required, significant data quality issues"
        )

    # Build the scorecard
    scorecard = f"""# Vendor MDM Quality Scorecard

**Generated:** {now}
**Data Source:** NYC Taxi TPEP Vendor Master Data
**Domain:** Vendor Management

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Overall Health** | {health_status} |
| **Health Description** | {health_desc} |
| **Total Vendor Records** | {total_records} |
| **Potential Duplicate Pairs** | {total_matches} |
| **Duplicate Rate** | {duplicate_rate:.1f}% |

---

## Match Confidence Distribution

| Status | Count | Percentage | Description |
|--------|-------|------------|-------------|
| ✅ `auto_merge` | {auto_merge_count} | {auto_pct:.1f}% | High confidence - can be merged automatically |
| ⚠️ `Steward_review` | {steward_count} | {steward_pct:.1f}% | Medium confidence - requires human review |
| ❌ `manual_resolution_needed` | {manual_count} | {manual_pct:.1f}% | Low confidence - complex investigation needed |

---

## Business Impact Analysis

### Financial Impact

| Issue | Count | Business Impact | Estimated Risk |
|-------|-------|-----------------|----------------|
| Duplicate vendor records | {total_matches} pairs | Potential duplicate payments | **High** - Each duplicate could result in overpayment |
| Missing Tax IDs | {missing_tax_ids} records | 1099 reporting compliance risk | **Critical** - IRS penalties possible |
| Missing License Numbers | {missing_licenses} records | Regulatory compliance risk | **Medium** - TLC audit findings |

### Operational Impact

| Metric | Value | Impact |
|--------|-------|--------|
| Auto-merge ready | {auto_merge_count} pairs | **{auto_merge_count * 5} minutes saved** (vs manual review) |
| Steward review queue | {steward_count} pairs | **{steward_count * 15} minutes** of steward time required |
| Manual investigation | {manual_count} pairs | **{manual_count * 30} minutes** of analyst time required |
| **Total Processing Time** | - | **{auto_merge_count * 5 + steward_count * 15 + manual_count * 30} minutes** estimated |

### Data Quality Dimensions

| Dimension | Score | Status | Business Impact |
|-----------|-------|--------|-----------------|
| **Uniqueness** | {100 - duplicate_rate:.1f}% | {"✅" if duplicate_rate < 30 else "⚠️" if duplicate_rate < 50 else "❌"} | Duplicate vendors affect payment accuracy |
| **Completeness (Tax ID)** | {(total_records - missing_tax_ids) / total_records * 100:.1f}% | {"✅" if missing_tax_ids == 0 else "⚠️" if missing_tax_ids < 3 else "❌"} | Missing Tax IDs block 1099 reporting |
| **Completeness (License)** | {(total_records - missing_licenses) / total_records * 100:.1f}% | {"✅" if missing_licenses == 0 else "⚠️" if missing_licenses < 3 else "❌"} | Missing licenses risk regulatory fines |

---

## Steward Review Queue

"""

    # Add steward review items
    steward_items = results_by_status[MatchStatus.STEWARD_REVIEW]
    if steward_items:
        scorecard += """| # | Vendor Pair | Score | Priority |
|---|-------------|-------|----------|
"""
        for i, (pair, score) in enumerate(steward_items, 1):
            rec1 = df.loc[pair[0]]
            rec2 = df.loc[pair[1]]
            priority = "High" if score >= 0.90 else "Medium" if score >= 0.85 else "Low"
            scorecard += f"| {i} | {rec1['company_name'][:25]} vs {rec2['company_name'][:25]} | {score:.2f} | {priority} |\n"
    else:
        scorecard += "*No items pending steward review.*\n"

    # Write to file
    with open(SCORECARD_FILE, "w") as f:
        f.write(scorecard)


if __name__ == "__main__":
    main()
