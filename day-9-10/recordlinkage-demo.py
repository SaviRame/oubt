#!/usr/bin/env python3
"""
Simple Record Linkage Tutorial

This beginner-friendly script demonstrates how to find duplicate records
in a dataset using the recordlinkage library.

To install: pip install recordlinkage pandas
"""

import pandas as pd
import recordlinkage


def create_sample_data() -> pd.DataFrame:
    """Create sample customer data with potential duplicates."""
    data = {
        "customer_id": [1, 2, 3, 4, 5, 6],
        "first_name": ["John", "Jon", "Jane", "Robert", "Bob", "Alice"],
        "last_name": ["Smith", "Smith", "Doe", "Johnson", "Johnson", "Williams"],
        "city": ["New York", "New York", "Boston", "Chicago", "Chicago", "Seattle"],
        "phone": [
            "555-1234",
            "555-1234",
            "555-5678",
            "555-9999",
            "555-9999",
            "555-0000",
        ],
    }
    return pd.DataFrame(data)


def print_dataframe(df: pd.DataFrame, title: str) -> None:
    """Print a dataframe with a nice header."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    print(df.to_string(index=False))


def print_comparison_result(
    df: pd.DataFrame, features: pd.DataFrame, pair: tuple
) -> None:
    """Print a detailed comparison between two records."""
    idx1, idx2 = pair
    rec1 = df.loc[idx1]
    rec2 = df.loc[idx2]
    scores = features.loc[pair]

    print(f"\n  Record {idx1} vs Record {idx2}")
    print(f"  {'-' * 50}")
    print(
        f"  {'Field':<15} {'Record ' + str(idx1):<15} {'Record ' + str(idx2):<15} {'Score':<10}"
    )
    print(f"  {'-' * 50}")

    for col in ["first_name", "last_name", "city", "phone"]:
        score_col = f"{col}_score"
        score = scores[score_col] if score_col in scores.index else "N/A"
        if isinstance(score, float):
            score_str = f"{score:.2f}"
        else:
            score_str = str(score)
        print(f"  {col:<15} {str(rec1[col]):<15} {str(rec2[col]):<15} {score_str:<10}")

    # Calculate average score
    avg_score_val = scores.values.mean()
    print(f"  {'-' * 50}")
    print(f"  {'Average Score:':<45} {avg_score_val:.2f}")

    # Recommendation
    if avg_score_val >= 0.9:
        recommendation = "✓ LIKELY DUPLICATE - Auto-merge recommended"
    elif avg_score_val >= 0.7:
        recommendation = "? POSSIBLE DUPLICATE - Manual review needed"
    else:
        recommendation = "✗ PROBABLY DIFFERENT - No action needed"
    print(f"  Recommendation: {recommendation}")


def main():
    """Run the record linkage demonstration."""

    print("\n" + "=" * 70)
    print("         RECORD LINKAGE TUTORIAL - Finding Duplicates")
    print("=" * 70)

    # Step 1: Create sample data
    df = create_sample_data()
    print_dataframe(df, "STEP 1: Sample Customer Data")

    print("\n  Notice potential duplicates:")
    print("  - Records 1 & 2: 'John Smith' vs 'Jon Smith' (same phone, city)")
    print("  - Records 4 & 5: 'Robert Johnson' vs 'Bob Johnson' (same phone, city)")

    # Step 2: Create candidate pairs using blocking
    print(f"\n{'=' * 70}")
    print("  STEP 2: Create Candidate Pairs (Blocking)")
    print(f"{'=' * 70}")

    indexer = recordlinkage.Index()
    indexer.block("city")  # Only compare records in the same city

    candidate_pairs = indexer.index(df)

    print("\n  Blocking on 'city' field reduces comparisons:")
    print(f"  - Total records: {len(df)}")
    print(f"  - Without blocking: {len(df) * (len(df) - 1) // 2} comparisons")
    print(f"  - With blocking: {len(candidate_pairs)} comparisons")
    print(
        f"  - Reduction: {100 * (1 - len(candidate_pairs) / (len(df) * (len(df) - 1) // 2)):.0f}%"
    )

    print("\n  Candidate pairs to compare:")
    for pair in candidate_pairs:
        city = df.loc[pair[0], "city"]
        print(f"    Record {pair[0]} vs Record {pair[1]} (both in {city})")

    # Step 3: Compare records
    print(f"\n{'=' * 70}")
    print("  STEP 3: Compare Records")
    print(f"{'=' * 70}")

    compare = recordlinkage.Compare()
    compare.string(
        "first_name", "first_name", method="jarowinkler", label="first_name_score"
    )
    compare.string(
        "last_name", "last_name", method="jarowinkler", label="last_name_score"
    )
    compare.exact("city", "city", label="city_score")
    compare.exact("phone", "phone", label="phone_score")

    features = compare.compute(candidate_pairs, df)

    print("\n  Comparison methods used:")
    print("  - first_name: Jaro-Winkler similarity (0.0 to 1.0)")
    print("  - last_name:  Jaro-Winkler similarity (0.0 to 1.0)")
    print("  - city:       Exact match (0 or 1)")
    print("  - phone:      Exact match (0 or 1)")

    # Step 4: Show detailed comparisons
    print(f"\n{'=' * 70}")
    print("  STEP 4: Detailed Comparison Results")
    print(f"{'=' * 70}")

    for pair in candidate_pairs:
        print_comparison_result(df, features, pair)

    # Step 5: Summary
    print(f"\n{'=' * 70}")
    print("  STEP 5: Summary - Identified Duplicates")
    print(f"{'=' * 70}")

    # Find likely duplicates (average score >= 0.7)
    likely_duplicates = []
    for pair in candidate_pairs:
        avg_score_val = features.loc[pair].values.mean()
        if avg_score_val >= 0.7:
            likely_duplicates.append((pair, avg_score_val))

    if likely_duplicates:
        print("\n  Likely duplicate pairs found:")
        for pair, score in likely_duplicates:
            rec1 = df.loc[pair[0]]
            rec2 = df.loc[pair[1]]
            print(f"\n    Records {pair[0]} & {pair[1]} (Score: {score:.2f})")
            print(f"      {rec1['first_name']} {rec1['last_name']} ({rec1['city']})")
            print(f"      {rec2['first_name']} {rec2['last_name']} ({rec2['city']})")
    else:
        print("\n  No likely duplicates found.")

    print(f"\n{'=' * 70}")
    print("                    KEY CONCEPTS")
    print(f"{'=' * 70}")
    print("""
  1. BLOCKING
     - Groups records by a common field (e.g., city)
     - Reduces number of comparisons dramatically
     - Trade-off: might miss duplicates in different blocks

  2. COMPARISON METHODS
     - Exact: 1 if identical, 0 otherwise
     - Jaro-Winkler: 0.0 to 1.0, good for names
     - Levenshtein: Edit distance based similarity
     - Soundex: Phonetic similarity

  3. SCORE INTERPRETATION
     - 0.9+: Very likely duplicate
     - 0.7-0.9: Possible duplicate, needs review
     - <0.7: Probably different records

  4. WORKFLOW
     Input Data → Blocking → Comparison → Classification → Merge/Review
""")


if __name__ == "__main__":
    main()
