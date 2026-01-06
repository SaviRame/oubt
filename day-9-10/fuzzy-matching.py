#!/usr/bin/env python3
"""
Simple Fuzzy Matching Tutorial using FuzzyWuzzy

This beginner-friendly script demonstrates basic fuzzy string matching
using the fuzzywuzzy library.

To install: pip install fuzzywuzzy python-Levenshtein
"""

from fuzzywuzzy import fuzz


def compare_strings(str1: str, str2: str) -> dict:
    """
    Compare two strings using different fuzzy matching methods.

    Args:
        str1: First string to compare
        str2: Second string to compare

    Returns:
        Dictionary with scores from each method (0-100, higher = more similar)
    """
    return {
        "ratio": fuzz.ratio(str1, str2),
        "partial_ratio": fuzz.partial_ratio(str1, str2),
        "token_sort_ratio": fuzz.token_sort_ratio(str1, str2),
        "token_set_ratio": fuzz.token_set_ratio(str1, str2),
    }


def print_comparison(str1: str, str2: str, description: str) -> None:
    """Print a formatted comparison between two strings."""
    scores = compare_strings(str1, str2)

    print(f"\n{'=' * 60}")
    print(f"Test: {description}")
    print(f"{'=' * 60}")
    print(f'String 1: "{str1}"')
    print(f'String 2: "{str2}"')
    print("\nScores:")
    print(
        f"  ratio():            {scores['ratio']:3d}  - Basic character-by-character comparison"
    )
    print(
        f"  partial_ratio():    {scores['partial_ratio']:3d}  - Best match of shorter in longer string"
    )
    print(
        f"  token_sort_ratio(): {scores['token_sort_ratio']:3d}  - Sorts words before comparing"
    )
    print(
        f"  token_set_ratio():  {scores['token_set_ratio']:3d}  - Compares unique word sets"
    )


def main():
    """Run example test cases to demonstrate fuzzy matching."""

    print("\n" + "=" * 60)
    print("       FUZZY MATCHING TUTORIAL WITH FUZZYWUZZY")
    print("=" * 60)

    # Test Case 1: Exact match
    print_comparison("Yellow Cab Company", "Yellow Cab Company", "Exact Match")

    # Test Case 2: Abbreviation
    print_comparison(
        "Yellow Cab Co", "Yellow Cab Company", "Abbreviation (Co vs Company)"
    )

    # Test Case 3: Typo
    print_comparison("Yelow Cab", "Yellow Cab", "Typo (missing 'l')")

    # Test Case 4: Different word order
    print_comparison("Cab Yellow Company", "Yellow Cab Company", "Different Word Order")

    # Test Case 5: Address variation
    print_comparison(
        "123 Main St", "123 Main Street", "Address Abbreviation (St vs Street)"
    )

    # Test Case 6: Partial match
    print_comparison("Apple", "Apple Inc.", "Partial Match (substring)")

    print("\n" + "=" * 60)
    print("                    SUMMARY")
    print("=" * 60)
    print("""
When to use each method:

• ratio() 
  - Basic comparison, good for similar-length strings
  - Use for: Simple typo detection

• partial_ratio()
  - Finds best match of shorter string in longer
  - Use for: Abbreviations, substrings

• token_sort_ratio()
  - Ignores word order
  - Use for: "John Smith" vs "Smith, John"

• token_set_ratio()
  - Compares unique words, ignores duplicates
  - Use for: Extra words, "The Yellow Cab" vs "Yellow Cab Company"

Score interpretation:
  100 = Perfect match
  90+ = Very similar (likely same entity)
  80+ = Similar (probably same, review recommended)
  <80 = Different (likely different entities)
""")


if __name__ == "__main__":
    main()
