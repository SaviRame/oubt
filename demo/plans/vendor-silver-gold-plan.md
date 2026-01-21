# Vendor Silver to Gold MDM Transformation Plan

## Overview

This document outlines a practical silver-to-gold transformation for the vendor MDM table: SCD Type 2 tracking plus a simple duplicate detection approach designed for demos / very low volume (convert to pandas, use `recordlinkage` on all pairs, then convert back to Spark).

## Architecture Flow

```mermaid
flowchart TD
    A[Silver Layer<br/>vendor table] --> B[Read Silver Data<br/>for ingestion_date]
    C[Gold Layer<br/>dim_vendor & xref_vendor_scd2] --> D[Read Existing Gold Data]
    B --> E[Generate Record Hash]
    D --> E
    E --> F[Candidate Matching<br/>pandas + recordlinkage (all pairs)]
    F --> G[Match Confidence Scoring<br/>Jaro-Winkler similarity]
    G --> H{Confidence Threshold}
    H -->|> 95%| I[Auto Approve]
    H -->|85-95%| J[Steward Review]
    H -->|75-85%| K[Manual Review]
    H -->|< 75%| L[No Match - New Golden Record]
    I --> M[Apply Survivorship Rules<br/>Longest Name]
    J --> M
    K --> M
    L --> M
    M --> N[SCD Type 2 Logic<br/>Track Attribute Changes]
    N --> O[Generate Crosswalk Mappings]
    O --> P[Write dim_vendor]
    O --> Q[Write xref_vendor_scd2]
```

## Data Model

### Input: Silver Layer Table

**Table:** `{silver_db}.vendor`

| Column | Type | Description |
|--------|------|-------------|
| vendor_id | INT | Source business key |
| vendor_name | STRING | Original vendor name |
| ingestion_date | DATE | Date of ingestion |
| normalized_name | STRING | Normalized name for matching |

### Output: Gold Layer Tables

#### 1. Golden Dimension Table (SCD Type 2)

**Table:** `{gold_db}.dim_vendor`

| Column | Type | Description |
|--------|------|-------------|
| vendor_gk | BIGINT | Golden Key (surrogate key) |
| canonical_name | STRING | Surviving attribute (longest name) |
| valid_from | DATE | SCD start date |
| valid_to | DATE | SCD end date (NULL for current) |
| is_current | BOOLEAN | Current flag |
| change_reason | STRING | NEW / RENAME / MERGE / CORRECTION |
| record_hash | STRING | Hash for change detection |

#### 2. Crosswalk Table (Identity Resolution)

**Table:** `{gold_db}.xref_vendor_scd2`

| Column | Type | Description |
|--------|------|-------------|
| vendor_id | INT | Source business key |
| vendor_gk | BIGINT | Golden Key reference |
| valid_from | DATE | Mapping start date |
| valid_to | DATE | Mapping end date (NULL for current) |
| is_current | BOOLEAN | Active mapping flag |
| match_rule | STRING | Rule applied (EXACT / RECORDLINKAGE) |
| match_confidence | DECIMAL(5,2) | Match score (0.00-1.00) |
| decision | STRING | AUTO / STEWARD_REVIEW / MANUAL_REVIEW / NO_MATCH |

## Implementation Components

### 1. Argument Parsing

```python
from awsglue.utils import getResolvedOptions

def get_glue_args_gold(argv):
    # Keep this separate so existing bronze/silver jobs don't suddenly require "gold_db".
    return getResolvedOptions(
        argv,
        ["JOB_NAME", "silver_db", "gold_db", "ingestion_date", "write_mode"],
    )

def parse_args(argv):
    args = get_glue_args_gold(argv)
    silver_table = f"{args['silver_db']}.vendor"
    gold_dim_table = f"{args['gold_db']}.dim_vendor"
    gold_xref_table = f"{args['gold_db']}.xref_vendor_scd2"
    ingestion_date = args["ingestion_date"]
    return args, silver_table, gold_dim_table, gold_xref_table, ingestion_date
```

**Note:** This avoids changing `demo/glue_utils.py`, which may be shared by other jobs.

### 2. Record Hash Generation

Generate a hash of the normalized name for change detection:

```python
def generate_record_hash(df):
    return df.withColumn(
        "record_hash",
        F.sha2(F.concat_ws("|", F.col("normalized_name")), 256)
    )
```

### 3. Duplicate Detection (Simple + Library-Based)

For a demo with very low volume (e.g. < 10 records), the simplest path is:

1. Convert the Spark DataFrame to pandas with `toPandas()`.
2. Use `recordlinkage` to score **all pairs** (no blocking).
3. Build duplicate groups (connected components) so groups can contain 2+ records.
4. Convert the group assignments and scores back to Spark.

### 4. Match Confidence Thresholds

| Confidence Range | Decision | Action |
|------------------|----------|--------|
| > 0.95 | AUTO | Automatically approve match |
| 0.85 - 0.95 | STEWARD_REVIEW | Flag for steward review |
| 0.75 - 0.85 | MANUAL_REVIEW | Flag for manual review |
| < 0.75 | NO_MATCH | Create new golden record |

### 5. Survivorship Rules

**Rule:** Select the vendor with the longest `vendor_name` as the golden record.

```python
def apply_survivorship_rules(matched_df):
    """
    Apply survivorship rules to select golden records.
    Rule: Longest vendor_name wins.
    """
    window = Window.partitionBy("match_group").orderBy(F.desc(F.length("vendor_name")))
    return matched_df.withColumn("rank", F.row_number().over(window)) \
                     .filter(F.col("rank") == 1) \
                     .drop("rank")
```

### 6. SCD Type 2 Logic

**Process:**

1. **New Records:** Create new golden record with `valid_from = ingestion_date`, `valid_to = NULL`, `is_current = TRUE`

2. **Attribute Changes:** 
   - Close old record: `valid_to = ingestion_date - 1 day`, `is_current = FALSE`
   - Create new record: `valid_from = ingestion_date`, `valid_to = NULL`, `is_current = TRUE`

3. **No Changes:** Keep existing record unchanged

```python
def apply_scd_type_2(new_records, existing_gold, ingestion_date):
    """
    Apply SCD Type 2 logic to handle attribute changes.
    """
    # Detect changes by comparing record_hash
    changed_records = detect_changes(new_records, existing_gold)
    
    # Close old records that have changed
    closed_records = close_old_records(changed_records, ingestion_date)
    
    # Create new records for changes and new vendors
    active_records = create_new_records(new_records, existing_gold, ingestion_date)
    
    # Combine: unchanged existing + closed + new
    final_gold = combine_records(existing_gold, closed_records, active_records)
    
    return final_gold
```

### 7. Crosswalk Generation

Create identity resolution mappings:

```python
def generate_crosswalk(vendor_scored, group_to_gk, ingestion_date):
    """
    Generate crosswalk mappings between vendor_id and vendor_gk.
    Note: crosswalk needs one row per source vendor_id (not just the surviving golden record).
    """
    return (
        vendor_scored.join(group_to_gk, on="match_group", how="left")
        .select(
            F.col("vendor_id"),
            F.col("vendor_gk"),
            F.lit(ingestion_date).cast("date").alias("valid_from"),
            F.lit(None).cast("date").alias("valid_to"),
            F.lit(True).alias("is_current"),
            F.col("match_rule"),
            F.col("match_confidence"),
            F.col("decision"),
        )
    )
```

## Processing Flow

### Step 1: Read Input Data

```python
# Read silver data for specific ingestion date
silver_df = spark.read.table(silver_table).filter(
    F.col("ingestion_date") == F.lit(ingestion_date).cast("date")
)

# Read all existing gold data
existing_dim = spark.read.table(gold_dim_table) if spark.catalog.tableExists(gold_dim_table) else None
existing_xref = spark.read.table(gold_xref_table) if spark.catalog.tableExists(gold_xref_table) else None
```

### Step 2: Preprocess Silver Data

```python
# Generate record hash for change detection
silver_df = generate_record_hash(silver_df)
```

### Step 3: Generate Matching Candidates

**Objective:** Identify potential duplicate vendors with the simplest possible code for low volume.

**Approach:**

1. **Spark → pandas (driver):**
   ```python
   pdf = (
       silver_df.select("vendor_id", "vendor_name", "normalized_name", "record_hash")
       .dropna(subset=["normalized_name"])
       .toPandas()
       .set_index("vendor_id")
   )
   ```

2. **Score all pairs with `recordlinkage` (no blocking):**
   ```python
   import recordlinkage as rl

   indexer = rl.Index()
   indexer.full()  # all pairs
   pairs = indexer.index(pdf)  # MultiIndex: (vendor_id_1, vendor_id_2)

   compare = rl.Compare()
   compare.string("normalized_name", "normalized_name", method="jarowinkler", label="match_confidence")
   features = compare.compute(pairs, pdf)  # DataFrame indexed by (vendor_id_1, vendor_id_2)

   pairs_df = (
       features.reset_index()
       .rename(columns={"level_0": "vendor_id_1", "level_1": "vendor_id_2"})
       .sort_values(["match_confidence", "vendor_id_1", "vendor_id_2"], ascending=[False, True, True])
   )
   ```

3. **Build duplicate groups that support 2+ duplicates (connected components):**
   ```python
   # Keep only "link" edges at/above the lowest match tier
   edges = pairs_df[pairs_df["match_confidence"] >= 0.75][["vendor_id_1", "vendor_id_2"]]

   # Union-Find / Disjoint Set to form connected components
   parent = {int(v): int(v) for v in pdf.index.tolist()}

   def find(x):
       while parent[x] != x:
           parent[x] = parent[parent[x]]
           x = parent[x]
       return x

   def union(a, b):
       ra, rb = find(a), find(b)
       if ra != rb:
           parent[rb] = ra

   for a, b in edges.itertuples(index=False):
       union(int(a), int(b))

   groups_df = (
       pdf.reset_index()[["vendor_id"]]
       .assign(match_group=lambda d: d["vendor_id"].map(lambda v: find(int(v))))
   )
   ```

4. **Compute per-record confidence + rule (for crosswalk metadata):**
   ```python
   # Best score per vendor across all pairs
   best_left = pairs_df.groupby("vendor_id_1")["match_confidence"].max()
   best_right = pairs_df.groupby("vendor_id_2")["match_confidence"].max()
   best = best_left.combine(best_right, max).fillna(0.0)

   metrics_df = groups_df.assign(
       match_confidence=groups_df["vendor_id"].map(best).fillna(0.0),
       match_rule=lambda d: d["match_confidence"].map(lambda c: "EXACT" if c == 1.0 else "RECORDLINKAGE"),
   )
   ```

5. **pandas → Spark:**
   ```python
   metrics_sdf = spark.createDataFrame(metrics_df)
   silver_scored = silver_df.join(metrics_sdf, on="vendor_id", how="left")
   ```

**Output:** `silver_scored` with `match_group`, `match_confidence`, and `match_rule` ready for thresholding and survivorship.

### Step 4: Apply Match Thresholds

**Objective:** Convert per-record confidence into an action tier.

**Classification Logic (Spark expressions, no UDF):**

```python
silver_scored = silver_scored.withColumn(
    "decision",
    F.when(F.col("match_confidence") > 0.95, F.lit("AUTO"))
     .when(F.col("match_confidence") >= 0.85, F.lit("STEWARD_REVIEW"))
     .when(F.col("match_confidence") >= 0.75, F.lit("MANUAL_REVIEW"))
     .otherwise(F.lit("NO_MATCH")),
)
```

**Note:** For this demo approach, transitive duplicates are handled by union-find connected components in Step 3 (A~B and B~C implies A/B/C share a `match_group`).

### Step 5: Apply Survivorship Rules

```python
# Select golden records using longest name rule
golden_records = apply_survivorship_rules(silver_scored)
```

### Step 6: Apply SCD Type 2 Logic

```python
# Handle attribute changes and new records
final_dim_vendor = apply_scd_type_2(golden_records, existing_dim, ingestion_date)
```

### Step 7: Generate Crosswalk

```python
# Create identity resolution mappings
group_to_gk = golden_records.select("match_group", "vendor_gk")
final_xref_vendor = generate_crosswalk(silver_scored, group_to_gk, ingestion_date)
```

### Step 8: Write to Gold Layer

```python
# Write dim_vendor table
write_gold_table(final_dim_vendor, gold_dim_table, spark)

# Write xref_vendor_scd2 table
write_gold_table(final_xref_vendor, gold_xref_table, spark)
```


## Key Design Decisions

1. **Incremental Processing:** Process only the specific ingestion_date from silver, but read all data from gold for SCD Type 2 comparison.

2. **Fuzzy Matching (Demo):** Use `recordlinkage` Jaro-Winkler similarity on `normalized_name` and score all pairs in pandas; group duplicates with union-find connected components.

3. **Survivorship:** Longest name rule ensures the most descriptive name survives as the canonical name.

4. **SCD Type 2:** Full history tracking allows for historical analytics and auditability.

5. **Crosswalk Separation:** Identity resolution is separated from attribute history, allowing for flexible match corrections.

6. **Match Confidence Tiers:** Four-tier confidence system provides appropriate human oversight for ambiguous matches.

## Testing Considerations

1. **Unit Tests:**
   - Fuzzy matching accuracy
   - Survivorship rule application
   - SCD Type 2 logic
   - Record hash generation

2. **Integration Tests:**
   - End-to-end silver to gold flow
   - Incremental updates
   - Crosswalk consistency

3. **Data Quality Checks:**
   - No orphaned vendor_ids in crosswalk
   - All current records have valid_to = NULL
   - No overlapping date ranges in SCD records
   - Match confidence values in valid range (0.0-1.0)

## Performance Considerations

1. **Caching:** Cache silver data and existing gold data during processing.

2. **Partitioning:** Consider partitioning gold tables by `valid_from` or `is_current` for query performance.

3. **Indexing:** Delta Lake statistics and Z-ORDER BY on frequently queried columns.

4. **Batch Size:** Process matching in batches if dealing with large datasets.
