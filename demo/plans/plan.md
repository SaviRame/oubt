## 5. S3 Data Lake Design (Medallion)

### 5.1 Bronze Layer – Raw & Immutable

**Trips**

```text
s3://data-lake/bronze/trips/
  └── ingest_date=YYYY-MM-DD/
```

**MDM Raw Domains**

```text
s3://data-lake/bronze/mdm/vendor/
s3://data-lake/bronze/mdm/zone/
s3://data-lake/bronze/mdm/rate_code/
s3://data-lake/bronze/mdm/payment_type/
```

**Rules**

- Append-only
- No deletes or updates
- Full lineage preserved

### 5.2 Silver Layer – Standardized & Conformed

**Trips**

```text
s3://data-lake/silver/trips/
  └── trip_date=YYYY-MM-DD/
```

**Actions**

- Schema enforcement
- Type casting
- Null handling
- Column normalization

**MDM Staging**

```text
s3://data-lake/silver/mdm/vendor/
```

**Enhancements**

- Normalized attributes (e.g., `name_norm`)
- Tokenized names
- Hash columns for change detection

### 5.3 Gold Layer – Curated & Mastered

**Trips**

```text
s3://data-lake/gold/trips_fact/
```

**(Optional enrichment)**

```text
s3://data-lake/gold/trips_fact_enriched/
```

**MDM Outputs**

```text
s3://data-lake/gold/mdm/
  ├── dim_vendor/
  ├── xref_vendor/
```

## 6. MDM Logical Data Model

### 6.1 Golden Dimension (SCD Type 2)

**Table:** `dim_vendor`

| Column | Description |
| --- | --- |
| `vendor_gk` | Golden Key (surrogate) |
| `canonical_name` | Surviving attribute |
| `valid_from` | SCD start date |
| `valid_to` | SCD end date |
| `is_current` | Current flag |
| `change_reason` | Rename / merge / correction |
| `record_hash` | Change detection |

**Purpose**

- Track attribute history
- Support historical analytics
- Preserve official name changes

### 6.2 Crosswalk (Identity Resolution)

**Table:** `xref_vendor_scd2`

| Column | Description |
| --- | --- |
| `vendor_id` | Source business key |
| `vendor_gk` | Golden Key |
| `valid_from` | Mapping start |
| `valid_to` | Mapping end |
| `is_current` | Active mapping |
| `match_rule` | Rule applied |
| `match_confidence` | Match score |
| `decision` | AUTO / REVIEW |

**Purpose**

- Preserve referential integrity
- Allow correction of match decisions
- Maintain historical identity resolution


## 7. Spark Processing Plan

### Job 1 – Trip Ingestion (Bronze → Silver)

- Enforce schema
- Normalize timestamps
- Basic DQ checks (nulls, ranges)
- Write Delta

### Job 2 – MDM Standardization (Bronze → Silver)

- Normalize names/descriptions
- Generate matching attributes

### Job 3 – Matching & Deduplication & scd

- Candidate generation
- Exact + fuzzy scoring
- Duplicate clustering
- Survivorship rule application

**Output**

- Golden records
- Crosswalk mappings
- Match evidence
- scd



