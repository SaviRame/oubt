# Master Data Management (MDM) Architecture & Implementation Plan  
**Use Case:** NYC Taxi Trip Data  
**Tech Stack:** Amazon S3 · Apache Spark · Amazon Athena · Amazon Redshift · Amazon QuickSight  
**Architecture Style:** Medallion (Bronze / Silver / Gold)  
**Primary Goal:** Demonstrate enterprise-style MDM without breaking referential integrity

---

## 1. Background & Problem Statement

The NYC Taxi dataset is largely clean and well-structured. However, to demonstrate real-world **Master Data Management (MDM)** capabilities, we must show how an organization would:

- Detect and resolve duplicate master data
- Maintain a single version of truth (golden records)
- Track historical changes (SCD Type 2)
- Measure and monitor data quality
- Preserve transactional data integrity

This project intentionally **simulates master data issues** in staging layers while keeping transactional trip data immutable.

---

## 2. Objectives

### Functional Objectives
- Implement MDM for the following domains:
  - Vendor
  - Zone
  - Rate Code
  - Payment Type
- Demonstrate:
  - Deduplication & matching engine
  - Golden record creation
  - Survivorship rules
  - SCD Type 2 for attribute history
  - Identity resolution via crosswalks
  - Data Quality (DQ) measurement & dashboards

### Technical Objectives
- Use **S3-based Medallion Architecture** for trip data
- Use **Spark** for transformations, matching, and SCD logic
- Use **Athena** for lake-based exploration and validation
- Use **Redshift** as the serving layer
- Build **QuickSight dashboards** for analytics and governance

---

## 3. Core Design Principles

1. **Transactional data is immutable**
   - Trip data is never updated or rewritten
2. **Master data is governed, not overwritten**
   - Changes are tracked using SCD Type 2
3. **Identity ≠ Attributes**
   - Identity resolution (crosswalk) is separated from attribute history (golden SCD)
4. **Time-awareness**
   - Both identity mappings and attributes can change over time
5. **Auditability**
   - All match decisions and data quality checks are traceable

---

## 4. High-Level Architecture

```text
        ┌───────────────────┐
        │  NYC Taxi Sources │
        └─────────┬─────────┘
                  ▼
        ┌───────────────────┐
        │   S3 – Bronze     │  (Raw, Immutable)
        └─────────┬─────────┘
                  ▼
        ┌───────────────────┐
        │   Spark Jobs      │
        │  (Clean, Match,  │
        │   SCD, DQ)       │
        └─────────┬─────────┘
                  ▼
        ┌───────────────────┐
        │   S3 – Silver     │  (Standardized)
        └─────────┬─────────┘
                  ▼
        ┌───────────────────┐
        │   S3 – Gold       │  (Curated + MDM)
        └─────────┬─────────┘
                  ▼
        ┌───────────────────┐
        │   Amazon Redshift │  (Serving Layer)
        └─────────┬─────────┘
                  ▼
        ┌───────────────────┐
        │   QuickSight      │  (Dashboards)
        └───────────────────┘
```

---

## 5. Bronze Layer Schema Definitions

The Bronze layer contains raw, immutable master data reference tables. These tables serve as the foundation for MDM operations.

### 5.1 Zone Table

**Table Name:** `bronze.zone`

**Source:** `data/taxi_zone_lookup.csv`

**Purpose:** Stores TLC Taxi Zone reference data for pickup and dropoff locations.

**Schema:**

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| LocationID | INT | Unique identifier for each taxi zone | 1, 2, 3, ..., 265 |
| Borough | STRING | NYC borough where the zone is located | "EWR", "Queens", "Bronx", "Manhattan", "Staten Island", "Brooklyn", "Unknown", "N/A" |
| Zone | STRING | Specific zone name within the borough | "Newark Airport", "Jamaica Bay", "Alphabet City", "Central Park", etc. |
| service_zone | STRING | Service zone classification for taxi operations | "EWR", "Boro Zone", "Yellow Zone", "Airports", "N/A" |

**Key Characteristics:**
- Primary Key: `LocationID`
- Total Records: 265 zones
- Service Zone Categories:
  - **EWR**: Newark Airport zone
  - **Boro Zone**: Borough-specific zones outside Manhattan
  - **Yellow Zone**: Manhattan zones served by yellow taxis
  - **Airports**: JFK and LaGuardia Airport zones
  - **N/A**: Unknown or outside NYC

**Sample Data:**
```
LocationID | Borough    | Zone                          | service_zone
-----------|------------|-------------------------------|-------------
1          | EWR        | Newark Airport                | EWR
4          | Manhattan  | Alphabet City                 | Yellow Zone
43         | Manhattan  | Central Park                  | Yellow Zone
132        | Queens     | JFK Airport                   | Airports
138        | Queens     | LaGuardia Airport             | Airports
264        | Unknown    | N/A                           | N/A
265        | N/A        | Outside of NYC                | N/A
```

---

### 5.2 Rate Code Table

**Table Name:** `bronze.rate_code`

**Source:** `data/data_dictionary_trip_records_yellow.pdf`

**Purpose:** Stores rate code definitions for different fare structures.

**Schema:**

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| RatecodeID | INT | Unique identifier for rate code type | 1, 2, 3, 4, 5, 6, 99 |
| Description | STRING | Human-readable description of the rate code | "Standard rate", "JFK", "Newark", etc. |

**Key Characteristics:**
- Primary Key: `RatecodeID`
- Total Records: 7 rate codes
- Rate Code Definitions:
  - **1**: Standard rate - Regular metered fare
  - **2**: JFK - Flat rate to/from JFK Airport
  - **3**: Newark - Flat rate to/from Newark Airport
  - **4**: Nassau or Westchester - Trips to these counties
  - **5**: Negotiated fare - Pre-arranged negotiated price
  - **6**: Group ride - Shared ride service
  - **99**: Null/unknown - Missing or invalid rate code

**Sample Data:**
```
RatecodeID | Description
-----------|-------------
1          | Standard rate
2          | JFK
3          | Newark
4          | Nassau or Westchester
5          | Negotiated fare
6          | Group ride
99         | Null/unknown
```

---

### 5.3 Payment Type Table

**Table Name:** `bronze.payment_type`

**Source:** `data/data_dictionary_trip_records_yellow.pdf`

**Purpose:** Stores payment method classifications for taxi trips.

**Schema:**

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| payment_type | INT | Unique identifier for payment method | 0, 1, 2, 3, 4, 5, 6 |
| Description | STRING | Human-readable description of payment type | "Credit card", "Cash", "No charge", etc. |

**Key Characteristics:**
- Primary Key: `payment_type`
- Total Records: 7 payment types
- Payment Type Definitions:
  - **0**: Flex Fare trip - Flexible fare program
  - **1**: Credit card - Payment via credit card
  - **2**: Cash - Payment with cash
  - **3**: No charge - Complimentary trip
  - **4**: Dispute - Payment under dispute
  - **5**: Unknown - Payment method not specified
  - **6**: Voided trip - Trip was voided

**Sample Data:**
```
payment_type | Description
-------------|-------------
0            | Flex Fare trip
1            | Credit card
2            | Cash
3            | No charge
4            | Dispute
5            | Unknown
6            | Voided trip
```

---

### 5.4 Vendor Table

**Table Name:** `bronze.vendor`

**Source:** NYC TLC Data Dictionary (VendorID mappings)

**Purpose:** Stores taxi service provider (vendor) reference data for trip records.

**Schema:**

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| vendor_id | INT | Unique identifier for each taxi vendor | 1, 2, 6, 7 |
| vendor_name | STRING | Human-readable vendor name | "Creative Mobile Technologies, LLC", "Curb Mobility, LLC", "Myle Technologies, LLC", "Helix" |
| ingestion_date | DATE | Date when data was loaded into bronze layer | 2025-01-19 |

**Key Characteristics:**
- Primary Key: `vendor_id`
- Total Records: 4 vendors
- Vendor Definitions:
  - **1**: Creative Mobile Technologies, LLC - Technology provider for taxi fleets
  - **2**: Curb Mobility, LLC - Mobile app-based taxi service
  - **6**: Myle Technologies, LLC - Taxi technology solutions provider
  - **7**: Helix - Taxi service technology platform

**Sample Data:**
```
vendor_id | vendor_name                          | ingestion_date
----------|--------------------------------------|---------------
1         | Creative Mobile Technologies, LLC      | 2025-01-19
2         | Curb Mobility, LLC                   | 2025-01-19
6         | Myle Technologies, LLC                | 2025-01-19
7         | Helix                                | 2025-01-19
```

---

### 5.5 Bronze Layer Storage Format

**S3 Path Structure:**
```
s3://week-4-oubt/bronze/
├── zone/
│   └── ingestion_date=YYYY-MM-DD/
│       └── taxi_zone_lookup.csv
├── rate_code/
│   └── ingestion_date=YYYY-MM-DD/
│       └── rate_code_reference.csv
├── payment_type/
│   └── ingestion_date=YYYY-MM-DD/
│       └── payment_type_reference.csv
└── vendor/
    └── ingestion_date=YYYY-MM-DD/
        └── vendor_reference.csv
```

**Partitioning Strategy:**
- **Partition Column:** `ingestion_date` (STRING, format: YYYY-MM-DD)
- **Purpose:** Track when data was loaded into the bronze layer
- **Benefits:**
  - Enables time-based data retrieval and auditing
  - Supports incremental processing and reprocessing
  - Facilitates data lineage and governance
  - Optimizes query performance when filtering by ingestion date

**File Format:** CSV (Comma-Separated Values)
- First row contains column headers
- Quoted string fields
- UTF-8 encoding

**Athena Table Definitions (DDL):**

Moved to `demo/bronze-ddl.sql`.

**Partition Management Commands:**

Moved to `demo/bronze-ddl.sql`.

**Query Examples:**

```sql
-- Query latest ingestion
SELECT * FROM bronze.zone
WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM bronze.zone);

-- Query specific date range
SELECT * FROM bronze.rate_code
WHERE ingestion_date BETWEEN '2025-01-01' AND '2025-01-31';

-- Compare data across ingestion dates
SELECT
    ingestion_date,
    COUNT(*) as record_count
FROM bronze.zone
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;

-- Query vendor data
SELECT * FROM bronze.vendor
WHERE ingestion_date = '2025-01-19'
ORDER BY vendor_id;

-- Count vendors by ingestion date
SELECT
    ingestion_date,
    COUNT(*) as vendor_count
FROM bronze.vendor
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;
```

---
