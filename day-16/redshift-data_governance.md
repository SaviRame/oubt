# Data Governance & Derivation Logic

## 1. Purpose
This document describes the **data lineage, derivation logic, data quality rules, and source-of-truth decisions** for the NYC Yellow Taxi analytics data model implemented in Amazon Redshift.  
It serves as a **governance artifact** to ensure transparency, auditability, and reproducibility of analytical data.

---

## 2. Data Sources

### 2.1 Authoritative Sources
- **NYC TLC Yellow Taxi Trip Records**  
  - Source: Public NYC TLC datasets stored in Amazon S3  
  - Reference: *NYC TLC Yellow Taxi Trip Records Data Dictionary (March 18, 2025)*  
  - Authority: Official source of trip-level transactional data

- **NYC TLC Taxi Zones**  
  - Source: NYC TLC Taxi Zone lookup data  
  - Authority: Golden reference for pickup and dropoff zones

### 2.2 Enrichment Sources
- **Vendor Company Details**  
  - Source: Given data definition file, Publicly available information gathered via Google search and some fake data 
  - Purpose: Analytical enrichment only (non-authoritative)

---

## 3. Data Lineage

```text
S3 (NYC TLC Trip Records)
        ↓
staging.stg_taxi_trips
        ↓
mdm.vendor_dim   mdm.zone_dim
        ↓
analytics.fact_taxi_trips
        ↓
analytics reports
```

---

## 4. Dimension Tables Governance

### 4.1 Zone Dimension (`mdm.zone_dim`)

**Source:** `staging.stg_taxi_zones`  
**Nature:** Golden reference (static master data)

| Column | Source | Derivation Logic |
|------|------|------------------|
| `zone_sk` | System-generated | Redshift surrogate key (IDENTITY) |
| `zone_id` | `LocationID` | Natural/business key; must be unique |
| `borough` | TLC zones file | Direct mapping |
| `zone_name` | TLC zones file | Direct mapping |
| `service_zone` | TLC zones file | Direct mapping |

**Data Quality Rules:**
- `zone_id` must be non-null and unique
- `zone_name` and `borough` must be non-null

---

### 4.2 Vendor Dimension (`mdm.vendor_dim`)

**Sources:**
- **Authoritative:** NYC TLC Data Dictionary (VendorID codes)
- **Enriched:** Public web sources (Google)

| Column | Source | Derivation Logic |
|------|------|------------------|
| `vendor_sk` | System-generated | Redshift surrogate key (IDENTITY) |
| `vendor_id` | `stg_taxi_trips.vendorid` | Business key defined by NYC TLC |
| `vendor_name` | NYC TLC Data Dictionary | VendorID mapping (1,2,6,7) |
| `company_name` | Google | Manual enrichment |
| `address` | Google | Manual enrichment |
| `city` | Google | Manual enrichment |
| `state` | Google | Manual enrichment |
| `phone` | Google | Manual enrichment |
| `license_number` | Google | Optional enrichment |

**Disclaimer:**  
Vendor identifiers and vendor names are sourced from the official NYC TLC Data Dictionary. Additional vendor attributes (company details, addresses, and contact information) are enriched from publicly available sources and are intended for analytical context only. These enriched fields are **not authoritative** and should not be used for regulatory or compliance purposes.

---

## 5. Fact Table Governance

### 5.1 Fact Table (`analytics.fact_taxi_trips`)

**Source:** `staging.stg_taxi_trips`  
**Grain:** One record per taxi trip

| Column | Source | Derivation Logic |
|------|------|------------------|
| `trip_id` | System-generated | Surrogate primary key |
| `vendor_sk` | `vendor_dim` | Lookup on `vendor_id` |
| `pickup_zone_sk` | `zone_dim` | Lookup on `pulocationid` |
| `dropoff_zone_sk` | `zone_dim` | Lookup on `dolocationid` |
| `tpep_pickup_datetime` | Staging | Direct mapping |
| `tpep_dropoff_datetime` | Staging | Direct mapping |
| `pickup_date` | Pickup timestamp | `CAST(tpep_pickup_datetime AS DATE)` |
| `trip_duration_minutes` | Pickup & dropoff timestamps | `DATEDIFF(second)/60` |
| Monetary fields | Staging | Direct mapping |
| Code fields | Staging | Validated against allowed domains |

---

## 6. Data Quality Rules (Fact Load)

### 6.1 Referential Integrity
- `vendor_id` must exist in `vendor_dim`
- `pulocationid` and `dolocationid` must exist in `zone_dim`

### 6.2 Temporal Validations
- Pickup time ≤ dropoff time
- No future timestamps
- Pickup timestamp ≥ dataset lower bound

### 6.3 Domain Rules
- Passenger count: 0–6
- Trip distance: >0 and ≤100 miles
- Fare amount: >0
- No negative monetary values
- Valid code values for rate code, payment type, and store-and-forward flag

Records failing any rule are excluded from the fact table.

---

## 7. Rejection Handling & Reconciliation

- Records failing data quality or referential rules are rejected during fact loading
- Rejected row counts are reconciled against staging vs fact totals
- Rejection reasons can be categorized for audit and reporting
- 3,226,666 (eligible)
- + 347,425 (rejected)
- = 3,574,091 (staging)

|Status |	Count  |	Interpretation|
|------|------|------------------|
|ELIGIBLE |	3,226,666  |	Correctly loaded into fact|
|FARE_NON_POSITIVE |	241,817	 |Adjustments / zero-fare / refunds |
|DISTANCE_OUT_OF_RANGE |	105,429	 | Extreme / corrupted distance values
|EXTRA_NEGATIVE	168	|  True data errors |
|PASSENGER_OUT_OF_RANGE	9	| True data errors |
|INVALID_TIMESTAMP_ORDER   |	2 |	True data errors|

---


## 8. Refresh & Ownership

| Item | Value |
|----|------|
| Data Owner | Analytics Engineering |
| Refresh Frequency | Batch (daily / on-demand) |
| Load Method | S3 → Redshift COPY |
| Data Model | Star schema |

---

## 9. Summary
This governance document ensures that all analytical data derived from NYC Yellow Taxi trip records is **traceable, validated, and transparent**, with a clear distinction between authoritative data and enriched attributes.
