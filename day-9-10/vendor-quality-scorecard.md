# Vendor MDM Quality Scorecard

**Generated:** 2026-01-06 17:22 UTC
**Data Source:** NYC Taxi TPEP Vendor Master Data
**Domain:** Vendor Management

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Overall Health** | ❌ CRITICAL |
| **Health Description** | High manual intervention required, significant data quality issues |
| **Total Vendor Records** | 18 |
| **Potential Duplicate Pairs** | 20 |
| **Duplicate Rate** | 77.8% |

---

## Match Confidence Distribution

| Status | Count | Percentage | Description |
|--------|-------|------------|-------------|
| ✅ `auto_merge` | 12 | 60.0% | High confidence - can be merged automatically |
| ⚠️ `Steward_review` | 2 | 10.0% | Medium confidence - requires human review |
| ❌ `manual_resolution_needed` | 6 | 30.0% | Low confidence - complex investigation needed |

---

## Business Impact Analysis

### Financial Impact

| Issue | Count | Business Impact | Estimated Risk |
|-------|-------|-----------------|----------------|
| Duplicate vendor records | 20 pairs | Potential duplicate payments | **High** - Each duplicate could result in overpayment |
| Missing Tax IDs | 2 records | 1099 reporting compliance risk | **Critical** - IRS penalties possible |
| Missing License Numbers | 1 records | Regulatory compliance risk | **Medium** - TLC audit findings |

### Operational Impact

| Metric | Value | Impact |
|--------|-------|--------|
| Auto-merge ready | 12 pairs | **60 minutes saved** (vs manual review) |
| Steward review queue | 2 pairs | **30 minutes** of steward time required |
| Manual investigation | 6 pairs | **180 minutes** of analyst time required |
| **Total Processing Time** | - | **270 minutes** estimated |

### Data Quality Dimensions

| Dimension | Score | Status | Business Impact |
|-----------|-------|--------|-----------------|
| **Uniqueness** | 22.2% | ❌ | Duplicate vendors affect payment accuracy |
| **Completeness (Tax ID)** | 88.9% | ⚠️ | Missing Tax IDs block 1099 reporting |
| **Completeness (License)** | 94.4% | ⚠️ | Missing licenses risk regulatory fines |

---

## Steward Review Queue

| # | Vendor Pair | Score | Priority |
|---|-------------|-------|----------|
| 1 | HELIX vs Helix | 0.82 | Low |
| 2 | Curb Mobility vs Curb Mobility, L.L.C. | 0.93 | High |
