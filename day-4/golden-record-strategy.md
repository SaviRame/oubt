# Golden Record Strategy for Taxi Zones

## Definition

A **Golden Record** is the single, authoritative, most accurate, and complete version of a master data entity created by merging duplicate records.

---

## Strategy Components

### 1. Survivorship Rules

Determines which value "wins" when duplicates have conflicting data:

| Field | Rule | Rationale |
|-------|------|-----------|
| `LocationID` | **MINIMUM** | Prefer original/earliest ID |
| `Borough` | **MOST_COMPLETE** | Non-null, longest value wins |
| `Zone` | **MOST_COMPLETE** | Non-null, longest value wins |
| `service_zone` | **MOST_FREQUENT** | Consensus/majority wins |

### 2. Available Survivorship Rules

| Rule | Description |
|------|-------------|
| `MINIMUM` | Take the smallest value |
| `MAXIMUM` | Take the largest value |
| `MOST_COMPLETE` | Prefer non-null, longest string |
| `MOST_FREQUENT` | Most common value in cluster |
| `MOST_RECENT` | Based on timestamp (if available) |
| `FIRST` | Keep first occurrence |
| `LAST` | Keep last occurrence |

### 3. Data Quality Scoring

```
Completeness Score = (Non-null fields / Total fields) × 100
```

**Example:**
- Record with 4/4 fields filled = 100% score
- Record with 3/4 fields filled = 75% score

---

## Algorithm

```
INPUT: Source dataset D, Match keys M, Survivorship rules S
OUTPUT: Golden dataset G with lineage

1. NORMALIZE all text fields (lowercase, trim whitespace)

2. CREATE match_key by concatenating M fields

3. GROUP records by match_key

4. FOR each group:
   IF group.size == 1:
      → Add record directly to G
   ELSE:
      → Create golden record:
         a. FOR each field F in record:
               Apply survivorship rule S[F]
               Track source record (lineage)
         b. Add merged record to G

5. RETURN G with lineage log
```

---

## Visual Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOURCE DATA (265 records)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: STANDARDIZATION                                        │
│  • Lowercase all text                                           │
│  • Trim whitespace                                              │
│  • Remove special characters                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: MATCHING                                               │
│  • Create composite key: Zone + Borough                         │
│  • Group records by key                                         │
│  • Identify duplicate clusters                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: SURVIVORSHIP                                           │
│  ┌─────────────┬─────────────┬─────────────┐                    │
│  │  Record A   │  Record B   │  GOLDEN     │                    │
│  ├─────────────┼─────────────┼─────────────┤                    │
│  │  ID: 56     │  ID: 57     │  ID: 56     │ ← MIN              │
│  │  Corona     │  Corona     │  Corona     │ ← COMPLETE         │
│  │  Queens     │  Queens     │  Queens     │ ← COMPLETE         │
│  │  Boro Zone  │  NULL       │  Boro Zone  │ ← FREQUENT         │
│  └─────────────┴─────────────┴─────────────┘                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 4: LINEAGE TRACKING                                       │
│  • Record source of each field value                            │
│  • Log alternative values discarded                             │
│  • Maintain audit trail                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   GOLDEN DATASET (262 records)                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Lineage Tracking Structure

For each merged record, capture:

```json
{
  "match_key": "corona|queens",
  "source_count": 2,
  "source_indices": [55, 56],
  "lineage": {
    "LocationID": {
      "value": 56,
      "rule": "MINIMUM",
      "alternatives": [56, 57]
    },
    "Borough": {
      "value": "Queens",
      "rule": "MOST_COMPLETE",
      "alternatives": ["Queens"]
    }
  }
}
```

---

## Expected Results for Taxi Zones

| Metric | Value |
|--------|-------|
| Source records | 265 |
| Unique entities | ~262 |
| Duplicates merged | ~3 |
| Golden records | ~262 |

### Known Duplicates to Merge:
1. **Corona** (LocationID 56, 57) → Golden ID: 56
2. **Governor's Island/Ellis Island/Liberty Island** (103, 104, 105) → Golden ID: 103

---

## Implementation Checklist

- [ ] Implement survivorship rule functions
- [ ] Create matching/blocking logic
- [ ] Build golden record creation function
- [ ] Add lineage tracking
- [ ] Export golden dataset to CSV
- [ ] Generate merge report
