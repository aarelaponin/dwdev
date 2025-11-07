# L2 → L3 Data Mapping Documentation

## Overview

This document maps the normalized MySQL Layer 2 (TA-RDM) schema to the dimensional ClickHouse Layer 3 (ta_dw) schema.

- **Layer 2**: Normalized operational database (MySQL, 12 schemas)
- **Layer 3**: Dimensional data warehouse (ClickHouse, star schema)

---

## dim_party Mapping

### Source Tables (L2 MySQL)

1. **party.party** - Main party table
2. **party.individual** - Individual-specific attributes
3. **compliance_control.taxpayer_risk_profile** - Risk assessment data
4. **reference.ref_gender** - Gender lookup
5. **reference.ref_marital_status** - Marital status lookup
6. **reference.ref_legal_form** - Legal form lookup
7. **reference.ref_industry** - Industry lookup
8. **reference.ref_country** - Country lookup
9. **reference.ref_region** - Region/province lookup
10. **reference.ref_district** - District lookup
11. **reference.ref_locality** - City/locality lookup

### Target Table (L3 ClickHouse)

**dim_party** - Party dimension with SCD Type 2

---

## Column Mapping: dim_party

| L3 Column | L3 Type | L2 Source | Transformation | Notes |
|-----------|---------|-----------|----------------|-------|
| **Surrogate Key** |
| party_key | Int64 | Generated | Auto-increment or max+1 | Data warehouse surrogate key |
| **Natural Key** |
| party_id | Int64 | party.party.party_id | Direct copy | Natural key from source |
| **Party Identification** |
| tin | String | party.party.tax_identification_number OR party.individual.tax_identification_number | Coalesce(party.tin, individual.tin) | Tax Identification Number |
| party_type | LowCardinality(String) | party.party.party_type_code | UPPER(party_type_code) | INDIVIDUAL or ENTERPRISE |
| party_name | String | party.party.party_name | Direct copy | Full legal name |
| party_status | LowCardinality(String) | party.party.party_status_code | COALESCE(status_code, 'ACTIVE') | ACTIVE, INACTIVE, etc. |
| party_segment | LowCardinality(String) | Derived | Business logic based on turnover | Large/Medium/Small/Micro |
| **Industry Classification** |
| industry_code | LowCardinality(String) | party.party.industry_code | Direct copy or NULL | ISIC industry code |
| industry_name | String | reference.ref_industry.industry_name | JOIN on industry_code | Industry description |
| **Risk Assessment** |
| risk_rating | LowCardinality(String) | compliance_control.taxpayer_risk_profile.risk_rating_code | COALESCE(risk_rating, 'UNKNOWN') | VERY_LOW, LOW, MEDIUM, HIGH, VERY_HIGH |
| risk_score | Decimal(5,2) | compliance_control.taxpayer_risk_profile.overall_risk_score | COALESCE(score, 0.0) | Numeric score 0-100 |
| **Individual Demographics** |
| birth_date | Date | party.individual.birth_date | Cast to Date | For individuals only |
| gender | LowCardinality(Nullable(String)) | party.individual.gender_code | UPPER(gender_code) | MALE, FEMALE, OTHER |
| **Enterprise Attributes** |
| legal_form | LowCardinality(Nullable(String)) | party.party.legal_form_code | Direct copy | LLC, CORPORATION, etc. |
| registration_date | Nullable(Date) | party.party.registration_date | Cast to Date | Business registration date |
| employee_count | Nullable(Int32) | NULL (Phase A) | Future: from enterprise table | Number of employees |
| annual_turnover | Nullable(Decimal(19,2)) | NULL (Phase A) | Future: from financial data | Annual revenue |
| **Address Information** |
| address_line1 | String | COALESCE(address, 'Unknown') | Future: from party_address table | Primary street address |
| address_line2 | Nullable(String) | NULL (Phase A) | Future: from party_address table | Secondary address line |
| city | String | reference.ref_locality.locality_name | JOIN or 'Unknown' | City/Town |
| district | String | reference.ref_district.district_name | JOIN or 'Unknown' | District/Province |
| postal_code | Nullable(String) | NULL (Phase A) | Future: from party_address table | Postal/ZIP code |
| country | String | reference.ref_country.iso_alpha_2 | JOIN or 'LK' (default) | ISO 3166-1 country code |
| **Contact Information** |
| email | Nullable(String) | NULL (Phase A) | Future: from party_contact table | Primary email |
| phone | Nullable(String) | NULL (Phase A) | Future: from party_contact table | Primary phone |
| mobile | Nullable(String) | NULL (Phase A) | Future: from party_contact table | Mobile phone |
| **SCD Type 2 Fields** |
| valid_from | DateTime | NOW() | Current timestamp for initial load | Effective start date |
| valid_to | Nullable(DateTime) | NULL | NULL for current records | Effective end date |
| is_current | UInt8 | 1 | Always 1 for initial load | 1=current, 0=historical |
| **Audit Fields** |
| created_by | String | 'ETL_SYSTEM' | Constant | ETL process identifier |
| created_date | DateTime | NOW() | Current timestamp | Record creation timestamp |
| updated_by | Nullable(String) | NULL | NULL for initial load | Last update user |
| updated_date | Nullable(DateTime) | NULL | NULL for initial load | Last update timestamp |
| **ETL Tracking** |
| etl_batch_id | Int64 | Generated | Batch sequence number | ETL batch identifier |

---

## SQL Query for Phase A Data Extraction

```sql
-- Extract party data from L2 MySQL for L3 load
SELECT
    p.party_id,
    COALESCE(p.tax_identification_number, i.tax_identification_number) AS tin,
    UPPER(p.party_type_code) AS party_type,
    p.party_name,
    COALESCE(p.party_status_code, 'ACTIVE') AS party_status,

    -- Industry (may be NULL in Phase A)
    p.industry_code,
    ind.industry_name,

    -- Risk assessment
    COALESCE(r.risk_rating_code, 'UNKNOWN') AS risk_rating,
    COALESCE(r.overall_risk_score, 0.0) AS risk_score,

    -- Individual demographics (NULL for enterprises)
    i.birth_date,
    UPPER(i.gender_code) AS gender,

    -- Enterprise attributes
    p.legal_form_code AS legal_form,
    p.registration_date,

    -- Location (using reference data)
    COALESCE(c.iso_alpha_2, 'LK') AS country,
    COALESCE(d.district_name, 'Unknown') AS district,
    COALESCE(l.locality_name, 'Unknown') AS city,

    -- Audit timestamps
    p.created_date,
    p.modified_date

FROM party.party p

-- Left join individual subtype
LEFT JOIN party.individual i ON p.party_id = i.party_id

-- Left join risk profile
LEFT JOIN compliance_control.taxpayer_risk_profile r ON p.party_id = r.party_id

-- Left join reference tables
LEFT JOIN reference.ref_industry ind ON p.industry_code = ind.industry_code
LEFT JOIN reference.ref_country c ON p.country_code = c.country_code
LEFT JOIN reference.ref_district d ON p.district_code = d.district_code
LEFT JOIN reference.ref_locality l ON p.locality_code = l.locality_code

ORDER BY p.party_id;
```

---

## Transformation Rules

### 1. Party Segmentation

Since Phase A doesn't have turnover data, use simplified segmentation:

```python
def get_party_segment(party_type: str, employee_count: int = None) -> str:
    """Derive party segment from available attributes."""
    if party_type == 'INDIVIDUAL':
        return 'INDIVIDUAL'
    elif party_type == 'ENTERPRISE':
        # Phase A: Default to 'SMALL' until we have turnover data
        return 'SMALL'
    else:
        return 'UNKNOWN'
```

### 2. Address Handling

Phase A has no address tables in L2, so use defaults:

```python
address_line1 = 'Not Available'  # Until party_address table exists
city = locality_name if locality_code else 'Unknown'
district = district_name if district_code else 'Unknown'
country = 'LK'  # Sri Lanka default
```

### 3. Risk Score Rounding

Ensure risk scores match ClickHouse DECIMAL(5,2) precision:

```python
risk_score = round(float(risk_score), 2) if risk_score else 0.0
```

### 4. SCD Type 2 Initial Load

For initial load (no history yet):

```python
valid_from = datetime.now()
valid_to = None  # NULL = current record
is_current = 1   # All records are current
```

### 5. Surrogate Key Generation

Generate party_key as auto-increment:

```python
def generate_party_key(clickhouse_conn) -> int:
    """Generate next party_key."""
    max_key = clickhouse_conn.get_max_value('dim_party', 'party_key')
    return (max_key or 0) + 1
```

---

## Data Quality Checks

Before loading to L3, validate:

1. **Primary Key**: party_id is NOT NULL
2. **Business Key**: tin is NOT NULL and unique
3. **Mandatory Fields**: party_name, party_type are NOT NULL
4. **Referential Integrity**: All FK references exist in reference tables
5. **Data Types**: Dates are valid, decimals are in range
6. **SCD Consistency**: is_current=1 AND valid_to IS NULL

---

## ETL Process Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    L2 MySQL (TA-RDM)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │ party.party  │  │ party.       │  │ compliance_     │   │
│  │              │  │ individual   │  │ control.        │   │
│  │              │  │              │  │ risk_profile    │   │
│  └──────┬───────┘  └──────┬───────┘  └────────┬────────┘   │
│         │                 │                   │             │
│         └─────────────────┴───────────────────┘             │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              │ EXTRACT
                              │ (SQL JOIN)
                              ▼
                    ┌─────────────────┐
                    │  ETL PROCESS    │
                    │  - Validate     │
                    │  - Transform    │
                    │  - Enrich       │
                    │  - Generate SKs │
                    └────────┬────────┘
                             │ LOAD
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                L3 ClickHouse (ta_dw)                        │
│                   ┌──────────────┐                          │
│                   │  dim_party   │                          │
│                   │  (SCD Type 2)│                          │
│                   │              │                          │
│                   └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Phase A Limitations

For Phase A (Foundation), the following fields will be NULL or defaulted:

| Field | Default Value | Reason |
|-------|---------------|--------|
| party_segment | 'SMALL' or 'INDIVIDUAL' | No turnover data yet |
| employee_count | NULL | No enterprise table in Phase A |
| annual_turnover | NULL | No financial data in Phase A |
| address_line1 | 'Not Available' | No party_address table yet |
| address_line2 | NULL | No party_address table yet |
| postal_code | NULL | No party_address table yet |
| email | NULL | No party_contact table yet |
| phone | NULL | No party_contact table yet |
| mobile | NULL | No party_contact table yet |

These fields will be populated in Phase B/C when the corresponding L2 tables are implemented.

---

## Next Steps

1. ✅ Document mapping (this file)
2. ⏳ Implement ETL script (`etl/l2_to_l3_party.py`)
3. ⏳ Test with Phase A data (5 parties)
4. ⏳ Validate dim_party in ClickHouse
5. 🔜 Extend to other dimensions (dim_tax_type, etc.)
6. 🔜 Implement fact table ETL (Phase B/C)

---

## References

- L2 Schema: `/docs/ta-rdm-mysql-ddl.sql`
- L3 Schema: `/docs/ta_dw_clickhouse_schema.sql`
- Phase A Design: `/docs/TA-RDM-TestData-Phase1-Design.md`
