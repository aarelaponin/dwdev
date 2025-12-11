# TA-RDM L3 Data Warehouse Model Overview

## Document Control

| Attribute | Value |
|-----------|-------|
| Version | 2.0.0 |
| Date | 2025-12-10 |
| Status | Production Ready (Generalized) |
| Author | TA-RDM L3 Generalization Process |
| Step | 4g of 7 (Final) |

---

## 1. Executive Summary

### 1.1 Purpose

The Tax Administration Reference Data Model (TA-RDM) Layer 3 Data Warehouse provides a **country-agnostic dimensional model** for tax administration analytics across multiple jurisdictions. This model supports:

- Multi-country deployments without schema changes
- Kimball-compliant star schema design
- ClickHouse-optimized physical implementation
- OECD-aligned compliance and risk management

### 1.2 Key Characteristics

| Characteristic | Description |
|----------------|-------------|
| **Methodology** | Kimball dimensional modeling |
| **Conformed Dimensions** | 16 dimensions supporting all facts |
| **Fact Tables** | 8 fact tables covering tax lifecycle |
| **Multi-Country** | dim_country as foundation; all components country-aware |
| **Physical Platform** | ClickHouse primary; PostgreSQL compatible |
| **SCD Support** | Type 1 (reference) and Type 2 (master) |

### 1.3 Version 2.0.0 Changes (Generalization)

| Change | Description | Pattern |
|--------|-------------|---------|
| **dim_country added** | Central country reference for all components | P1 |
| **dim_tax_scheme added** | Tax registration schemes (VAT Articles, etc.) | P3 |
| **dim_account_subtype added** | Malta imputation → generic pattern | P5 |
| **dim_party generalized** | Multi-identifier pattern, country_key | P2 |
| **dim_geography generalized** | Generic 4-level hierarchy | P4 |
| **dim_date generalized** | Configurable fiscal year, externalized holidays | P6, P7 |
| **dim_tax_period generalized** | Country-specific period patterns | P6 |
| **fact_refund generalized** | Imputation columns → bridge table | P5, P9 |
| **bridge_party_identifier added** | Multi-identifier lookup | P2 |
| **bridge_refund_account added** | Country-specific refund breakdown | P5 |
| **ref_country_holiday added** | Externalized holiday calendar | P7 |

### 1.4 Countries Supported (Initial Deployment)

| Country | Code | Fiscal Year | Currency | Special Features |
|---------|------|-------------|----------|------------------|
| Malta | MLT | CY+1 (YA) | EUR | Imputation system, EU VAT |
| Sri Lanka | LKA | April-March | LKR | SVAT (abolished), RAMIS integration |
| Moldova | MDA | Calendar | MDL | EU candidate, IDNO system |
| Ukraine | UKR | Calendar | UAH | EDRPOU system |
| Lebanon | LBN | Calendar | LBP | Multi-currency |

---

## 2. Architecture Overview

### 2.1 Layer Context

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      DATA ARCHITECTURE LAYERS                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  L0: RAW            Source system extracts (files, APIs)                │
│       ↓                                                                  │
│  L1: STAGING        Cleaned, deduplicated, type-cast                    │
│       ↓                                                                  │
│  L2: CANONICAL      TA-RDM Normalized Model (3NF)                       │
│       ↓             ~160 tables across 15 schemas                       │
│                                                                          │
│  L3: WAREHOUSE      Dimensional Model (Star Schema) ◄── THIS DOCUMENT   │
│       ↓             16 dimensions + 8 facts + 2 bridges                 │
│                                                                          │
│  L4: MARTS          Subject-specific views and aggregations             │
│                     Compliance, Revenue, Risk dashboards                 │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Physical Platform

| Aspect | Specification |
|--------|--------------|
| **Primary Database** | ClickHouse |
| **Compatibility** | PostgreSQL DDL variants available |
| **Schema** | `warehouse` |
| **Dimension Engine** | ReplacingMergeTree (SCD Type 2), MergeTree (Type 1) |
| **Fact Engine** | MergeTree with monthly partitioning |
| **Index Strategy** | Bloom filter, MinMax, Set indexes |

---

## 3. Multi-Country Architecture

### 3.1 Country Dimension as Foundation

All country-sensitive components reference dim_country:

```
                        ┌──────────────────────┐
                        │     dim_country      │
                        │   (country_key)      │
                        │  Fiscal year config  │
                        │  Currency/timezone   │
                        │  Tax system rules    │
                        └──────────┬───────────┘
                                   │
           ┌───────────────────────┼───────────────────────┐
           │                       │                       │
           ▼                       ▼                       ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│    dim_party     │   │  dim_tax_type    │   │  dim_geography   │
│  (country_key)   │   │  (country_key)   │   │  (country_key)   │
└──────────────────┘   └──────────────────┘   └──────────────────┘
           │                       │                       │
           │                       │                       │
           ▼                       ▼                       ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  dim_tax_period  │   │ dim_tax_scheme   │   │dim_account_subtype│
│  (country_key)   │   │  (country_key)   │   │  (country_key)   │
└──────────────────┘   └──────────────────┘   └──────────────────┘
           │
           │         All dimensions conform to country context
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FACT TABLES                               │
│   fact_filing, fact_assessment, fact_payment, fact_refund       │
│   fact_account_balance, fact_customs_declaration                 │
│   fact_compliance_event, fact_risk_score                         │
│                                                                  │
│              ALL FACTS HAVE country_key FK                       │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Country-Specific Pattern Solutions

| Pattern | Problem | Solution | Example |
|---------|---------|----------|---------|
| **Fiscal Year** | Malta YA = CY+1; Sri Lanka = April FY | dim_country.fiscal_year_offset_years, dim_date.fiscal_year_* | Malta: fiscal_year_jan_offset1 |
| **Tax Schemes** | VAT Articles 10/11/12 are Malta-only | dim_tax_scheme by country | MLT_VAT_ART10, LKA_VAT_STD |
| **Account Types** | Malta imputation (FTA/MTA/FIA/IPA/UA) | dim_account_subtype by country | Malta: 5 accounts; Others: 1 STD |
| **Identifiers** | TIN, VAT, EORI, IDNO, EDRPOU | bridge_party_identifier | Multiple per party |
| **Holidays** | Country-specific public holidays | ref_country_holiday (query-time) | 14 Malta holidays, 26 Sri Lanka |
| **Geography** | District/locality vs Province/District | dim_geography with generic levels | Level 1-4 per country |
| **Translations** | Day/month names in local languages | ref_localized_text (Pattern P8) | day_name in Maltese |

### 3.3 dim_country Key Attributes

```yaml
dim_country:
  # Fiscal Year Configuration
  fiscal_year_start_month: 1-12
  fiscal_year_offset_years: 0 (most) or 1 (Malta)
  fiscal_year_label_pattern: "FY{YEAR}" or "YA{YEAR}"
  tax_year_type: CALENDAR, FISCAL_APR, FISCAL_JUL
  
  # Tax System
  primary_tax_id_type: TIN, IDNO, EDRPOU
  vat_country_prefix: MT, LK, MD, UA, LB
  has_imputation_system: 1 (Malta) or 0 (others)
  
  # EU Membership
  is_eu_member: 1 (Malta) or 0 (others)
  is_eurozone_member: 1 (Malta) or 0
  requires_intrastat: derived from EU membership
```

---

## 4. Dimension Catalog

### 4.1 Core Dimensions (Country-Aware)

| # | Dimension | Grain | SCD Type | Key Attributes | Pattern |
|---|-----------|-------|----------|----------------|---------|
| 1 | **dim_country** | One per country | 1 | country_code, fiscal_year_config, currency | P1 |
| 2 | **dim_date** | One per calendar day | 0 | calendar_date, fiscal_year variants | P6, P7 |
| 3 | **dim_party** | One per party/version | 2 | primary_tax_id, country_key | P2 |
| 4 | **dim_tax_type** | One per tax type | 1 | tax_type_code, country_key | - |
| 5 | **dim_tax_period** | One per period | 1 | period_code, country_key | P6 |
| 6 | **dim_registration** | One per registration/version | 2 | tax_scheme_key, country_key | P3 |
| 7 | **dim_geography** | One per admin division | 1 | level_1..4, country_key | P4 |
| 8 | **dim_tax_scheme** | One per scheme/version | 2 | scheme_code, thresholds, filing rules | P3 |
| 9 | **dim_account_subtype** | One per account type | 1 | refund_rate, is_imputation | P5 |

### 4.2 Reference Dimensions (Standalone)

| # | Dimension | Grain | SCD Type | Purpose |
|---|-----------|-------|----------|---------|
| 10 | **dim_assessment_type** | One per type | 1 | Assessment categories (self, desk, field) |
| 11 | **dim_compliance_status** | One per status | 1 | Compliance states (filed, late, non-filer) |
| 12 | **dim_customs_procedure** | One per procedure | 1 | Customs codes (import, export, transit) |
| 13 | **dim_payment_method** | One per method | 1 | Payment channels (bank, card, cash) |
| 14 | **dim_risk_category** | One per category | 1 | Risk classification (low, medium, high) |
| 15 | **dim_international_country** | One per country | 1 | All ISO countries for trade/reporting |

### 4.3 Bridge Tables

| Bridge | Purpose | Grain | v2.0 Status |
|--------|---------|-------|-------------|
| **bridge_party_identifier** | Party → multiple identifiers | One per identifier | NEW |
| **bridge_refund_account** | Refund → account subtypes | One per account | NEW |

### 4.4 Reference Tables (Supporting)

| Table | Purpose | Used By |
|-------|---------|---------|
| **ref_country_holiday** | Country-specific holidays | dim_date queries |
| **ref_identifier_type** | Identifier format rules | bridge_party_identifier |
| **ref_tax_scheme** | L2 source for dim_tax_scheme | ETL |
| **ref_localized_text** | Translations | All dimensions |

---

## 5. Fact Table Catalog

### 5.1 Transactional Facts

| # | Fact | Grain | Key Measures | Est. Rows |
|---|------|-------|--------------|-----------|
| 1 | **fact_filing** | One per tax return | filed_amount, tax_due, penalties | 5M initial |
| 2 | **fact_assessment** | One per assessment | assessed_amount, penalty, interest | 3M initial |
| 3 | **fact_payment** | One per payment | payment_amount, allocation breakdown | 10M initial |
| 4 | **fact_refund** | One per refund claim | claimed, approved, paid, offset | 500K initial |
| 5 | **fact_customs_declaration** | One per declaration | customs_value, duty, import_vat | 2M initial |

### 5.2 Periodic Snapshot Facts

| # | Fact | Grain | Periodicity | Key Measures | Est. Rows |
|---|------|-------|-------------|--------------|-----------|
| 6 | **fact_account_balance** | One per account/month | Monthly | opening, closing, movement, aging | 50M initial |
| 7 | **fact_risk_score** | One per party/quarter | Quarterly | risk_score, tax_gap_amount | 10M initial |

### 5.3 Factless Fact

| # | Fact | Grain | Purpose | Est. Rows |
|---|------|-------|---------|-----------|
| 8 | **fact_compliance_event** | One per event | Coverage analysis, event correlation | 2M initial |

### 5.4 Volume Summary

| Category | Initial Rows | Annual Growth | 5-Year Projection |
|----------|-------------:|-------------:|------------------:|
| Dimensions | ~525K | ~50K | ~750K |
| Facts | ~82.5M | ~10.7M | ~136M |
| **Total** | **~83M** | **~10.75M** | **~137M** |

---

## 6. Enterprise Bus Matrix

### 6.1 Dimension-Fact Conformance Matrix

```
                                            D I M E N S I O N S
                    ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
                    │cntry│date │party│tax_ │tax_ │regis│geog │tax_ │acct │asmt │comp │cust │pay_ │risk │
FACTS               │     │     │     │type │period│trat │     │scheme│subtp│type │stat │proc │meth │cat  │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_filing       │  ●  │  4  │  ●  │  ●  │  ●  │  ●  │  ○  │  ○  │     │     │  ●  │     │     │  ○  │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_assessment   │  ●  │  4  │  ●  │  ●  │  ●  │  ●  │     │     │     │  ●  │     │     │     │  ○  │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_payment      │  ●  │  4  │  ●  │  ●  │  ○  │  ●  │     │     │     │     │  ●  │     │  ●  │     │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_refund       │  ●  │  5  │  ●  │  ●  │  ○  │  ●  │     │  ●  │  *  │     │  ●  │     │  ●  │     │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_account_bal  │  ●  │  1  │  ●  │  ●  │     │  ●  │  ○  │     │  ○  │     │  ●  │     │     │  ○  │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_customs_decl │  ●  │  3  │  2  │     │     │  ●  │     │     │     │     │     │  ●  │  ○  │  ○  │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_compliance   │  ●  │  3  │  ●  │  ○  │  ○  │  ○  │  ○  │     │     │     │  ●  │     │     │  ○  │
├───────────────────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ fact_risk_score   │  ●  │  1  │  ●  │  ○  │     │  ○  │  ○  │     │     │     │     │     │     │  2  │
└───────────────────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘

LEGEND:
  ●  = Required FK (NOT NULL)
  ○  = Optional FK (nullable, default -1 for Unknown)
  N  = Number of role-playing FKs (e.g., "4" = 4 date roles)
  *  = Via bridge table (bridge_refund_account)
```

### 6.2 Conformed Dimension Verification

| Dimension | Facts Using | Conformance | Notes |
|-----------|-------------|-------------|-------|
| dim_country | 8/8 (100%) | ✓ Full | Foundation for multi-country |
| dim_date | 8/8 (100%) | ✓ Full | Role-played (1-5 roles per fact) |
| dim_party | 8/8 (100%) | ✓ Full | Same structure in all facts |
| dim_tax_type | 6/8 (75%) | ✓ Partial | Not in customs, compliance (by design) |
| dim_tax_period | 5/8 (63%) | ✓ Partial | Not applicable to snapshots |
| dim_registration | 7/8 (88%) | ✓ Full | Most transaction types |
| dim_geography | 4/8 (50%) | ○ Optional | Used where location matters |
| dim_tax_scheme | 2/8 (25%) | ○ Specialized | Filing and refund only |

---

## 7. ETL Load Sequence

### 7.1 Recommended Load Order

```
Phase 1: STATIC REFERENCE DIMENSIONS (No Dependencies)
├── 1.  dim_country           ← LOAD FIRST
├── 2.  dim_date              ← Generate 50+ years
├── 3.  dim_tax_scheme        ← Reference data
├── 4.  dim_account_subtype   ← Reference data
├── 5.  dim_assessment_type   ← Lookup table
├── 6.  dim_compliance_status ← Lookup table
├── 7.  dim_customs_procedure ← Lookup table
├── 8.  dim_payment_method    ← Lookup table
├── 9.  dim_risk_category     ← Lookup table
└── 10. dim_international_country ← ISO countries

Phase 2: MASTER DIMENSIONS (Reference Dependencies)
├── 11. dim_tax_type          → dim_country
├── 12. dim_geography         → dim_country
├── 13. dim_party             → dim_country, dim_geography (SCD2)
├── 14. dim_tax_period        → dim_country, dim_tax_type
└── 15. dim_registration      → dim_party, dim_tax_type, dim_tax_scheme (SCD2)

Phase 3: BRIDGE TABLES
├── 16. bridge_party_identifier → dim_party
└── 17. bridge_refund_account   ← Post fact_refund

Phase 4: FACT TABLES (All Dimension Dependencies)
├── 18. fact_filing
├── 19. fact_assessment
├── 20. fact_payment
├── 21. fact_refund
├── 22. fact_account_balance  ← Monthly snapshot
├── 23. fact_customs_declaration
├── 24. fact_compliance_event
└── 25. fact_risk_score       ← Quarterly snapshot
```

### 7.2 Load Frequency Matrix

| Layer | Frequency | Strategy | Notes |
|-------|-----------|----------|-------|
| Reference Dimensions | On-change | Full refresh | < 1000 rows typically |
| dim_date | Initial only | Full load | + daily flag update |
| Master Dimensions | Daily | Incremental + SCD2 | dim_party, dim_registration |
| Bridge Tables | With parent | Incremental | Sync with parent dimension |
| Transaction Facts | Daily | Incremental append | Partition by month |
| Snapshot Facts | Period-end | Full for period | Monthly/quarterly |

### 7.3 ETL Timing (Production Schedule)

```
TIME        TASK                                    DURATION
─────────────────────────────────────────────────────────────
00:00       L0/L1 Extract from source systems       60 min
01:00       Phase 1: Static dimensions              30 min
01:30       Phase 2: Master dimensions (SCD2)       60 min
02:30       Phase 3: Bridge tables                  15 min
02:45       Phase 4: Transaction facts              90 min
04:15       Snapshot facts (if period-end)          30 min
04:45       Index optimization & statistics         15 min
05:00       Data quality validation                 30 min
05:30       Ready for consumption                   ─────────
```

---

## 8. Query Patterns

### 8.1 Multi-Country Filing Analysis

```sql
-- Compare filing compliance across countries
SELECT 
    c.country_name,
    t.tax_type_name,
    p.period_code,
    COUNT(*) AS filing_count,
    SUM(CASE WHEN f.is_filed_on_time = 1 THEN 1 ELSE 0 END) AS on_time_count,
    SUM(f.tax_due_amount) AS total_tax_due,
    AVG(f.days_to_file) AS avg_days_to_file
FROM warehouse.fact_filing f
JOIN warehouse.dim_country c ON f.country_key = c.country_key
JOIN warehouse.dim_tax_type t ON f.tax_type_key = t.tax_type_key
JOIN warehouse.dim_tax_period p ON f.tax_period_key = p.tax_period_key
WHERE f.filing_date_key BETWEEN 20250101 AND 20251231
GROUP BY c.country_name, t.tax_type_name, p.period_code
ORDER BY c.country_name, total_tax_due DESC;
```

### 8.2 Malta Refund with Imputation Breakdown (Using Bridge)

```sql
-- Refund analysis with imputation account breakdown
SELECT 
    p.display_name,
    r.claim_reference_number,
    r.claimed_amount,
    r.approved_amount,
    a.account_subtype_code,
    a.account_subtype_name,
    b.refund_amount,
    a.refund_rate_display,
    b.refund_rate_applied
FROM warehouse.fact_refund r
JOIN warehouse.dim_party p ON r.party_key = p.party_key
JOIN warehouse.bridge_refund_account b ON r.refund_source_id = b.refund_source_id
JOIN warehouse.dim_account_subtype a ON b.account_subtype_key = a.account_subtype_key
WHERE r.country_key = (SELECT country_key FROM dim_country WHERE country_code = 'MLT')
  AND r.is_imputation_refund = 1
  AND p.is_current = 1
ORDER BY r.refund_source_id, a.distribution_priority;
```

### 8.3 Fiscal Year Comparison Across Countries

```sql
-- Revenue by fiscal year across different country systems
SELECT 
    c.country_code,
    c.country_name,
    CASE c.tax_year_type
        WHEN 'CALENDAR' THEN d.fiscal_year_jan + c.fiscal_year_offset_years
        WHEN 'FISCAL_APR' THEN d.fiscal_year_apr + c.fiscal_year_offset_years
        WHEN 'FISCAL_JUL' THEN d.fiscal_year_jul + c.fiscal_year_offset_years
    END AS fiscal_year,
    c.fiscal_year_label_pattern,
    SUM(f.tax_due_amount) AS total_tax_due,
    COUNT(DISTINCT f.party_key) AS unique_filers
FROM warehouse.fact_filing f
JOIN warehouse.dim_country c ON f.country_key = c.country_key
JOIN warehouse.dim_date d ON f.filing_date_key = d.date_key
WHERE d.calendar_year IN (2024, 2025)
GROUP BY c.country_code, c.country_name, fiscal_year, c.fiscal_year_label_pattern
ORDER BY c.country_code, fiscal_year;
```

### 8.4 Party Identifier Lookup (Multi-Identifier Pattern)

```sql
-- Find party by any identifier type
SELECT 
    p.party_key,
    p.display_name,
    p.party_type,
    p.country_code,
    b.identifier_type_code,
    b.identifier_value,
    b.is_primary,
    b.is_verified
FROM warehouse.dim_party p
JOIN warehouse.bridge_party_identifier b ON p.party_key = b.party_key
WHERE b.identifier_value_normalized = UPPER(REPLACE(:search_value, ' ', ''))
  AND b.is_current = 1
  AND p.is_current = 1;
```

### 8.5 Holiday-Aware Business Day Calculation

```sql
-- Calculate business days between filing and due date (Malta)
WITH malta_dates AS (
    SELECT 
        d.date_key,
        d.calendar_date,
        d.is_weekend,
        CASE WHEN h.holiday_date IS NOT NULL THEN 1 ELSE 0 END AS is_holiday
    FROM warehouse.dim_date d
    LEFT JOIN reference.ref_country_holiday h 
        ON d.calendar_date = h.holiday_date
        AND h.country_code = 'MLT'
    WHERE d.calendar_date BETWEEN :start_date AND :end_date
)
SELECT 
    COUNT(*) AS calendar_days,
    SUM(CASE WHEN is_weekend = 0 AND is_holiday = 0 THEN 1 ELSE 0 END) AS business_days
FROM malta_dates;
```

### 8.6 Tax Scheme Eligibility Check

```sql
-- Check if taxpayer qualifies for Article 11 (small enterprise exemption)
SELECT 
    s.scheme_code,
    s.scheme_name,
    s.registration_threshold,
    s.registration_threshold_currency,
    s.allows_input_recovery,
    s.filing_frequency_name,
    CASE 
        WHEN :annual_turnover < s.registration_threshold THEN 'ELIGIBLE'
        ELSE 'NOT_ELIGIBLE'
    END AS eligibility_status
FROM warehouse.dim_tax_scheme s
WHERE s.country_code = 'MLT'
  AND s.tax_type_code = 'VAT'
  AND s.eligible_party_types LIKE '%ENTERPRISE%'
  AND s.is_current = 1
  AND s.is_active = 1
ORDER BY s.registration_threshold;
```

---

## 9. Implementation Checklist

### 9.1 Pre-Deployment Requirements

| # | Requirement | Status |
|---|-------------|--------|
| 1 | L2 canonical model deployed and populated | ☐ |
| 2 | ClickHouse/PostgreSQL database created | ☐ |
| 3 | Schema `warehouse` created | ☐ |
| 4 | Reference data loaded in L2 (ref_* tables) | ☐ |
| 5 | Country configurations defined | ☐ |

### 9.2 Dimension Deployment Checklist

| # | Dimension | DDL | Data | Validated |
|---|-----------|-----|------|-----------|
| 1 | dim_country | ☐ | ☐ 5+ countries | ☐ |
| 2 | dim_date | ☐ | ☐ 50+ years | ☐ |
| 3 | dim_party | ☐ | ☐ From L2 | ☐ |
| 4 | dim_tax_type | ☐ | ☐ With country_key | ☐ |
| 5 | dim_tax_period | ☐ | ☐ With country_key | ☐ |
| 6 | dim_registration | ☐ | ☐ With tax_scheme_key | ☐ |
| 7 | dim_geography | ☐ | ☐ Generic levels | ☐ |
| 8 | dim_tax_scheme | ☐ | ☐ VAT schemes per country | ☐ |
| 9 | dim_account_subtype | ☐ | ☐ Malta imputation + STD | ☐ |
| 10 | dim_assessment_type | ☐ | ☐ Reference data | ☐ |
| 11 | dim_compliance_status | ☐ | ☐ Reference data | ☐ |
| 12 | dim_customs_procedure | ☐ | ☐ Reference data | ☐ |
| 13 | dim_payment_method | ☐ | ☐ Reference data | ☐ |
| 14 | dim_risk_category | ☐ | ☐ Reference data | ☐ |

### 9.3 Bridge Table Deployment

| # | Bridge | DDL | Data | Validated |
|---|--------|-----|------|-----------|
| 1 | bridge_party_identifier | ☐ | ☐ | ☐ |
| 2 | bridge_refund_account | ☐ | ☐ | ☐ |

### 9.4 Fact Table Deployment

| # | Fact | DDL | Partitioned | Data | Validated |
|---|------|-----|-------------|------|-----------|
| 1 | fact_filing | ☐ | ☐ Monthly | ☐ | ☐ |
| 2 | fact_assessment | ☐ | ☐ Monthly | ☐ | ☐ |
| 3 | fact_payment | ☐ | ☐ Monthly | ☐ | ☐ |
| 4 | fact_refund | ☐ | ☐ Monthly | ☐ | ☐ |
| 5 | fact_account_balance | ☐ | ☐ Monthly | ☐ | ☐ |
| 6 | fact_customs_declaration | ☐ | ☐ Monthly | ☐ | ☐ |
| 7 | fact_compliance_event | ☐ | ☐ Monthly | ☐ | ☐ |
| 8 | fact_risk_score | ☐ | ☐ Quarterly | ☐ | ☐ |

### 9.5 Validation Tests

| # | Test | Expected | Actual |
|---|------|----------|--------|
| 1 | Dimension row counts match L2 | ☐ | |
| 2 | Fact row counts match L2 | ☐ | |
| 3 | Referential integrity verified | ☐ | |
| 4 | Multi-country queries work | ☐ | |
| 5 | Bridge table joins correct | ☐ | |
| 6 | Fiscal year calculations correct | ☐ | |
| 7 | SCD Type 2 history intact | ☐ | |
| 8 | Partition pruning effective | ☐ | |

---

## 10. Related Documents

| Document | Purpose | Location |
|----------|---------|----------|
| L2-00-model-overview.md | L2 canonical model overview | Project Knowledge |
| L2-extension-guide.md | Country implementation for L2 | Project Knowledge |
| L3-Generalization-Patterns-Catalog.md | Reusable L3 patterns | Project Knowledge |
| dim_country.yaml | Country dimension specification | Step 4a output |
| dim_tax_scheme.yaml | Tax scheme dimension specification | Step 4b output |
| dim_party.yaml | Party dimension specification | Step 4c output |
| dim_geography.yaml | Geography dimension specification | Step 4d output |
| dim_account_subtype.yaml | Account subtype specification | Step 4e output |
| bridge_party_identifier.yaml | Party identifier bridge | Step 4c output |
| bridge_refund_account.yaml | Refund account bridge | Step 4e output |
| fact_refund_generalized.yaml | Generalized refund fact | Step 4e output |
| dim_date_generalized.yaml | Generalized date dimension | Step 4f output |
| dim_tax_period_generalized.yaml | Generalized tax period | Step 4f output |
| ref_country_holiday.yaml | Country holiday reference | Step 4f output |
| enterprise_bus_matrix.md | Full bus matrix details | Project Knowledge |
| etl_load_sequence.md | Detailed ETL specifications | Project Knowledge |
| fact_table_inventory.md | All fact specifications | Project Knowledge |

---

## 11. Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 2.0.0 | 2025-12-10 | TA-RDM L3 Generalization | Multi-country generalization complete |
| 1.0.0 | 2025-11-01 | MTCA ORS Project | Initial release (Malta-specific) |

### Version 2.0.0 Detailed Changes

| Component | v1.0.0 | v2.0.0 | Migration Impact |
|-----------|--------|--------|------------------|
| dim_country | Not present | NEW - Central reference | Add country_key to all |
| dim_party.tin | Malta TIN column | primary_tax_id (generic) | Rename + generalize |
| dim_party.vat_number | Malta VAT column | Moved to bridge | Use bridge_party_identifier |
| dim_date.year_of_assessment | Malta YA calculation | fiscal_year_jan_offset1 | Query change only |
| dim_date.is_malta_public_holiday | Malta flag | Removed | Use ref_country_holiday |
| dim_geography | Malta locality/district | Generic level_1..4 | Remap hierarchy |
| dim_registration.vat_scheme_code | Malta Article codes | tax_scheme_key FK | Use dim_tax_scheme |
| fact_refund imputation columns | 7 Malta columns | Moved to bridge | Use bridge_refund_account |

---

## Appendix A: Pattern Reference Summary

| ID | Pattern Name | Components | Purpose |
|----|--------------|------------|---------|
| P1 | Country Dimension | dim_country | Central country configuration |
| P2 | Multi-Identifier Party | dim_party + bridge_party_identifier | Flexible identifier management |
| P3 | Tax Scheme Dimension | dim_tax_scheme | Country-agnostic scheme rules |
| P4 | Generic Geography Hierarchy | dim_geography | 4-level country-neutral |
| P5 | Account Subtype Dimension | dim_account_subtype + bridge_refund_account | Country-specific breakdown |
| P6 | Configurable Fiscal Year | dim_date + dim_tax_period + dim_country | Pre-calculated fiscal years |
| P7 | Externalized Holidays | ref_country_holiday | Query-time lookup |
| P8 | Localized Text | ref_localized_text | Multi-language support |
| P9 | Country-Conditional Measures | fact_refund (generalized) | Bridge for country-specific |

---

## Appendix B: Quick Reference Card

### Dimension Type Selection

| Question | SCD Type 0 | SCD Type 1 | SCD Type 2 |
|----------|:----------:|:----------:|:----------:|
| Values never change? | ✓ | | |
| Track current only? | | ✓ | |
| Track full history? | | | ✓ |
| Reference data? | | ✓ | |
| Master data? | | | ✓ |

### Measure Additivity

| If you need to... | Measure Type | Example |
|-------------------|--------------|---------|
| SUM across all dimensions | Additive | payment_amount |
| SUM except across time | Semi-Additive | account_balance |
| AVG or ratio calculation | Non-Additive | approval_rate |

### Country Context Checklist

When adding a new country:
1. ☐ Add row to dim_country with fiscal year config
2. ☐ Add tax types to dim_tax_type with country_key
3. ☐ Add tax periods to dim_tax_period with country_key
4. ☐ Add geography hierarchy to dim_geography
5. ☐ Add tax schemes to dim_tax_scheme
6. ☐ Add account subtypes (or STD default) to dim_account_subtype
7. ☐ Add holidays to ref_country_holiday
8. ☐ Add identifier types to ref_identifier_type (if new)

---

*Document End - Step 4g Complete*
