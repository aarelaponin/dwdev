# TA-RDM L3 Generalization Patterns Catalog

**Version:** 1.0.0  
**Date:** 2025-12-10  
**Author:** TA-RDM L3 Generalization Process  
**Purpose:** Reusable patterns for making L3 dimensional models country-agnostic

---

## Overview

This catalog provides standardized patterns for generalizing dimensional warehouse models across multiple tax administration jurisdictions. Each pattern addresses a specific type of country-specific leakage and provides a reusable solution.

---

## Pattern Index

| # | Pattern Name | Type | Priority | Use Case |
|---|--------------|------|----------|----------|
| P1 | Country Dimension | Dimension | CRITICAL | Country context for all components |
| P2 | Multi-Identifier Party | Dimension | HIGH | Generic taxpayer identification |
| P3 | Tax Scheme Dimension | Dimension | HIGH | Country-specific tax schemes |
| P4 | Generic Geography Hierarchy | Dimension | HIGH | Multi-level admin divisions |
| P5 | Account Subtype Dimension | Dimension | HIGH | Country-specific account types |
| P6 | Configurable Fiscal Year | Dimension | MEDIUM | Year-of-Assessment variations |
| P7 | Externalized Holidays | Reference | MEDIUM | Country-specific holidays |
| P8 | Localization Pattern | Design | LOW | Multi-language support |
| P9 | Country-Conditional Measures | Fact | MEDIUM | Measures that exist in some countries |
| P10 | Bridge Table for Variants | Design | LOW | Handle country variations |

---

## Pattern P1: Country Dimension

### Problem
The L3 model has no central country dimension, making it impossible to support multiple countries in the same warehouse.

### Solution
Create a conformed `dim_country` dimension referenced by all country-sensitive dimensions and facts.

### Implementation

```yaml
# =============================================================================
# dim_country - Country Dimension
# =============================================================================
dimension:
  name: dim_country
  schema: warehouse
  description: |
    Central country reference dimension providing configuration for
    country-specific business rules, fiscal years, and defaults.
    
  scd_type: 1
  natural_key: [country_code]
  surrogate_key: country_key
  
  columns:
    # Keys
    - name: country_key
      data_type: INT
      nullable: false
      auto_increment: true
      role: SURROGATE_KEY
      
    - name: country_code
      data_type: VARCHAR(3)
      nullable: false
      description: "ISO 3166-1 alpha-3 code (MLT, LKA, MDA, UKR)"
      role: NATURAL_KEY
      
    # Basic Attributes
    - name: country_code_alpha2
      data_type: VARCHAR(2)
      description: "ISO 3166-1 alpha-2 code (MT, LK, MD, UA)"
      
    - name: country_name
      data_type: VARCHAR(100)
      nullable: false
      description: "English country name"
      
    - name: country_name_local
      data_type: VARCHAR(100)
      description: "Name in local language"
      
    # Currency
    - name: currency_code
      data_type: VARCHAR(3)
      nullable: false
      description: "ISO 4217 currency code (EUR, LKR, MDL, UAH)"
      
    - name: currency_symbol
      data_type: VARCHAR(5)
      description: "Currency symbol (€, Rs, L, ₴)"
      
    # Fiscal Year Configuration
    - name: fiscal_year_start_month
      data_type: SMALLINT
      nullable: false
      default_value: 1
      description: "Month fiscal year starts (1=Jan, 4=Apr)"
      
    - name: fiscal_year_offset_years
      data_type: SMALLINT
      nullable: false
      default_value: 0
      description: "Offset for assessment year (0=same year, 1=next year)"
      
    - name: fiscal_year_label_pattern
      data_type: VARCHAR(20)
      default_value: "FY{YEAR}"
      description: "Pattern for fiscal year label (YA{YEAR}, FY{YEAR})"
      
    # Regional Configuration
    - name: region_code
      data_type: VARCHAR(10)
      description: "Geographic region (EU, APAC, EMEA)"
      
    - name: timezone
      data_type: VARCHAR(50)
      description: "Primary timezone (Europe/Malta, Asia/Colombo)"
      
    - name: default_language_code
      data_type: VARCHAR(5)
      description: "Primary language (en, si, ro, uk)"
      
    # Membership Flags
    - name: is_eu_member
      data_type: BOOLEAN
      nullable: false
      default_value: false
      
    - name: eu_membership_date
      data_type: DATE
      description: "Date of EU accession (NULL if not member)"
      
    - name: is_eurozone_member
      data_type: BOOLEAN
      nullable: false
      default_value: false
      
    # Tax System Configuration
    - name: primary_tax_id_type
      data_type: VARCHAR(20)
      description: "Primary tax identifier type (TIN, IDNO, etc.)"
      
    - name: vat_country_prefix
      data_type: VARCHAR(2)
      description: "VAT number prefix (MT, NULL for non-EU)"
      
    # Status
    - name: is_active
      data_type: BOOLEAN
      nullable: false
      default_value: true
      
    # ETL Metadata
    - name: etl_batch_id
      data_type: BIGINT
      nullable: false
      
    - name: etl_load_timestamp
      data_type: TIMESTAMP
      nullable: false

sample_data:
  - country_code: MLT
    country_code_alpha2: MT
    country_name: Malta
    currency_code: EUR
    fiscal_year_start_month: 1
    fiscal_year_offset_years: 1  # YA = CY + 1
    fiscal_year_label_pattern: "YA{YEAR}"
    is_eu_member: true
    eu_membership_date: "2004-05-01"
    is_eurozone_member: true
    primary_tax_id_type: TIN
    vat_country_prefix: MT
    
  - country_code: LKA
    country_code_alpha2: LK
    country_name: Sri Lanka
    currency_code: LKR
    fiscal_year_start_month: 4  # April-March
    fiscal_year_offset_years: 0
    fiscal_year_label_pattern: "FY{YEAR}/{NEXT_YEAR_SHORT}"
    is_eu_member: false
    primary_tax_id_type: TIN
    
  - country_code: MDA
    country_code_alpha2: MD
    country_name: Moldova
    currency_code: MDL
    fiscal_year_start_month: 1
    fiscal_year_offset_years: 0
    fiscal_year_label_pattern: "FY{YEAR}"
    is_eu_member: false
    primary_tax_id_type: IDNO
```

### Usage
Every dimension and fact that varies by country should include:
```yaml
- name: country_key
  data_type: INT
  nullable: false
  foreign_key: warehouse.dim_country(country_key)
```

---

## Pattern P2: Multi-Identifier Party Dimension

### Problem
Party dimensions have hardcoded identifier columns (tin, vat_number, eori_number) with country-specific formats.

### Solution
Use generic identifier columns with type references, leveraging L2's party_identifier pattern.

### Implementation

```yaml
# dim_party identifier section (generalized)
columns:
  # Generic primary identifier
  - name: primary_tax_id
    data_type: VARCHAR(50)
    nullable: true
    description: "Primary tax identifier value (format varies by country)"
    
  - name: primary_tax_id_type
    data_type: VARCHAR(20)
    nullable: true
    description: "Type of primary identifier (TIN, IDNO, NIC, etc.)"
    source: "Derived from ref_identifier_type based on country_key"
    
  - name: secondary_id
    data_type: VARCHAR(50)
    nullable: true
    description: "Secondary identifier (VAT number, EORI, etc.)"
    
  - name: secondary_id_type
    data_type: VARCHAR(20)
    nullable: true
    description: "Type of secondary identifier"
    
  # National ID (separate for KYC)
  - name: national_id_number
    data_type: VARCHAR(50)
    nullable: true
    description: "National identity document number"
    note: "Renamed from malta_id_card_number"

# Bridge table for multiple identifiers
bridge_party_identifier:
  name: bridge_party_identifier
  schema: warehouse
  description: "Links parties to all their identifiers"
  
  columns:
    - name: party_key
      foreign_key: dim_party(party_key)
      
    - name: identifier_type_code
      data_type: VARCHAR(20)
      description: "TIN, VAT, EORI, NATID, etc."
      
    - name: country_code
      data_type: VARCHAR(3)
      description: "Country that issued identifier"
      
    - name: identifier_value
      data_type: VARCHAR(50)
      
    - name: is_primary
      data_type: BOOLEAN
      default_value: false
      
    - name: is_verified
      data_type: BOOLEAN
      default_value: false
      
    - name: valid_from_date
      data_type: DATE
      
    - name: valid_to_date
      data_type: DATE
```

### Query Pattern
```sql
-- Get party with all identifiers
SELECT 
    dp.party_key,
    dp.display_name,
    dp.primary_tax_id,
    dp.primary_tax_id_type,
    GROUP_CONCAT(CONCAT(bpi.identifier_type_code, ':', bpi.identifier_value)) as all_identifiers
FROM warehouse.dim_party dp
LEFT JOIN warehouse.bridge_party_identifier bpi ON dp.party_key = bpi.party_key
WHERE dp.is_current_flag = TRUE
GROUP BY dp.party_key;
```

---

## Pattern P3: Tax Scheme Dimension

### Problem
Registration dimensions hardcode scheme attributes (vat_scheme_code = 'ART10', threshold = €35,000).

### Solution
Create a conformed `dim_tax_scheme` dimension sourced from L2's ref_tax_scheme.

### Implementation

```yaml
dimension:
  name: dim_tax_scheme
  schema: warehouse
  description: |
    Tax registration schemes by country. Replaces hardcoded VAT Article
    attributes with flexible, country-specific scheme definitions.
    
  scd_type: 1
  natural_key: [scheme_code]
  surrogate_key: tax_scheme_key
  
  columns:
    - name: tax_scheme_key
      data_type: INT
      auto_increment: true
      role: SURROGATE_KEY
      
    - name: scheme_code
      data_type: VARCHAR(30)
      nullable: false
      description: "Globally unique scheme code (MLT_VAT_ART10, LKA_VAT_STD)"
      role: NATURAL_KEY
      
    - name: country_key
      data_type: INT
      nullable: false
      foreign_key: dim_country(country_key)
      
    - name: tax_type_key
      data_type: INT
      nullable: false
      foreign_key: dim_tax_type(tax_type_key)
      
    - name: scheme_name
      data_type: VARCHAR(200)
      description: "Full scheme name"
      
    - name: scheme_name_short
      data_type: VARCHAR(50)
      description: "Abbreviated name (Article 10, SVAT)"
      
    # Thresholds
    - name: registration_threshold
      data_type: DECIMAL(15,2)
      description: "Mandatory registration threshold"
      
    - name: exit_threshold
      data_type: DECIMAL(15,2)
      description: "De-registration threshold (if different)"
      
    - name: threshold_currency
      data_type: VARCHAR(3)
      description: "Currency for thresholds"
      
    - name: threshold_period
      data_type: VARCHAR(20)
      description: "ANNUAL, QUARTERLY, etc."
      
    # Filing Requirements
    - name: filing_frequency_code
      data_type: VARCHAR(20)
      allowed_values: [ANNUAL, QUARTERLY, MONTHLY, TRANSACTIONAL]
      
    - name: return_due_days
      data_type: INT
      description: "Days after period end for return due"
      
    - name: payment_due_days
      data_type: INT
      description: "Days after period end for payment due"
      
    # Recovery Rules
    - name: allows_input_recovery
      data_type: BOOLEAN
      default_value: true
      description: "Can recover input tax"
      
    - name: input_recovery_percentage
      data_type: DECIMAL(5,2)
      default_value: 100.00
      description: "% of input tax recoverable"
      
    # Classification
    - name: is_default
      data_type: BOOLEAN
      default_value: false
      description: "Default scheme for new registrations"
      
    - name: is_voluntary
      data_type: BOOLEAN
      default_value: false
      description: "Can register voluntarily below threshold"
      
    - name: is_simplified
      data_type: BOOLEAN
      default_value: false
      description: "Simplified/small business scheme"

sample_data:
  # Malta VAT Schemes
  - scheme_code: MLT_VAT_ART10
    country_key: 1  # Malta
    scheme_name: "Article 10 - Standard VAT Registration"
    scheme_name_short: "Article 10"
    registration_threshold: NULL  # Voluntary or mandatory
    filing_frequency_code: MONTHLY
    allows_input_recovery: true
    input_recovery_percentage: 100.00
    is_default: true
    
  - scheme_code: MLT_VAT_ART11
    country_key: 1  # Malta
    scheme_name: "Article 11 - Small Undertaking Exemption"
    scheme_name_short: "Article 11"
    registration_threshold: 35000.00
    threshold_currency: EUR
    filing_frequency_code: QUARTERLY
    allows_input_recovery: false
    input_recovery_percentage: 0.00
    is_simplified: true
    
  # Sri Lanka VAT Schemes
  - scheme_code: LKA_VAT_STD
    country_key: 2  # Sri Lanka
    scheme_name: "Standard VAT Registration"
    registration_threshold: 80000000.00  # LKR 80M
    threshold_currency: LKR
    filing_frequency_code: MONTHLY
    allows_input_recovery: true
    is_default: true
```

### Usage in dim_registration
```yaml
# Replace Malta-specific columns
columns:
  - name: tax_scheme_key
    data_type: INT
    nullable: true
    foreign_key: dim_tax_scheme(tax_scheme_key)
    description: "Registration scheme (replaces vat_scheme_code)"
    
  # REMOVED: vat_scheme_code, article_11_turnover_limit
```

---

## Pattern P4: Generic Geographic Hierarchy

### Problem
dim_geography has Malta-specific structure (locality → district → region → island).

### Solution
Create generic level-based hierarchy with country context.

### Implementation

```yaml
dimension:
  name: dim_geography
  schema: warehouse
  description: |
    Country-agnostic geographic hierarchy supporting 1-5 administrative
    levels depending on country structure.
    
  scd_type: 1
  natural_key: [country_key, geography_code]
  surrogate_key: geography_key
  
  columns:
    - name: geography_key
      data_type: INT
      auto_increment: true
      
    - name: country_key
      data_type: INT
      nullable: false
      foreign_key: dim_country(country_key)
      
    - name: geography_code
      data_type: VARCHAR(20)
      nullable: false
      description: "Unique code within country"
      
    # Hierarchy Position
    - name: hierarchy_level
      data_type: SMALLINT
      nullable: false
      description: "Level in hierarchy (1=highest, 5=lowest)"
      allowed_values: [1, 2, 3, 4, 5]
      
    - name: parent_geography_key
      data_type: INT
      foreign_key: dim_geography(geography_key)
      description: "Parent in hierarchy"
      
    # Generic Level Attributes
    - name: level_1_code
      data_type: VARCHAR(20)
      description: "Highest admin level code"
      
    - name: level_1_name
      data_type: VARCHAR(100)
      description: "Highest admin level name"
      
    - name: level_2_code
      data_type: VARCHAR(20)
      
    - name: level_2_name
      data_type: VARCHAR(100)
      
    - name: level_3_code
      data_type: VARCHAR(20)
      
    - name: level_3_name
      data_type: VARCHAR(100)
      
    - name: level_4_code
      data_type: VARCHAR(20)
      
    - name: level_4_name
      data_type: VARCHAR(100)
      
    - name: level_5_code
      data_type: VARCHAR(20)
      description: "Lowest admin level code"
      
    - name: level_5_name
      data_type: VARCHAR(100)
      description: "Lowest admin level name"
      
    # Display
    - name: geography_name
      data_type: VARCHAR(200)
      description: "Full name at this level"
      
    - name: geography_name_local
      data_type: VARCHAR(200)
      description: "Name in local language"
      
    - name: full_path
      data_type: VARCHAR(500)
      description: "Full hierarchy path (Malta > Northern Harbour > Sliema)"
      
    # Postal
    - name: postal_code_pattern
      data_type: VARCHAR(20)
      description: "Postal code prefix/pattern"
      
    # Classification
    - name: is_urban
      data_type: BOOLEAN
      
    - name: population
      data_type: INT
      
    - name: area_sq_km
      data_type: DECIMAL(10,2)

hierarchy_definitions:
  MLT:  # Malta
    level_1: Island
    level_2: Region
    level_3: District
    level_4: Locality
    
  LKA:  # Sri Lanka
    level_1: Province
    level_2: District
    level_3: Divisional Secretariat
    level_4: GN Division
    
  MDA:  # Moldova
    level_1: Region (Raion)
    level_2: Municipality
    level_3: Commune/City
```

---

## Pattern P5: Account Subtype Dimension

### Problem
fact_refund has Malta imputation columns (mta_refund_amount, fia_refund_amount) that don't exist in other countries.

### Solution
Create dim_account_subtype to handle country-specific account variations.

### Implementation

```yaml
dimension:
  name: dim_account_subtype
  schema: warehouse
  description: |
    Country-specific tax account subtypes. Handles Malta's imputation
    accounts (FTA, MTA, FIA, IPA, UA) and similar variations.
    
  scd_type: 1
  natural_key: [country_key, account_subtype_code]
  surrogate_key: account_subtype_key
  
  columns:
    - name: account_subtype_key
      data_type: INT
      auto_increment: true
      
    - name: country_key
      data_type: INT
      foreign_key: dim_country(country_key)
      
    - name: account_subtype_code
      data_type: VARCHAR(20)
      description: "FTA, MTA, FIA, IPA, UA, STD, etc."
      
    - name: account_subtype_name
      data_type: VARCHAR(100)
      
    - name: tax_type_code
      data_type: VARCHAR(20)
      description: "Applicable tax type (CIT, PIT, etc.)"
      
    # Refund Configuration
    - name: refund_rate
      data_type: DECIMAL(5,4)
      description: "Refund rate (0.8571 for 6/7, 0.7143 for 5/7)"
      
    - name: refund_formula
      data_type: VARCHAR(100)
      description: "Human-readable formula (6/7 of underlying tax)"
      
    # Classification
    - name: is_imputation_account
      data_type: BOOLEAN
      default_value: false
      description: "TRUE for imputation system accounts"
      
    - name: sort_order
      data_type: INT
      
    - name: is_active
      data_type: BOOLEAN

sample_data:
  # Malta Imputation Accounts
  - country_key: 1  # MLT
    account_subtype_code: FTA
    account_subtype_name: "Final Tax Account"
    tax_type_code: CIT
    refund_rate: 0.0000
    is_imputation_account: true
    
  - country_key: 1
    account_subtype_code: MTA
    account_subtype_name: "Maltese Taxed Account"
    tax_type_code: CIT
    refund_rate: 0.8571  # 6/7
    refund_formula: "6/7 of underlying corporate tax"
    is_imputation_account: true
    
  - country_key: 1
    account_subtype_code: FIA
    account_subtype_name: "Foreign Income Account"
    tax_type_code: CIT
    refund_rate: 0.7143  # 5/7
    refund_formula: "5/7 of underlying corporate tax"
    is_imputation_account: true
    
  - country_key: 1
    account_subtype_code: IPA
    account_subtype_name: "Immovable Property Account"
    tax_type_code: CIT
    refund_rate: 0.6667  # 2/3
    refund_formula: "2/3 of underlying tax"
    is_imputation_account: true
    
  # Standard account for non-imputation countries
  - country_key: 2  # LKA
    account_subtype_code: STD
    account_subtype_name: "Standard Account"
    tax_type_code: CIT
    refund_rate: 0.0000
    is_imputation_account: false
```

### Usage in fact_refund

**Option A: Single Subtype FK (simpler)**
```yaml
# Add to fact_refund
dimension_keys:
  - name: account_subtype_key
    foreign_key: dim_account_subtype(account_subtype_key)
    nullable: true
    description: "Account subtype for refund allocation"

# REMOVE: mta_refund_amount, fia_refund_amount, fta_refund_amount, ipa_refund_amount
```

**Option B: Bridge for Multiple Subtypes (Malta complexity)**
```yaml
bridge_refund_account:
  description: "Allocates refund amounts across account subtypes"
  columns:
    - refund_source_id (FK)
    - account_subtype_key (FK)
    - refund_amount
    - underlying_tax_amount
    - refund_rate_applied
```

---

## Pattern P6: Configurable Fiscal Year

### Problem
dim_date hardcodes Malta's Year of Assessment (YA = Calendar Year + 1).

### Solution
Make fiscal year calculation data-driven based on country configuration.

### Implementation

```yaml
# dim_date additions
columns:
  # Keep calendar attributes as-is
  - name: calendar_year
  - name: quarter_number
  - name: month_number
  
  # Fiscal year becomes derived at query time or via view
  # DO NOT hardcode: year_of_assessment = calendar_year + 1
  
  # Optional: Store common fiscal years
  - name: fiscal_year_jan_dec
    description: "Fiscal year for Jan-Dec countries"
    derivation: "calendar_year"
    
  - name: fiscal_year_apr_mar
    description: "Fiscal year for Apr-Mar countries (Sri Lanka)"
    derivation: "CASE WHEN month_number >= 4 THEN calendar_year ELSE calendar_year - 1 END"

# Alternative: Fiscal Year Bridge/View
view: v_date_fiscal_year
sql: |
  SELECT 
      d.date_key,
      d.calendar_date,
      d.calendar_year,
      c.country_key,
      c.country_code,
      CASE c.fiscal_year_start_month
          WHEN 1 THEN d.calendar_year + c.fiscal_year_offset_years
          WHEN 4 THEN CASE WHEN d.month_number >= 4 
                           THEN d.calendar_year + c.fiscal_year_offset_years
                           ELSE d.calendar_year - 1 + c.fiscal_year_offset_years 
                      END
          -- Add other patterns as needed
      END AS fiscal_year,
      REPLACE(c.fiscal_year_label_pattern, '{YEAR}', 
              CAST(calculated_fiscal_year AS CHAR)) AS fiscal_year_label
  FROM warehouse.dim_date d
  CROSS JOIN warehouse.dim_country c
  WHERE c.is_active = TRUE
```

---

## Pattern P7: Externalized Holidays

### Problem
Malta public holidays hardcoded in dim_date YAML.

### Solution
Create ref_country_holiday reference table.

### Implementation

```yaml
reference_table:
  name: ref_country_holiday
  schema: reference
  description: "Country-specific public holidays"
  
  columns:
    - name: country_code
      data_type: VARCHAR(3)
      
    - name: holiday_date
      data_type: DATE
      description: "Actual date (for fixed and calculated holidays)"
      
    - name: holiday_code
      data_type: VARCHAR(30)
      description: "Stable identifier (NEW_YEAR, GOOD_FRIDAY, etc.)"
      
    - name: holiday_name
      data_type: VARCHAR(100)
      description: "English name"
      
    - name: holiday_name_local
      data_type: VARCHAR(100)
      description: "Name in local language"
      
    - name: is_fixed_date
      data_type: BOOLEAN
      description: "TRUE if same date every year (Jan 1)"
      
    - name: fixed_month
      data_type: SMALLINT
      description: "For fixed holidays: month (1-12)"
      
    - name: fixed_day
      data_type: SMALLINT
      description: "For fixed holidays: day (1-31)"
      
    - name: calculation_rule
      data_type: VARCHAR(100)
      description: "For calculated holidays: EASTER-2, EASTER+39, etc."
      
    - name: year
      data_type: SMALLINT
      description: "Year this record applies to"

# dim_date becomes simpler
dim_date:
  columns:
    # REMOVE: is_malta_public_holiday, malta_holiday_name, malta_holiday_name_mt
    
    # Use lookup at query time or via view
    
# View for holidays
view: v_date_with_holidays
sql: |
  SELECT 
      d.*,
      h.holiday_name,
      h.holiday_name_local,
      CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_public_holiday
  FROM warehouse.dim_date d
  LEFT JOIN reference.ref_country_holiday h 
      ON d.calendar_date = h.holiday_date
      AND h.country_code = :country_code  -- Parameterized
```

---

## Pattern P8: Localization Pattern

### Problem
Translations embedded in dimension columns (day_of_week_name_mt, malta_holiday_name_mt).

### Solution
Externalize translations to separate localization tables.

### Implementation

```yaml
reference_table:
  name: ref_localized_text
  schema: reference
  description: "Localized text for dimension values"
  
  columns:
    - name: object_type
      data_type: VARCHAR(50)
      description: "DIM_DATE, DIM_TAX_TYPE, DIM_GEOGRAPHY, etc."
      
    - name: object_key
      data_type: VARCHAR(100)
      description: "Key identifying the object (date_key, tax_type_code, etc.)"
      
    - name: attribute_name
      data_type: VARCHAR(50)
      description: "day_name, month_name, tax_type_name, etc."
      
    - name: language_code
      data_type: VARCHAR(5)
      description: "ISO language code (en, mt, si, ro)"
      
    - name: localized_value
      data_type: VARCHAR(500)
      description: "Translated text"

# Usage: Dimension stays English-only
dim_date:
  columns:
    - name: day_of_week_name  # English only
    - name: month_name        # English only
    # REMOVE: day_of_week_name_mt, month_name_mt

# Query with localization
sql: |
  SELECT 
      d.date_key,
      d.day_of_week_name,
      COALESCE(lt.localized_value, d.day_of_week_name) AS day_name_display
  FROM warehouse.dim_date d
  LEFT JOIN reference.ref_localized_text lt
      ON lt.object_type = 'DIM_DATE'
      AND lt.object_key = CAST(d.day_of_week AS CHAR)
      AND lt.attribute_name = 'day_name'
      AND lt.language_code = :user_language
```

---

## Pattern P9: Country-Conditional Measures

### Problem
Some measures only exist in certain countries (Malta imputation) but shouldn't be NULL everywhere else.

### Solution
Use bridge tables or conditional logic rather than sparse fact columns.

### Implementation Options

**Option A: Bridge Table (Recommended for Complex Cases)**
```yaml
# See Pattern P5 - bridge_refund_account
```

**Option B: Conditional Column with Default**
```yaml
# In fact table, make country-specific measures optional
- name: imputation_refund_amount
  data_type: DECIMAL(15,2)
  nullable: true
  default_value: NULL
  description: "Malta imputation refund (NULL for non-imputation countries)"
  applicable_countries: [MLT]
```

**Option C: Extension Fact Table**
```yaml
# Core fact table stays generic
fact_refund:
  measures:
    - claimed_amount
    - approved_amount
    - paid_amount

# Country-specific extension
fact_refund_malta_ext:
  description: "Malta-specific imputation detail"
  grain: "One row per refund claim (Malta only)"
  columns:
    - refund_source_id (FK)
    - mta_refund_amount
    - fia_refund_amount
    - fta_refund_amount
    - ipa_refund_amount
```

---

## Pattern P10: Bridge Table for Country Variants

### Problem
Many-to-many or variant relationships that differ by country.

### Solution
Use bridge tables with country context.

### Implementation

```yaml
# Generic bridge pattern
bridge_template:
  columns:
    - source_key  # FK to fact or dimension
    - variant_key  # FK to variant dimension
    - country_key  # Country context
    - weight  # For allocation bridges
    - is_primary
    - effective_date
    
# Example: Party-Identifier bridge
bridge_party_identifier:
  columns:
    - party_key
    - identifier_type_code
    - country_code
    - identifier_value
    - is_primary
    - is_verified
    
# Example: Refund-Account bridge
bridge_refund_account:
  columns:
    - refund_source_id
    - account_subtype_key
    - refund_amount
    - refund_rate_applied
```

---

## Pattern Application Summary

| Component | Patterns to Apply |
|-----------|-------------------|
| dim_party | P1, P2 |
| dim_date | P1, P6, P7, P8 |
| dim_tax_type | P1, P8 |
| dim_tax_period | P1, P6 |
| dim_registration | P1, P3 |
| dim_geography | P1, P4 |
| fact_refund | P5, P9 |
| fact_filing | P1 |
| All dimensions | P1 (country_key) |

---

## Implementation Checklist

- [ ] Create dim_country (P1)
- [ ] Add country_key to all country-sensitive dimensions (P1)
- [ ] Generalize dim_party identifiers (P2)
- [ ] Create dim_tax_scheme (P3)
- [ ] Generalize dim_geography (P4)
- [ ] Create dim_account_subtype (P5)
- [ ] Make fiscal year configurable (P6)
- [ ] Externalize holidays (P7)
- [ ] Extract translations (P8)
- [ ] Generalize fact_refund measures (P9)
- [ ] Update ETL processes

---

*End of L3 Generalization Patterns Catalog*
