# TA-RDM L2 Country Extension Guide

## Document Control

| Attribute | Value |
|-----------|-------|
| **Version** | 1.0.0 |
| **Date** | 2025-12-10 |
| **Audience** | Implementers, Data Architects, System Integrators |
| **Prerequisites** | L2-00-model-overview.md |
| **Step** | 2f of 6 (Final - L2 Generalization) |

---

## 1. Introduction

### 1.1 Purpose

This guide explains how to implement the TA-RDM L2 Canonical Model for a specific country or jurisdiction. The generic L2 model provides a country-agnostic foundation that requires country-specific configuration to become operational.

The L2 model is designed with a "configure, don't code" philosophy — most country-specific requirements are handled through reference data loading rather than schema modifications.

### 1.2 Scope

This guide covers:

| Activity | Description |
|----------|-------------|
| **System Configuration** | Setting country-specific parameters |
| **Reference Data Loading** | Adding geographic, identifier, and tax reference data |
| **Identifier Configuration** | Setting up TIN, VAT, and other identifier formats |
| **Tax Scheme Setup** | Configuring registration schemes and thresholds |
| **Extension Tables** | Creating country-specific tables (when necessary) |
| **Validation** | Verifying complete and correct configuration |

### 1.3 Prerequisites

Before starting country implementation:

- [ ] L2 schema fully deployed (all 12 domains, ~112 tables)
- [ ] Reference data tables exist (empty or with universal codes only)
- [ ] Database user with INSERT/UPDATE privileges on reference schema
- [ ] Country tax legislation documented and analyzed
- [ ] Identifier formats and validation rules documented
- [ ] Tax types and filing frequencies documented

### 1.4 Configuration Philosophy

```
┌─────────────────────────────────────────────────────────────────┐
│                 Configuration Approach                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PREFERRED: Configure via Reference Data                        │
│  ─────────────────────────────────────────                      │
│  • Add rows to ref_identifier_type                              │
│  • Add rows to ref_tax_scheme                                   │
│  • Add country-specific codes to existing ref_* tables          │
│  • Load tax types, rates, periods                               │
│                                                                 │
│  WHEN NECESSARY: Create Extension Tables                        │
│  ─────────────────────────────────────────                      │
│  • Only when unique business rules cannot be accommodated       │
│  • Use 1:1 FK pattern linking to core tables                    │
│  • Create in country-specific schema                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Implementation Steps Overview

| Step | Description | Effort | Dependencies |
|------|-------------|--------|--------------|
| 1 | Configure System Parameters | 0.5 day | None |
| 2 | Load Geographic Reference Data | 0.5 day | Step 1 |
| 3 | Configure Identifier Types | 1 day | Step 2 |
| 4 | Load Tax Types and Categories | 1 day | Step 2 |
| 5 | Configure Tax Schemes | 1 day | Steps 3, 4 |
| 6 | Load Operational Reference Data | 2 days | Steps 4, 5 |
| 7 | Create Extension Tables (if needed) | Variable | Step 6 |
| 8 | Validate Configuration | 1 day | All steps |

**Total Estimated Effort:** 7-10 days per country (excluding extension tables)

```
Timeline Visualization:
Day 1: ████ Steps 1-2 (System & Geographic)
Day 2: ████████ Step 3 (Identifiers)
Day 3: ████████ Step 4 (Tax Types)
Day 4: ████████ Step 5 (Tax Schemes)
Day 5-6: ████████████████ Step 6 (Operational Reference Data)
Day 7+: ████████ Step 7 (Extensions - if needed)
Day 8: ████████ Step 8 (Validation)
```

---

## 3. Step 1: Configure System Parameters

System parameters define the country context for all operations.

### 3.1 Required Parameters

```sql
-- =============================================================================
-- SYSTEM PARAMETERS - Core Country Configuration
-- =============================================================================

-- Country identification
INSERT INTO reference.system_parameter 
(parameter_code, parameter_category, data_type, parameter_value, description, effective_from)
VALUES
('COUNTRY_CODE', 'SYSTEM', 'STRING', 'MLT', 'Primary country code (ISO 3166-1 alpha-3)', '2000-01-01'),
('COUNTRY_NAME', 'SYSTEM', 'STRING', 'Malta', 'Country name in English', '2000-01-01'),
('COUNTRY_NAME_LOCAL', 'SYSTEM', 'STRING', 'Malta', 'Country name in local language', '2000-01-01'),
('DEFAULT_CURRENCY', 'SYSTEM', 'STRING', 'EUR', 'Default currency code (ISO 4217)', '2008-01-01'),
('DEFAULT_LANGUAGE', 'SYSTEM', 'STRING', 'en', 'Default language code (ISO 639-1)', '2000-01-01'),
('SECONDARY_LANGUAGE', 'SYSTEM', 'STRING', 'mt', 'Secondary language (Maltese)', '2000-01-01'),
('TIMEZONE', 'SYSTEM', 'STRING', 'Europe/Malta', 'Default timezone (IANA)', '2000-01-01');

-- Fiscal year configuration
INSERT INTO reference.system_parameter VALUES
('FISCAL_YEAR_START_MONTH', 'TAX_FRAMEWORK', 'INTEGER', '1', 'Fiscal year start month (1=January)', '2000-01-01'),
('FISCAL_YEAR_START_DAY', 'TAX_FRAMEWORK', 'INTEGER', '1', 'Fiscal year start day', '2000-01-01'),
('TAX_YEAR_TYPE', 'TAX_FRAMEWORK', 'STRING', 'CALENDAR', 'CALENDAR or FISCAL or YOA (Year of Assessment)', '2000-01-01'),
('YOA_BASIS', 'TAX_FRAMEWORK', 'STRING', 'PRECEDING', 'Year of Assessment basis: CURRENT or PRECEDING', '2000-01-01');

-- Display format configuration
INSERT INTO reference.system_parameter VALUES
('DATE_FORMAT', 'DISPLAY', 'STRING', 'DD/MM/YYYY', 'Display date format', '2000-01-01'),
('DATE_FORMAT_ISO', 'DISPLAY', 'STRING', 'YYYY-MM-DD', 'Storage date format (always ISO)', '2000-01-01'),
('DECIMAL_SEPARATOR', 'DISPLAY', 'STRING', '.', 'Decimal separator', '2000-01-01'),
('THOUSANDS_SEPARATOR', 'DISPLAY', 'STRING', ',', 'Thousands separator', '2000-01-01'),
('CURRENCY_SYMBOL_POSITION', 'DISPLAY', 'STRING', 'BEFORE', 'BEFORE or AFTER amount', '2000-01-01');

-- Tax-specific rates (main rates - details in tax_rate table)
INSERT INTO reference.system_parameter VALUES
('VAT_STANDARD_RATE', 'VAT', 'DECIMAL', '18.00', 'Current standard VAT rate (%)', '2024-01-01'),
('VAT_REDUCED_RATE_1', 'VAT', 'DECIMAL', '7.00', 'First reduced VAT rate (%)', '2024-01-01'),
('VAT_REDUCED_RATE_2', 'VAT', 'DECIMAL', '5.00', 'Second reduced VAT rate (%)', '2024-01-01'),
('CIT_STANDARD_RATE', 'INCOME_TAX', 'DECIMAL', '35.00', 'Corporate income tax rate (%)', '2007-01-01'),
('PIT_MAX_RATE', 'INCOME_TAX', 'DECIMAL', '35.00', 'Maximum personal income tax rate (%)', '2024-01-01');

-- Penalty and interest configuration
INSERT INTO reference.system_parameter VALUES
('LATE_FILING_PENALTY_RATE', 'PENALTY', 'DECIMAL', '1.00', 'Monthly late filing penalty (%)', '2020-01-01'),
('LATE_PAYMENT_INTEREST_RATE', 'INTEREST', 'DECIMAL', '0.54', 'Monthly late payment interest (%)', '2020-01-01'),
('INTEREST_CALCULATION_METHOD', 'INTEREST', 'STRING', 'SIMPLE', 'SIMPLE or COMPOUND', '2000-01-01');
```

### 3.2 Sri Lanka Example

```sql
-- Sri Lanka system parameters
INSERT INTO reference.system_parameter VALUES
('COUNTRY_CODE', 'SYSTEM', 'STRING', 'LKA', 'Primary country code', '2000-01-01'),
('COUNTRY_NAME', 'SYSTEM', 'STRING', 'Sri Lanka', 'Country name in English', '2000-01-01'),
('COUNTRY_NAME_LOCAL', 'SYSTEM', 'STRING', 'ශ්‍රී ලංකාව', 'Country name in Sinhala', '2000-01-01'),
('DEFAULT_CURRENCY', 'SYSTEM', 'STRING', 'LKR', 'Sri Lankan Rupee', '2000-01-01'),
('DEFAULT_LANGUAGE', 'SYSTEM', 'STRING', 'en', 'Default language', '2000-01-01'),
('SECONDARY_LANGUAGE', 'SYSTEM', 'STRING', 'si', 'Secondary language (Sinhala)', '2000-01-01'),
('TERTIARY_LANGUAGE', 'SYSTEM', 'STRING', 'ta', 'Tertiary language (Tamil)', '2000-01-01'),
('VAT_STANDARD_RATE', 'VAT', 'DECIMAL', '18.00', 'Standard VAT rate (effective 2024)', '2024-01-01');
```

### 3.3 Validation

```sql
-- Verify all required system parameters exist
SELECT parameter_category, COUNT(*) as param_count
FROM reference.system_parameter 
WHERE parameter_category IN ('SYSTEM', 'TAX_FRAMEWORK', 'DISPLAY', 'VAT', 'INCOME_TAX')
GROUP BY parameter_category
ORDER BY parameter_category;

-- Expected minimum counts:
-- SYSTEM: 7+
-- TAX_FRAMEWORK: 4+
-- DISPLAY: 5+
-- VAT: 3+
-- INCOME_TAX: 2+
```

---

## 4. Step 2: Load Geographic Reference Data

### 4.1 Country and Currency

```sql
-- =============================================================================
-- GEOGRAPHIC REFERENCE DATA
-- =============================================================================

-- Ensure country exists in ref_country
INSERT INTO reference.ref_country 
(country_code, country_name, country_name_local, iso_alpha_2, currency_code, 
 eu_member, eu_vat_member, customs_union_member, is_active)
VALUES 
('MLT', 'Malta', 'Malta', 'MT', 'EUR', TRUE, TRUE, TRUE, TRUE)
ON DUPLICATE KEY UPDATE 
  country_name = VALUES(country_name),
  eu_member = VALUES(eu_member);

-- Ensure currency exists
INSERT INTO reference.ref_currency 
(currency_code, currency_name, symbol, decimal_places, is_active)
VALUES 
('EUR', 'Euro', '€', 2, TRUE)
ON DUPLICATE KEY UPDATE currency_name = VALUES(currency_name);
```

### 4.2 Administrative Divisions

```sql
-- Malta regions (Level 1)
INSERT INTO reference.ref_administrative_division 
(country_code, division_level, division_code, division_name, division_name_local, 
 parent_division_code, display_order, is_active)
VALUES
('MLT', 1, 'MLT_SOUTH', 'Southern Region', 'Reġjun tan-Nofsinhar', NULL, 1, TRUE),
('MLT', 1, 'MLT_NORTH', 'Northern Region', 'Reġjun tat-Tramuntana', NULL, 2, TRUE),
('MLT', 1, 'MLT_SOUTHEAST', 'South Eastern Region', 'Reġjun tal-Lbiċ', NULL, 3, TRUE),
('MLT', 1, 'MLT_CENTRAL', 'Central Region', 'Reġjun Ċentrali', NULL, 4, TRUE),
('MLT', 1, 'MLT_HARBOUR', 'Harbour Region', 'Reġjun tal-Port', NULL, 5, TRUE),
('MLT', 1, 'MLT_GOZO', 'Gozo and Comino', 'Għawdex u Kemmuna', NULL, 6, TRUE);

-- Malta localities (Level 2) - showing subset
INSERT INTO reference.ref_administrative_division VALUES
('MLT', 2, 'MLT_VALLETTA', 'Valletta', 'Valletta', 'MLT_HARBOUR', 1, TRUE),
('MLT', 2, 'MLT_SLIEMA', 'Sliema', 'Tas-Sliema', 'MLT_HARBOUR', 2, TRUE),
('MLT', 2, 'MLT_STPAUL', 'St. Paul''s Bay', 'San Pawl il-Baħar', 'MLT_NORTH', 3, TRUE),
('MLT', 2, 'MLT_BIRKIRKARA', 'Birkirkara', 'Birkirkara', 'MLT_CENTRAL', 4, TRUE),
('MLT', 2, 'MLT_MOSTA', 'Mosta', 'Il-Mosta', 'MLT_CENTRAL', 5, TRUE),
('MLT', 2, 'MLT_VICTORIA', 'Victoria (Rabat)', 'Ir-Rabat, Għawdex', 'MLT_GOZO', 1, TRUE);
-- Continue for all 68 localities...
```

### 4.3 Sri Lanka Example - Provinces and Districts

```sql
-- Sri Lanka provinces (Level 1)
INSERT INTO reference.ref_administrative_division VALUES
('LKA', 1, 'LKA_WP', 'Western Province', 'බස්නාහිර පළාත', NULL, 1, TRUE),
('LKA', 1, 'LKA_CP', 'Central Province', 'මධ්‍යම පළාත', NULL, 2, TRUE),
('LKA', 1, 'LKA_SP', 'Southern Province', 'දකුණු පළාත', NULL, 3, TRUE),
('LKA', 1, 'LKA_NP', 'Northern Province', 'வட மாகாணம்', NULL, 4, TRUE),
('LKA', 1, 'LKA_EP', 'Eastern Province', 'கிழக்கு மாகாணம்', NULL, 5, TRUE),
('LKA', 1, 'LKA_NW', 'North Western Province', 'වයඹ පළාත', NULL, 6, TRUE),
('LKA', 1, 'LKA_NC', 'North Central Province', 'උතුරු මැද පළාත', NULL, 7, TRUE),
('LKA', 1, 'LKA_UVA', 'Uva Province', 'ඌව පළාත', NULL, 8, TRUE),
('LKA', 1, 'LKA_SAB', 'Sabaragamuwa Province', 'සබරගමුව පළාත', NULL, 9, TRUE);

-- Sri Lanka districts (Level 2) - showing Western Province
INSERT INTO reference.ref_administrative_division VALUES
('LKA', 2, 'LKA_CMB', 'Colombo', 'කොළඹ', 'LKA_WP', 1, TRUE),
('LKA', 2, 'LKA_GAM', 'Gampaha', 'ගම්පහ', 'LKA_WP', 2, TRUE),
('LKA', 2, 'LKA_KAL', 'Kalutara', 'කළුතර', 'LKA_WP', 3, TRUE);
-- Continue for all 25 districts...
```

---

## 5. Step 3: Configure Identifier Types

Identifier types define how TINs, VAT numbers, and other identifiers are formatted and validated for each country.

### 5.1 Malta Identifier Types

```sql
-- =============================================================================
-- IDENTIFIER TYPES - Malta
-- =============================================================================

INSERT INTO reference.ref_identifier_type 
(identifier_type_code, country_code, identifier_name, identifier_name_local,
 format_regex, format_description, format_example, 
 min_length, max_length, checksum_algorithm,
 is_tax_identifier, is_unique_per_party, allows_multiple_countries,
 issuing_authority, issuing_authority_url, applicable_party_types,
 effective_from, display_order, is_active, notes)
VALUES
-- Primary Tax Identification Number (TIN)
('TIN', 'MLT', 
 'Tax Identification Number', 
 'Numru ta'' Identifikazzjoni tat-Taxxa',
 '^[0-9]{9}$', 
 '9 digits',
 '123456789',
 9, 9, NULL,
 TRUE, TRUE, FALSE,
 'Commissioner for Revenue', 'https://cfr.gov.mt',
 'ALL',
 '2000-01-01', 10, TRUE,
 'Primary tax identifier for all Malta taxpayers. Issued by CFR upon registration.'),

-- Malta ID Card Number (for Maltese individuals)
('NATID', 'MLT',
 'Malta ID Card Number',
 'Numru tal-Karta tal-Identità',
 '^[0-9]{1,7}[A-Z]$',
 '1-7 digits followed by a check letter',
 '123456M',
 2, 8, 'malta_id',
 FALSE, TRUE, FALSE,
 'Identity Malta', 'https://identitymalta.com',
 'INDIVIDUAL',
 '1990-01-01', 20, TRUE,
 'Malta ID card uses modulo 23 check letter algorithm'),

-- VAT Registration Number
('VAT', 'MLT',
 'VAT Registration Number',
 'Numru tar-Reġistrazzjoni tal-VAT',
 '^MT[0-9]{8}$',
 'MT prefix followed by 8 digits',
 'MT12345678',
 10, 10, 'mod97',
 FALSE, TRUE, FALSE,
 'Commissioner for Revenue - VAT Department', 'https://cfr.gov.mt',
 'ALL',
 '2004-05-01', 30, TRUE,
 'EU VAT format. Issued upon VAT registration under Article 10, 11, or 12.'),

-- Social Security Number
('SSC', 'MLT',
 'Social Security Number',
 'Numru tas-Sigurtà Soċjali',
 '^[0-9]{1,7}[A-Z]$',
 'Same format as ID card',
 '123456M',
 2, 8, 'malta_id',
 FALSE, TRUE, FALSE,
 'Department of Social Security', 'https://socialsecurity.gov.mt',
 'INDIVIDUAL',
 '1990-01-01', 40, TRUE,
 'Usually same as Malta ID card number'),

-- Company Registration Number
('CRN', 'MLT',
 'Company Registration Number',
 'Numru tar-Reġistrazzjoni tal-Kumpanija',
 '^C[0-9]{1,6}$',
 'C followed by up to 6 digits',
 'C12345',
 2, 7, NULL,
 FALSE, TRUE, FALSE,
 'Malta Business Registry', 'https://mbr.mt',
 'ENTERPRISE',
 '1995-01-01', 50, TRUE,
 'ROC registration number for limited companies'),

-- EORI Number (Customs)
('EORI', 'MLT',
 'EORI Number',
 'Numru EORI',
 '^MT[0-9A-Z]{1,15}$',
 'MT followed by up to 15 alphanumeric',
 'MT12345678901',
 3, 17, NULL,
 FALSE, TRUE, TRUE,
 'Malta Customs', 'https://customs.gov.mt',
 'ALL',
 '2009-07-01', 60, TRUE,
 'Economic Operators Registration and Identification. Valid across EU.');
```

### 5.2 Sri Lanka Identifier Types

```sql
-- =============================================================================
-- IDENTIFIER TYPES - Sri Lanka
-- =============================================================================

INSERT INTO reference.ref_identifier_type VALUES
-- Primary Tax Identification Number
('TIN', 'LKA',
 'Tax Identification Number',
 'බදු හඳුනාගැනීමේ අංකය',
 '^[0-9]{9}$',
 '9 digits',
 '123456789',
 9, 9, NULL,
 TRUE, TRUE, FALSE,
 'Inland Revenue Department', 'https://ird.gov.lk',
 'ALL',
 '2000-01-01', 10, TRUE,
 'Primary tax identifier issued by IRD. Required for all tax registrations.'),

-- Old NIC Format (pre-2016)
('NIC_OLD', 'LKA',
 'National Identity Card (Old Format)',
 'ජාතික හැඳුනුම්පත',
 '^[0-9]{9}[VXvx]$',
 '9 digits followed by V or X',
 '901234567V',
 10, 10, NULL,
 FALSE, TRUE, FALSE,
 'Department for Registration of Persons', 'https://drp.gov.lk',
 'INDIVIDUAL',
 '1970-01-01', 20, TRUE,
 'Old format: First 2 digits = year of birth, V=male, X=female. Valid for citizens born before 2016.'),

-- New NIC Format (2016+)
('NIC_NEW', 'LKA',
 'National Identity Card (New Format)',
 'ජාතික හැඳුනුම්පත (නව)',
 '^[0-9]{12}$',
 '12 digits',
 '200012345678',
 12, 12, NULL,
 FALSE, TRUE, FALSE,
 'Department for Registration of Persons', 'https://drp.gov.lk',
 'INDIVIDUAL',
 '2016-01-01', 21, TRUE,
 'New format: First 4 = year, next 3 = day of year (500+ for female). Mandatory for new registrations.'),

-- Business Registration Number
('BRN', 'LKA',
 'Business Registration Number',
 'ව්‍යාපාර ලියාපදිංචි අංකය',
 '^(PV|PB|GA)[0-9]+$',
 'Prefix (PV/PB/GA) followed by digits',
 'PV12345',
 4, 15, NULL,
 FALSE, TRUE, FALSE,
 'Department of Registrar of Companies', 'https://drc.gov.lk',
 'ENTERPRISE',
 '2000-01-01', 30, TRUE,
 'PV=Private Limited, PB=Public Limited, GA=Guarantee Company'),

-- VAT Registration Number
('VAT', 'LKA',
 'VAT Registration Number',
 'VAT ලියාපදිංචි අංකය',
 '^[0-9]{9}-[0-9]{4}$',
 'TIN followed by dash and 4-digit suffix',
 '123456789-0000',
 14, 14, NULL,
 FALSE, TRUE, FALSE,
 'Inland Revenue Department', 'https://ird.gov.lk',
 'ALL',
 '2002-01-01', 40, TRUE,
 'Based on TIN with VAT-specific suffix. Threshold LKR 80M annually (2024).');
```

### 5.3 Universal Identifier Types

```sql
-- =============================================================================
-- UNIVERSAL IDENTIFIER TYPES (country_code = NULL)
-- =============================================================================

INSERT INTO reference.ref_identifier_type VALUES
-- Passport (universal)
('PASSPORT', NULL,
 'Passport Number',
 NULL,
 '^[A-Z0-9]{5,20}$',
 '5-20 alphanumeric characters',
 'AB1234567',
 5, 20, NULL,
 FALSE, FALSE, TRUE,
 NULL, NULL,
 'INDIVIDUAL',
 '1900-01-01', 100, TRUE,
 'International travel document. Format varies by issuing country.'),

-- EU EORI (universal pattern)
('EORI', NULL,
 'EORI Number',
 NULL,
 '^[A-Z]{2}[0-9A-Z]{1,15}$',
 '2-letter country + up to 15 alphanumeric',
 'MT12345678901',
 3, 17, NULL,
 FALSE, FALSE, TRUE,
 'EU Customs', NULL,
 'ALL',
 '2009-07-01', 110, TRUE,
 'Economic Operators Registration and Identification. Valid across EU customs union.');
```

### 5.4 Validation

```sql
-- Check identifier types for country
SELECT identifier_type_code, identifier_name, is_tax_identifier, format_regex
FROM reference.ref_identifier_type
WHERE country_code = 'MLT' OR country_code IS NULL
ORDER BY country_code NULLS LAST, display_order;

-- Verify exactly one primary tax identifier per country
SELECT country_code, COUNT(*) as tax_id_count
FROM reference.ref_identifier_type
WHERE is_tax_identifier = TRUE
  AND is_active = TRUE
  AND country_code IS NOT NULL
GROUP BY country_code
HAVING COUNT(*) != 1;
-- Expected: 0 rows (should have exactly 1 per country)

-- Test format regex validity (application-level)
SELECT identifier_type_code, country_code, format_example,
       CASE WHEN format_example REGEXP format_regex THEN 'PASS' ELSE 'FAIL' END as validation
FROM reference.ref_identifier_type
WHERE format_regex IS NOT NULL AND format_example IS NOT NULL;
```

---

## 6. Step 4: Load Tax Types and Categories

### 6.1 Tax Categories

```sql
-- =============================================================================
-- TAX CATEGORIES - Universal + Country-Specific
-- =============================================================================

-- Universal categories (OECD-aligned)
INSERT INTO reference.ref_tax_category 
(tax_category_code, country_code, category_name, category_name_local,
 oecd_classification, parent_category_code, display_order, is_active)
VALUES
('DIRECT', NULL, 'Direct Taxes', NULL, '1000', NULL, 10, TRUE),
('INCOME', NULL, 'Income and Profits', NULL, '1100', 'DIRECT', 11, TRUE),
('PAYROLL', NULL, 'Payroll and Workforce', NULL, '2000', NULL, 20, TRUE),
('SSC', NULL, 'Social Security Contributions', NULL, '2100', 'PAYROLL', 21, TRUE),
('PROPERTY', NULL, 'Property Taxes', NULL, '4000', NULL, 30, TRUE),
('INDIRECT', NULL, 'Indirect Taxes', NULL, '5000', NULL, 40, TRUE),
('VAT', NULL, 'Value Added Tax', NULL, '5111', 'INDIRECT', 41, TRUE),
('EXCISE', NULL, 'Excise Duties', NULL, '5121', 'INDIRECT', 42, TRUE),
('CUSTOMS', NULL, 'Customs and Import Duties', NULL, '5123', NULL, 50, TRUE),
('OTHER', NULL, 'Other Taxes', NULL, '6000', NULL, 90, TRUE)
ON DUPLICATE KEY UPDATE category_name = VALUES(category_name);

-- Malta-specific categories
INSERT INTO reference.ref_tax_category VALUES
('GAMING', 'MLT', 'Gaming Taxes', 'Taxxi fuq l-Imħatri', '5126', 'INDIRECT', 60, TRUE),
('ENVIRON', 'MLT', 'Environmental Levies', 'Imposti Ambjentali', '5200', 'INDIRECT', 70, TRUE),
('IMPUTATION', 'MLT', 'Imputation Credit System', 'Sistema ta'' Kreditu ta'' Imputazzjoni', NULL, 'INCOME', 12, TRUE);
```

### 6.2 Malta Tax Types

```sql
-- =============================================================================
-- TAX TYPES - Malta
-- =============================================================================

INSERT INTO tax_framework.tax_type
(tax_type_code, country_code, tax_type_name, tax_type_name_local,
 tax_category_code, parent_tax_type_code, legal_reference,
 filing_frequency_code, standard_rate, effective_from, is_active)
VALUES
-- Direct Taxes: Income
('CIT', 'MLT', 'Corporate Income Tax', 'Taxxa fuq l-Introjtu Korporattiv',
 'INCOME', NULL, 'Income Tax Act (Cap. 123)',
 'ANNUAL', 35.00, '2007-01-01', TRUE),

('PIT', 'MLT', 'Personal Income Tax', 'Taxxa fuq l-Introjtu Personali',
 'INCOME', NULL, 'Income Tax Act (Cap. 123)',
 'ANNUAL', 35.00, '2000-01-01', TRUE),

-- Withholding Taxes (under PIT/CIT)
('WHT', 'MLT', 'Withholding Tax', 'Taxxa Miżmuma',
 'INCOME', NULL, 'Income Tax Act (Cap. 123)',
 'MONTHLY', NULL, '2000-01-01', TRUE),

('PAYE', 'MLT', 'Pay As You Earn', 'PAYE',
 'INCOME', 'WHT', 'Income Tax Act (Cap. 123), FSS Rules',
 'MONTHLY', NULL, '2000-01-01', TRUE),

('FSS', 'MLT', 'Final Settlement System', 'FSS',
 'INCOME', 'WHT', 'Income Tax Act (Cap. 123), FSS Rules',
 'MONTHLY', NULL, '2000-01-01', TRUE),

('WHT_INT', 'MLT', 'Withholding Tax - Interest', 'WHT fuq l-Imgħax',
 'INCOME', 'WHT', 'Income Tax Act (Cap. 123)',
 'MONTHLY', 15.00, '2000-01-01', TRUE),

('WHT_DIV', 'MLT', 'Withholding Tax - Dividends', 'WHT fuq id-Dividendi',
 'INCOME', 'WHT', 'Income Tax Act (Cap. 123)',
 'TRANSACTION', NULL, '2000-01-01', TRUE),

-- Indirect Taxes: VAT
('VAT', 'MLT', 'Value Added Tax', 'Taxxa fuq il-Valur Miżjud',
 'VAT', NULL, 'VAT Act (Cap. 406)',
 'QUARTERLY', 18.00, '2004-05-01', TRUE),

-- Customs & Excise
('CUSTOMS', 'MLT', 'Customs Duty', 'Dazju Doganali',
 'CUSTOMS', NULL, 'Customs Ordinance (Cap. 37)',
 'TRANSACTION', NULL, '2004-05-01', TRUE),

('EXCISE', 'MLT', 'Excise Duty', 'Dazju tas-Sisa',
 'EXCISE', NULL, 'Excise Duty Act (Cap. 382)',
 'MONTHLY', NULL, '2004-05-01', TRUE),

-- Property
('STAMP', 'MLT', 'Stamp Duty', 'Dazju tal-Bolla',
 'PROPERTY', NULL, 'Duty on Documents and Transfers Act (Cap. 364)',
 'TRANSACTION', 5.00, '2000-01-01', TRUE),

('PTT', 'MLT', 'Property Transfer Tax', 'Taxxa fuq it-Trasferiment tal-Proprjetà',
 'PROPERTY', NULL, 'Duty on Documents and Transfers Act (Cap. 364)',
 'TRANSACTION', 5.00, '2000-01-01', TRUE),

('CGT', 'MLT', 'Capital Gains Tax', 'Taxxa fuq il-Qligħ Kapitali',
 'PROPERTY', NULL, 'Income Tax Act (Cap. 123)',
 'TRANSACTION', 8.00, '2000-01-01', TRUE),

-- Social Security
('SSC', 'MLT', 'Social Security Contributions', 'Kontribuzzjonijiet tas-Sigurtà Soċjali',
 'SSC', NULL, 'Social Security Act (Cap. 318)',
 'MONTHLY', NULL, '2000-01-01', TRUE),

('MAT', 'MLT', 'Maternity Fund Contribution', 'Kontribuzzjoni għall-Fond tal-Maternità',
 'SSC', 'SSC', 'Social Security Act (Cap. 318)',
 'MONTHLY', 0.30, '2000-01-01', TRUE),

-- Gaming (Malta-specific)
('GAMING', 'MLT', 'Gaming Tax', 'Taxxa tal-Logħob',
 'GAMING', NULL, 'Gaming Act (Cap. 583)',
 'MONTHLY', NULL, '2018-08-01', TRUE),

('GAMING_CC', 'MLT', 'Gaming Compliance Contribution', 'Kontribuzzjoni ta'' Konformità tal-Logħob',
 'GAMING', 'GAMING', 'Gaming Act (Cap. 583)',
 'ANNUAL', NULL, '2018-08-01', TRUE),

-- Environmental (Malta-specific)
('ECO_PROD', 'MLT', 'Eco-Contribution (Products)', 'Eko-Kontribuzzjoni (Prodotti)',
 'ENVIRON', NULL, 'Eco-Contribution Act (Cap. 473)',
 'QUARTERLY', NULL, '2010-01-01', TRUE),

('ECO_ACCOM', 'MLT', 'Eco-Contribution (Accommodation)', 'Eko-Kontribuzzjoni (Akkomodazzjoni)',
 'ENVIRON', NULL, 'Eco-Contribution Act (Cap. 473)',
 'QUARTERLY', 0.50, '2016-06-01', TRUE);
```

### 6.3 Sri Lanka Tax Types

```sql
-- =============================================================================
-- TAX TYPES - Sri Lanka
-- =============================================================================

INSERT INTO tax_framework.tax_type VALUES
-- Income Taxes
('CIT', 'LKA', 'Corporate Income Tax', 'ආයතනික ආදායම් බද්ද',
 'INCOME', NULL, 'Inland Revenue Act No. 24 of 2017',
 'ANNUAL', 30.00, '2018-04-01', TRUE),

('PIT', 'LKA', 'Personal Income Tax', 'පුද්ගල ආදායම් බද්ද',
 'INCOME', NULL, 'Inland Revenue Act No. 24 of 2017',
 'ANNUAL', 36.00, '2018-04-01', TRUE),

-- Withholding Taxes
('WHT', 'LKA', 'Withholding Tax', 'රඳවා ගත් බද්ද',
 'INCOME', NULL, 'Inland Revenue Act No. 24 of 2017',
 'MONTHLY', NULL, '2018-04-01', TRUE),

('PAYE', 'LKA', 'Pay As You Earn', 'PAYE',
 'INCOME', 'WHT', 'Inland Revenue Act No. 24 of 2017',
 'MONTHLY', NULL, '2018-04-01', TRUE),

-- VAT
('VAT', 'LKA', 'Value Added Tax', 'එකතු කළ අගය මත බද්ද',
 'VAT', NULL, 'Value Added Tax Act No. 14 of 2002',
 'MONTHLY', 18.00, '2024-01-01', TRUE),

('VAT_FS', 'LKA', 'VAT on Financial Services', 'මූල්‍ය සේවා VAT',
 'VAT', 'VAT', 'Value Added Tax Act, Section 25C',
 'QUARTERLY', 18.00, '2018-04-01', TRUE),

-- Customs
('CUSTOMS', 'LKA', 'Customs Duty', 'රේගු බද්ද',
 'CUSTOMS', NULL, 'Customs Ordinance',
 'TRANSACTION', NULL, '2000-01-01', TRUE),

('PAL', 'LKA', 'Ports and Airports Levy', 'වරාය සහ ගුවන්තොටුපොළ බද්ද',
 'CUSTOMS', 'CUSTOMS', 'Ports and Airports Development Levy Act',
 'TRANSACTION', 5.00, '2018-04-01', TRUE);
```

---

## 7. Step 5: Configure Tax Schemes

Tax schemes define registration rules, thresholds, and filing requirements for each tax type.

### 7.1 Malta VAT Schemes

```sql
-- =============================================================================
-- TAX SCHEMES - Malta VAT
-- =============================================================================

INSERT INTO reference.ref_tax_scheme
(scheme_code, country_code, tax_type_code, scheme_name, scheme_name_local,
 scheme_description, legal_reference, legal_reference_url,
 is_default, is_voluntary, is_mandatory,
 eligible_party_types, registration_threshold, registration_threshold_currency, 
 registration_threshold_period, exit_threshold,
 filing_frequency_code, return_due_days, payment_due_days,
 allows_input_recovery, input_recovery_percentage,
 requires_tax_invoices, simplified_invoice_threshold,
 requires_intrastat, intrastat_threshold_arrivals, intrastat_threshold_dispatches,
 requires_recapitulative, oss_eligible,
 effective_from, display_order, is_active)
VALUES
-- Article 10: Standard VAT Registration
('MLT_VAT_ART10', 'MLT', 'VAT',
 'Article 10 - Standard VAT Registration',
 'Artikolu 10 - Reġistrazzjoni Standard tal-VAT',
 'Full VAT registration with input tax recovery. Mandatory when turnover exceeds €20,000. 
  Quarterly filing with full invoicing and reporting requirements.',
 'VAT Act (Cap. 406), Article 10',
 'https://legislation.mt/eli/cap/406',
 TRUE, TRUE, TRUE,  -- default, voluntary below threshold, mandatory above
 'ALL',
 20000.00, 'EUR', '12M', 16000.00,  -- threshold with hysteresis
 'QUARTERLY', 45, 45,  -- due 45 days after period end
 TRUE, 100.00,  -- full input recovery
 TRUE, 100.00,  -- tax invoices required, simplified under €100
 TRUE, 700000.00, 700000.00,  -- Intrastat thresholds
 TRUE, TRUE,  -- recapitulative statement, OSS eligible
 '2024-01-01', 10, TRUE),

-- Article 11: Small Undertakings Exemption
('MLT_VAT_ART11', 'MLT', 'VAT',
 'Article 11 - Small Undertakings Exemption',
 'Artikolu 11 - Eżenzjoni għall-Impriżi Żgħar',
 'Exempt from VAT registration for businesses below threshold. No input tax recovery.
  Annual declaration only. Cannot issue tax invoices.',
 'VAT Act (Cap. 406), Article 11',
 'https://legislation.mt/eli/cap/406',
 FALSE, TRUE, FALSE,  -- not default, voluntary, not mandatory
 'ALL',
 20000.00, 'EUR', '12M', NULL,  -- must stay below threshold
 'ANNUAL', 90, NULL,  -- declaration by March 31
 FALSE, 0.00,  -- no input recovery
 FALSE, NULL,  -- no tax invoices
 FALSE, NULL, NULL,
 FALSE, FALSE,
 '2024-01-01', 20, TRUE),

-- Article 12: Intra-Community Acquisitions Only
('MLT_VAT_ART12', 'MLT', 'VAT',
 'Article 12 - Intra-Community Acquisitions',
 'Artikolu 12 - Akkwisti Intra-Komunitarji Biss',
 'For businesses making intra-community acquisitions above threshold but no domestic 
  taxable supplies. Full input recovery on IC acquisitions.',
 'VAT Act (Cap. 406), Article 12',
 'https://legislation.mt/eli/cap/406',
 FALSE, TRUE, TRUE,  -- mandatory when IC acquisitions exceed threshold
 'ALL',
 10000.00, 'EUR', '12M', 8000.00,  -- IC acquisition threshold
 'QUARTERLY', 45, 45,
 TRUE, 100.00,  -- input recovery on IC acquisitions
 TRUE, 100.00,
 TRUE, 700000.00, 700000.00,
 TRUE, FALSE,  -- recapitulative yes, OSS no
 '2024-01-01', 30, TRUE),

-- Exempt Supplies
('MLT_VAT_EXEMPT', 'MLT', 'VAT',
 'Exempt Supplies Registration',
 'Reġistrazzjoni għall-Provvisti Eżenti',
 'For businesses making only exempt supplies (e.g., financial services, insurance, 
  education, healthcare). No input recovery.',
 'VAT Act (Cap. 406), Fifth Schedule',
 'https://legislation.mt/eli/cap/406',
 FALSE, FALSE, FALSE,
 'ALL',
 NULL, 'EUR', NULL, NULL,
 'ANNUAL', 90, NULL,
 FALSE, 0.00,
 FALSE, NULL,
 FALSE, NULL, NULL,
 FALSE, FALSE,
 '2024-01-01', 40, TRUE);
```

### 7.2 Sri Lanka VAT Schemes

```sql
-- =============================================================================
-- TAX SCHEMES - Sri Lanka VAT
-- =============================================================================

INSERT INTO reference.ref_tax_scheme VALUES
-- Standard VAT
('LKA_VAT_STD', 'LKA', 'VAT',
 'Standard VAT Registration',
 'සම්මත VAT ලියාපදිංචිය',
 'Mandatory registration when quarterly turnover exceeds LKR 20M or annual exceeds LKR 80M.
  Monthly filing for zero-rated suppliers, quarterly for others.',
 'Value Added Tax Act No. 14 of 2002',
 'https://ird.gov.lk',
 TRUE, TRUE, TRUE,
 'ALL',
 80000000.00, 'LKR', '12M', 60000000.00,  -- LKR 80M annual threshold
 'QUARTERLY', 30, 20,  -- file by month end, pay by 20th
 TRUE, 100.00,
 TRUE, 50000.00,  -- simplified invoice under LKR 50,000
 FALSE, NULL, NULL,  -- no Intrastat (not EU)
 FALSE, FALSE,
 '2024-01-01', 10, TRUE),

-- SVAT (Simplified VAT) - ABOLISHED October 2025
('LKA_VAT_SVAT', 'LKA', 'VAT',
 'Simplified VAT (SVAT) - ABOLISHED',
 'සරල කරන ලද VAT',
 'ABOLISHED effective October 1, 2025. Historical scheme for suspended VAT between 
  Registered Identified Purchasers (RIP) and Suppliers (RIS).',
 'Gazette Extraordinary No. 1986/9',
 NULL,
 FALSE, FALSE, FALSE,
 'ALL',
 NULL, 'LKR', NULL, NULL,
 'MONTHLY', 30, 20,
 TRUE, 100.00,
 TRUE, NULL,
 FALSE, NULL, NULL,
 FALSE, FALSE,
 '2016-09-27', 20, FALSE),  -- is_active = FALSE
 
-- Tourism VAT Scheme
('LKA_VAT_TOUR', 'LKA', 'VAT',
 'Tourism VAT Scheme',
 'සංචාරක VAT යෝජනා ක්‍රමය',
 'Special scheme for registered tourism establishments providing services to 
  foreign tourists. Zero-rated on eligible services.',
 'Value Added Tax Act, Section 22(7)',
 NULL,
 FALSE, TRUE, FALSE,
 'ENTERPRISE',
 NULL, 'LKR', NULL, NULL,
 'MONTHLY', 30, 20,
 TRUE, 100.00,
 TRUE, NULL,
 FALSE, NULL, NULL,
 FALSE, FALSE,
 '2018-04-01', 30, TRUE);
```

### 7.3 Validation

```sql
-- Verify only one default scheme per country/tax_type
SELECT country_code, tax_type_code, COUNT(*) as default_count
FROM reference.ref_tax_scheme
WHERE is_default = TRUE AND is_active = TRUE
GROUP BY country_code, tax_type_code
HAVING COUNT(*) != 1;
-- Expected: 0 rows

-- Check threshold consistency (exit <= registration)
SELECT scheme_code, registration_threshold, exit_threshold
FROM reference.ref_tax_scheme
WHERE exit_threshold IS NOT NULL
  AND exit_threshold > registration_threshold;
-- Expected: 0 rows

-- List all schemes for a country
SELECT scheme_code, scheme_name, is_default, filing_frequency_code, 
       registration_threshold, allows_input_recovery
FROM reference.ref_tax_scheme
WHERE country_code = 'MLT' AND tax_type_code = 'VAT' AND is_active = TRUE
ORDER BY display_order;
```

---

## 8. Step 6: Load Operational Reference Data

### 8.1 Assessment Types

```sql
-- =============================================================================
-- ASSESSMENT TYPES - Universal + Country-Specific
-- =============================================================================

-- Universal types
INSERT INTO reference.ref_assessment_type
(assessment_type_code, country_code, assessment_type_name, description,
 is_taxpayer_initiated, is_appealable, display_order, is_active)
VALUES
('SELF', NULL, 'Self-Assessment', 'Assessment based on taxpayer-filed return', TRUE, FALSE, 10, TRUE),
('AMENDED', NULL, 'Amended Assessment', 'Correction to prior self-assessment', TRUE, TRUE, 20, TRUE),
('AUDIT', NULL, 'Audit Assessment', 'Assessment resulting from audit/examination', FALSE, TRUE, 30, TRUE),
('ESTIMATED', NULL, 'Estimated Assessment', 'Administrative estimate for non-filers', FALSE, TRUE, 40, TRUE),
('PROVISIONAL', NULL, 'Provisional Assessment', 'Interim assessment pending final', FALSE, TRUE, 50, TRUE),
('JEOPARDY', NULL, 'Jeopardy Assessment', 'Urgent assessment to protect revenue', FALSE, TRUE, 60, TRUE)
ON DUPLICATE KEY UPDATE assessment_type_name = VALUES(assessment_type_name);

-- Malta-specific
INSERT INTO reference.ref_assessment_type VALUES
('CFC', 'MLT', 'CFC Assessment', 'Controlled Foreign Company assessment', FALSE, TRUE, 70, TRUE),
('IMPUTATION', 'MLT', 'Imputation Credit Assessment', 'Assessment of shareholder imputation credits', FALSE, TRUE, 80, TRUE),
('TRANSFER_PRICE', 'MLT', 'Transfer Pricing Adjustment', 'Assessment from TP audit', FALSE, TRUE, 90, TRUE);

-- Sri Lanka-specific
INSERT INTO reference.ref_assessment_type VALUES
('RAMIS_AUTO', 'LKA', 'RAMIS Auto-Assessment', 'System-generated assessment from RAMIS', FALSE, FALSE, 70, TRUE),
('SVAT_CONV', 'LKA', 'SVAT Conversion Assessment', 'Assessment from SVAT scheme abolition', FALSE, TRUE, 80, TRUE);
```

### 8.2 Penalty Types

```sql
-- Universal penalty types
INSERT INTO reference.ref_penalty_type
(penalty_type_code, country_code, penalty_type_name, calculation_method, 
 penalty_rate, max_penalty_rate, legal_reference, is_active)
VALUES
('LATE_FILING', NULL, 'Late Filing Penalty', 'PERCENTAGE_PER_MONTH', 1.00, 25.00, NULL, TRUE),
('LATE_PAYMENT', NULL, 'Late Payment Penalty', 'PERCENTAGE_OF_TAX', 5.00, NULL, NULL, TRUE),
('UNDERSTATEMENT', NULL, 'Understatement Penalty', 'PERCENTAGE_OF_DEFICIENCY', 20.00, NULL, NULL, TRUE),
('FRAUD', NULL, 'Fraud Penalty', 'PERCENTAGE_OF_TAX', 75.00, NULL, NULL, TRUE),
('NON_FILING', NULL, 'Non-Filing Penalty', 'FIXED_AMOUNT', NULL, NULL, NULL, TRUE)
ON DUPLICATE KEY UPDATE penalty_type_name = VALUES(penalty_type_name);

-- Malta-specific
INSERT INTO reference.ref_penalty_type VALUES
('ART11_EXIT', 'MLT', 'Article 11 Exit Penalty', 'PERCENTAGE_OF_TAX', 10.00, NULL, 'VAT Act Article 11(5)', TRUE),
('FSS_LATE', 'MLT', 'FSS Late Submission', 'FIXED_AMOUNT', 50.00, 500.00, 'FSS Rules', TRUE);
```

### 8.3 Payment Methods

```sql
-- Universal payment methods
INSERT INTO reference.ref_payment_method
(payment_method_code, country_code, payment_method_name, is_electronic,
 is_real_time, processing_days, is_active)
VALUES
('CASH', NULL, 'Cash Payment', FALSE, TRUE, 0, TRUE),
('CHEQUE', NULL, 'Cheque/Check', FALSE, FALSE, 3, TRUE),
('BANK_TRANSFER', NULL, 'Bank Transfer', TRUE, FALSE, 1, TRUE),
('CARD', NULL, 'Debit/Credit Card', TRUE, TRUE, 0, TRUE),
('DIRECT_DEBIT', NULL, 'Direct Debit', TRUE, FALSE, 2, TRUE)
ON DUPLICATE KEY UPDATE payment_method_name = VALUES(payment_method_name);

-- Malta-specific
INSERT INTO reference.ref_payment_method VALUES
('CFR_PORTAL', 'MLT', 'CFR Online Portal', TRUE, TRUE, 0, TRUE),
('BOV_ONLINE', 'MLT', 'BOV Internet Banking', TRUE, FALSE, 1, TRUE),
('HSBC_ONLINE', 'MLT', 'HSBC Internet Banking', TRUE, FALSE, 1, TRUE),
('APS_ONLINE', 'MLT', 'APS Internet Banking', TRUE, FALSE, 1, TRUE);

-- Sri Lanka-specific
INSERT INTO reference.ref_payment_method VALUES
('RAMIS_ONLINE', 'LKA', 'RAMIS Online Payment', TRUE, TRUE, 0, TRUE),
('LANKA_PAY', 'LKA', 'LankaPay', TRUE, TRUE, 0, TRUE),
('BOC_ONLINE', 'LKA', 'Bank of Ceylon Online', TRUE, FALSE, 1, TRUE),
('PEOPLES_ONLINE', 'LKA', 'People''s Bank Online', TRUE, FALSE, 1, TRUE);
```

### 8.4 Document Types

```sql
-- Malta-specific document types
INSERT INTO reference.ref_document_type
(document_type_code, country_code, document_type_name, category,
 applicable_tax_types, retention_years, is_taxpayer_generated, is_active)
VALUES
('TRA61', 'MLT', 'TRA 61 - Corporate Tax Return', 'TAX_RETURN', 'CIT', 10, TRUE, TRUE),
('TRA62', 'MLT', 'TRA 62 - Personal Tax Return', 'TAX_RETURN', 'PIT', 10, TRUE, TRUE),
('TRA63', 'MLT', 'TRA 63 - Tax Account Allocation', 'TAX_RETURN', 'CIT', 10, TRUE, TRUE),
('TRA64', 'MLT', 'TRA 64 - Dividend Distribution', 'TAX_RETURN', 'WHT_DIV', 10, TRUE, TRUE),
('FS3', 'MLT', 'FS3 - Employer Monthly Return', 'TAX_RETURN', 'FSS', 7, TRUE, TRUE),
('FS5', 'MLT', 'FS5 - Annual Employer Reconciliation', 'TAX_RETURN', 'FSS', 7, TRUE, TRUE),
('FS7', 'MLT', 'FS7 - Employee Annual Statement', 'STATEMENT', 'FSS', 7, FALSE, TRUE),
('VAT_RETURN', 'MLT', 'VAT Return', 'TAX_RETURN', 'VAT', 10, TRUE, TRUE),
('EC_SALES', 'MLT', 'EC Sales List', 'REPORT', 'VAT', 10, TRUE, TRUE);

-- Sri Lanka-specific
INSERT INTO reference.ref_document_type VALUES
('TPR_002_E', 'LKA', 'TPR 002 - Taxpayer Registration', 'REGISTRATION', 'ALL', 10, TRUE, TRUE),
('TPR_005_E', 'LKA', 'TPR 005 - Tax Type Registration', 'REGISTRATION', 'ALL', 10, TRUE, TRUE),
('VAT_RETURN', 'LKA', 'Standard VAT Return', 'TAX_RETURN', 'VAT', 10, TRUE, TRUE),
('VATFS', 'LKA', 'VAT on Financial Services Return', 'TAX_RETURN', 'VAT_FS', 10, TRUE, TRUE),
('SVAT_02', 'LKA', 'SVAT Form 02 - Suspended Invoice', 'INVOICE', 'VAT', 10, TRUE, FALSE),
('SVAT_04', 'LKA', 'SVAT Form 04 - Transaction Submission', 'TAX_RETURN', 'VAT', 10, TRUE, FALSE),
('SVAT_06', 'LKA', 'SVAT Form 06 - RIP Monthly Summary', 'TAX_RETURN', 'VAT', 10, TRUE, FALSE),
('SVAT_07', 'LKA', 'SVAT Form 07 - RIS Monthly Summary', 'TAX_RETURN', 'VAT', 10, TRUE, FALSE);
```

---

## 9. Step 7: Create Extension Tables (If Needed)

### 9.1 When to Create Extension Tables

Create country-specific extension tables **only when**:

| Scenario | Example | Solution |
|----------|---------|----------|
| Unique business rules | Malta 5-account imputation system | Extension table |
| Additional data elements | Malta shareholder register for refunds | Extension table |
| Special calculations | Malta refund percentages (6/7, 5/7, 2/3) | Extension table + calculation rules |
| Country workflow | Sri Lanka SVAT dual-approval | Process tables (if not covered by core) |

**Do NOT create extension tables for:**
- Data that can be stored in existing columns with different reference codes
- Variations that can be handled by system parameters
- Simple additional fields (use JSON column in core table if necessary)

### 9.2 Extension Table Naming Convention

```
Schema: {country_code}_{domain}
Table:  {descriptive_name}

Examples:
- malta_corporate.corporate_tax_account
- malta_corporate.shareholder_register
- malta_corporate.dividend_distribution
- lka_customs.cusdec_extended
```

### 9.3 Malta Corporate Imputation Extension Tables

Malta's unique 5-account corporate imputation system requires extension tables:

```sql
-- =============================================================================
-- MALTA EXTENSION: Corporate Imputation System
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS malta_corporate;

-- Corporate Tax Account (extends registration.tax_account)
CREATE TABLE malta_corporate.corporate_tax_account (
    corporate_tax_account_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Link to core tax_account (1:1 relationship)
    tax_account_id BIGINT NOT NULL,
    CONSTRAINT fk_cta_tax_account 
        FOREIGN KEY (tax_account_id) 
        REFERENCES registration.tax_account(tax_account_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT uk_cta_tax_account UNIQUE (tax_account_id),
    
    -- Year of Assessment
    year_of_assessment SMALLINT NOT NULL,
    
    -- Final Tax Account (FTA) - distributed from profits taxed at 35%
    fta_opening_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fta_additions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fta_distributions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fta_closing_balance DECIMAL(15,2) GENERATED ALWAYS AS 
        (fta_opening_balance + fta_additions - fta_distributions) STORED,
    
    -- Maltese Taxed Account (MTA) - domestic Malta-source income
    mta_opening_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    mta_additions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    mta_distributions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    mta_closing_balance DECIMAL(15,2) GENERATED ALWAYS AS 
        (mta_opening_balance + mta_additions - mta_distributions) STORED,
    
    -- Foreign Income Account (FIA) - foreign source income
    fia_opening_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fia_additions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fia_distributions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fia_closing_balance DECIMAL(15,2) GENERATED ALWAYS AS 
        (fia_opening_balance + fia_additions - fia_distributions) STORED,
    
    -- Immovable Property Account (IPA) - Malta property income
    ipa_opening_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ipa_additions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ipa_distributions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ipa_closing_balance DECIMAL(15,2) GENERATED ALWAYS AS 
        (ipa_opening_balance + ipa_additions - ipa_distributions) STORED,
    
    -- Untaxed Account (UA) - untaxed income (exempt, foreign exempt)
    ua_opening_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ua_additions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ua_distributions DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ua_closing_balance DECIMAL(15,2) GENERATED ALWAYS AS 
        (ua_opening_balance + ua_additions - ua_distributions) STORED,
    
    -- Total distributable profits
    total_distributable DECIMAL(15,2) GENERATED ALWAYS AS (
        fta_closing_balance + mta_closing_balance + fia_closing_balance + 
        ipa_closing_balance + ua_closing_balance
    ) STORED,
    
    -- Status
    is_finalized BOOLEAN NOT NULL DEFAULT FALSE,
    finalized_date DATE,
    
    -- Audit columns
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP,
    
    -- Unique constraint per tax account per year
    CONSTRAINT uk_cta_year UNIQUE (tax_account_id, year_of_assessment)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Malta imputation system - five tax accounts per company per year';

-- Dividend Distribution (links to payment_refund domain)
CREATE TABLE malta_corporate.dividend_distribution (
    distribution_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Source company
    corporate_tax_account_id BIGINT NOT NULL,
    CONSTRAINT fk_dd_cta 
        FOREIGN KEY (corporate_tax_account_id)
        REFERENCES malta_corporate.corporate_tax_account(corporate_tax_account_id),
    
    -- Distribution details
    distribution_date DATE NOT NULL,
    distribution_type VARCHAR(20) NOT NULL COMMENT 'DIVIDEND, LIQUIDATION, REDEMPTION',
    gross_amount DECIMAL(15,2) NOT NULL,
    
    -- Allocation from accounts (must sum to gross_amount)
    fta_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    mta_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    fia_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ipa_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    ua_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    
    -- Shareholder receiving distribution
    shareholder_party_id BIGINT NOT NULL,
    CONSTRAINT fk_dd_shareholder 
        FOREIGN KEY (shareholder_party_id)
        REFERENCES party.party(party_id),
    
    -- Refund eligibility
    refund_percentage_67 DECIMAL(15,2) COMMENT '6/7 refund (MTA, FIA standard)',
    refund_percentage_57 DECIMAL(15,2) COMMENT '5/7 refund (FIA participations)',
    refund_percentage_23 DECIMAL(15,2) COMMENT '2/3 refund (IPA)',
    total_refund_eligible DECIMAL(15,2),
    
    -- Audit
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Validation
    CONSTRAINT chk_dd_allocation CHECK (
        ABS((fta_amount + mta_amount + fia_amount + ipa_amount + ua_amount) - gross_amount) < 0.01
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Malta dividend distributions with account allocation';

-- Index for common queries
CREATE INDEX idx_cta_year ON malta_corporate.corporate_tax_account(year_of_assessment);
CREATE INDEX idx_dd_shareholder ON malta_corporate.dividend_distribution(shareholder_party_id);
CREATE INDEX idx_dd_date ON malta_corporate.dividend_distribution(distribution_date);
```

### 9.4 Extension Table Best Practices

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   Extension Table Guidelines                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. ALWAYS link to core table via 1:1 FK                               │
│     corporate_tax_account.tax_account_id → registration.tax_account    │
│                                                                         │
│  2. Include standard audit columns                                      │
│     created_by, created_date, modified_by, modified_date               │
│                                                                         │
│  3. Use generated columns for calculated totals                        │
│     GENERATED ALWAYS AS (...) STORED                                   │
│                                                                         │
│  4. Document with table and column comments                            │
│     COMMENT 'Purpose description'                                      │
│                                                                         │
│  5. Add validation constraints                                         │
│     CHECK constraints for business rules                               │
│                                                                         │
│  6. Create appropriate indexes                                         │
│     On FK columns and common query patterns                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Step 8: Validate Configuration

### 10.1 Comprehensive Validation Checklist

```sql
-- =============================================================================
-- VALIDATION QUERIES
-- Run after completing all configuration steps
-- =============================================================================

-- Set the country being validated
SET @country = 'MLT';

-- -----------------------------------------------------------------------------
-- 1. SYSTEM PARAMETERS
-- -----------------------------------------------------------------------------
SELECT 'System Parameters' as check_name,
       CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM reference.system_parameter 
WHERE parameter_category = 'SYSTEM';

-- Check required parameters exist
SELECT 'Required Parameters' as check_name,
       CASE WHEN SUM(CASE WHEN parameter_code IS NOT NULL THEN 1 ELSE 0 END) = 5 
            THEN 'PASS' ELSE 'FAIL' END as status
FROM reference.system_parameter
WHERE parameter_code IN ('COUNTRY_CODE', 'COUNTRY_NAME', 'DEFAULT_CURRENCY', 
                         'DEFAULT_LANGUAGE', 'TIMEZONE');

-- -----------------------------------------------------------------------------
-- 2. COUNTRY EXISTS
-- -----------------------------------------------------------------------------
SELECT 'Country Exists' as check_name,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END as status
FROM reference.ref_country 
WHERE country_code = @country;

-- -----------------------------------------------------------------------------
-- 3. IDENTIFIER TYPES
-- -----------------------------------------------------------------------------
SELECT 'Identifier Types Configured' as check_name,
       CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM reference.ref_identifier_type 
WHERE country_code = @country AND is_active = TRUE;

-- Check primary tax identifier exists
SELECT 'Primary Tax Identifier' as check_name,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END as status
FROM reference.ref_identifier_type 
WHERE country_code = @country 
  AND is_tax_identifier = TRUE 
  AND is_active = TRUE;

-- Validate format examples match regex
SELECT identifier_type_code,
       format_example,
       format_regex,
       CASE WHEN format_example REGEXP format_regex THEN 'PASS' ELSE 'FAIL' END as regex_test
FROM reference.ref_identifier_type
WHERE country_code = @country
  AND format_example IS NOT NULL
  AND format_regex IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 4. TAX TYPES
-- -----------------------------------------------------------------------------
SELECT 'Tax Types Loaded' as check_name,
       CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM tax_framework.tax_type 
WHERE country_code = @country AND is_active = TRUE;

-- Check core tax types exist
SELECT 'Core Tax Types' as check_name,
       CASE WHEN COUNT(*) >= 4 THEN 'PASS' ELSE 'FAIL' END as status
FROM tax_framework.tax_type
WHERE country_code = @country
  AND tax_type_code IN ('CIT', 'PIT', 'VAT', 'WHT')
  AND is_active = TRUE;

-- -----------------------------------------------------------------------------
-- 5. TAX SCHEMES
-- -----------------------------------------------------------------------------
SELECT 'Tax Schemes Configured' as check_name,
       CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM reference.ref_tax_scheme 
WHERE country_code = @country AND is_active = TRUE;

-- Check for exactly one default per tax type
SELECT 'Default Schemes' as check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL - Multiple defaults' END as status
FROM (
    SELECT tax_type_code, COUNT(*) as cnt
    FROM reference.ref_tax_scheme
    WHERE country_code = @country 
      AND is_default = TRUE 
      AND is_active = TRUE
    GROUP BY tax_type_code
    HAVING COUNT(*) > 1
) t;

-- -----------------------------------------------------------------------------
-- 6. REFERENCE DATA COMPLETENESS
-- -----------------------------------------------------------------------------
SELECT 'Assessment Types' as check_name,
       CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM reference.ref_assessment_type 
WHERE country_code = @country OR country_code IS NULL;

SELECT 'Penalty Types' as check_name,
       CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM reference.ref_penalty_type 
WHERE country_code = @country OR country_code IS NULL;

SELECT 'Payment Methods' as check_name,
       CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END as status,
       COUNT(*) as count
FROM reference.ref_payment_method 
WHERE country_code = @country OR country_code IS NULL;

-- -----------------------------------------------------------------------------
-- 7. DATA QUALITY
-- -----------------------------------------------------------------------------
-- Check temporal validity
SELECT 'Temporal Validity' as check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM reference.ref_tax_scheme
WHERE effective_to IS NOT NULL 
  AND effective_to < effective_from;

-- Check threshold consistency
SELECT 'Threshold Consistency' as check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM reference.ref_tax_scheme
WHERE exit_threshold IS NOT NULL 
  AND exit_threshold > registration_threshold;
```

### 10.2 Validation Summary Query

```sql
-- =============================================================================
-- CONFIGURATION SUMMARY REPORT
-- =============================================================================

SELECT 
    'Configuration Summary for ' || @country as report_title,
    NOW() as generated_at;

SELECT category, item, count, status
FROM (
    SELECT 1 as seq, 'System' as category, 'Parameters' as item, 
           (SELECT COUNT(*) FROM reference.system_parameter WHERE parameter_category IN ('SYSTEM','TAX_FRAMEWORK')) as count,
           CASE WHEN count >= 10 THEN '✓' ELSE '✗' END as status
    UNION ALL
    SELECT 2, 'Geographic', 'Country Record', 
           (SELECT COUNT(*) FROM reference.ref_country WHERE country_code = @country),
           CASE WHEN count = 1 THEN '✓' ELSE '✗' END
    UNION ALL
    SELECT 3, 'Geographic', 'Admin Divisions',
           (SELECT COUNT(*) FROM reference.ref_administrative_division WHERE country_code = @country),
           CASE WHEN count >= 5 THEN '✓' ELSE '○' END
    UNION ALL
    SELECT 4, 'Identifiers', 'Identifier Types',
           (SELECT COUNT(*) FROM reference.ref_identifier_type WHERE country_code = @country),
           CASE WHEN count >= 3 THEN '✓' ELSE '✗' END
    UNION ALL
    SELECT 5, 'Identifiers', 'Primary Tax ID',
           (SELECT COUNT(*) FROM reference.ref_identifier_type WHERE country_code = @country AND is_tax_identifier = TRUE),
           CASE WHEN count = 1 THEN '✓' ELSE '✗' END
    UNION ALL
    SELECT 6, 'Tax Framework', 'Tax Types',
           (SELECT COUNT(*) FROM tax_framework.tax_type WHERE country_code = @country),
           CASE WHEN count >= 5 THEN '✓' ELSE '✗' END
    UNION ALL
    SELECT 7, 'Tax Framework', 'Tax Schemes',
           (SELECT COUNT(*) FROM reference.ref_tax_scheme WHERE country_code = @country AND is_active = TRUE),
           CASE WHEN count >= 1 THEN '✓' ELSE '✗' END
    UNION ALL
    SELECT 8, 'Reference', 'Assessment Types',
           (SELECT COUNT(*) FROM reference.ref_assessment_type WHERE country_code = @country OR country_code IS NULL),
           CASE WHEN count >= 5 THEN '✓' ELSE '○' END
    UNION ALL
    SELECT 9, 'Reference', 'Payment Methods',
           (SELECT COUNT(*) FROM reference.ref_payment_method WHERE country_code = @country OR country_code IS NULL),
           CASE WHEN count >= 3 THEN '✓' ELSE '○' END
    UNION ALL
    SELECT 10, 'Reference', 'Document Types',
           (SELECT COUNT(*) FROM reference.ref_document_type WHERE country_code = @country),
           CASE WHEN count >= 5 THEN '✓' ELSE '○' END
) summary
ORDER BY seq;

-- Legend: ✓ = Required/Complete, ✗ = Missing/Failed, ○ = Optional/Partial
```

---

## 11. Country Implementation Examples

### 11.1 Malta Implementation Summary

| Component | Count | Key Items | Notes |
|-----------|-------|-----------|-------|
| System Parameters | 20+ | Currency EUR, bilingual EN/MT | Calendar year basis |
| Identifier Types | 6 | TIN, NATID, VAT, SSC, CRN, EORI | Malta ID has check letter |
| Tax Categories | 3 country-specific | GAMING, ENVIRON, IMPUTATION | + universal categories |
| Tax Types | 18+ | CIT, PIT, VAT, FSS, GAMING, etc. | With hierarchy |
| Tax Schemes | 4 VAT schemes | ART10, ART11, ART12, EXEMPT | Default: ART10 |
| Extension Tables | 2 | corporate_tax_account, dividend_distribution | Imputation system |
| Reference Records | ~600 | All categories | Including 68 localities |

### 11.2 Sri Lanka Implementation Summary

| Component | Count | Key Items | Notes |
|-----------|-------|-----------|-------|
| System Parameters | 15+ | Currency LKR, trilingual EN/SI/TA | Calendar year |
| Identifier Types | 5 | TIN, NIC_OLD, NIC_NEW, BRN, VAT | NIC format changed 2016 |
| Tax Categories | 0 country-specific | Uses universal only | Simpler structure |
| Tax Types | 8 | CIT, PIT, VAT, VAT_FS, PAYE, etc. | Flatter hierarchy |
| Tax Schemes | 3 VAT schemes | STD, SVAT (abolished), TOUR | Default: STD |
| Extension Tables | 0 | None required | Core model sufficient |
| Reference Records | ~400 | All categories | Including 25 districts |

### 11.3 Comparison

| Aspect | Malta | Sri Lanka |
|--------|-------|-----------|
| **Complexity** | High (imputation system) | Medium |
| **Extension Tables** | Required | Not required |
| **Languages** | 2 (EN, MT) | 3 (EN, SI, TA) |
| **VAT Schemes** | 4 (EU aligned) | 3 (1 abolished) |
| **Identifier Complexity** | Check letter algorithm | Format change (old/new NIC) |
| **EU Integration** | Yes (Intrastat, EC Sales) | No |
| **Estimated Effort** | 10-12 days | 7-8 days |

---

## 12. Implementation Checklist

### 12.1 Pre-Implementation

- [ ] L2 schema fully deployed and verified
- [ ] Database access with appropriate privileges
- [ ] Country tax legislation reviewed and summarized
- [ ] Tax types and hierarchy documented
- [ ] Identifier formats and validation rules documented
- [ ] Registration schemes and thresholds documented
- [ ] Stakeholder sign-off on requirements

### 12.2 Configuration Steps

| Step | Task | Status | Validator | Date |
|------|------|--------|-----------|------|
| 1 | System parameters configured | ☐ | | |
| 2 | Geographic data loaded | ☐ | | |
| 3 | Identifier types configured | ☐ | | |
| 4 | Tax types and categories loaded | ☐ | | |
| 5 | Tax schemes configured | ☐ | | |
| 6 | Assessment types loaded | ☐ | | |
| 7 | Penalty types loaded | ☐ | | |
| 8 | Interest types loaded | ☐ | | |
| 9 | Payment methods loaded | ☐ | | |
| 10 | Document types loaded | ☐ | | |
| 11 | Filing statuses loaded | ☐ | | |
| 12 | Compliance statuses loaded | ☐ | | |
| 13 | Extension tables created (if needed) | ☐ | | |

### 12.3 Validation Steps

- [ ] All validation queries pass
- [ ] Primary tax identifier configured correctly
- [ ] Default scheme exists per tax type
- [ ] Format regex validated with examples
- [ ] Temporal validity constraints satisfied
- [ ] Threshold consistency verified

### 12.4 Post-Implementation

- [ ] Sample data inserted and verified
- [ ] ETL source-to-target mapping documented
- [ ] Application code updated for country support
- [ ] User acceptance testing completed
- [ ] Documentation updated
- [ ] Go-live approval obtained

---

## 13. Troubleshooting

### 13.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| FK violation on tax_type insert | Parent type not loaded | Load parent tax types first in hierarchy order |
| Duplicate identifier type | Same code+country exists | Use `ON DUPLICATE KEY UPDATE` or delete first |
| Missing default scheme | No `is_default=TRUE` | Set exactly one scheme per tax_type as default |
| Regex validation fails | Invalid regex syntax | Test regex with online tool, escape special chars |
| Threshold validation fails | exit > registration | Ensure `exit_threshold <= registration_threshold` |
| Circular FK reference | Self-referencing parent | Insert with NULL parent first, update after |

### 13.2 Diagnostic Queries

```sql
-- Find orphaned tax types (invalid parent reference)
SELECT t.tax_type_code, t.parent_tax_type_code
FROM tax_framework.tax_type t
LEFT JOIN tax_framework.tax_type p ON t.parent_tax_type_code = p.tax_type_code
WHERE t.parent_tax_type_code IS NOT NULL AND p.tax_type_code IS NULL;

-- Find duplicate identifier types
SELECT identifier_type_code, country_code, COUNT(*)
FROM reference.ref_identifier_type
GROUP BY identifier_type_code, country_code
HAVING COUNT(*) > 1;

-- Find schemes without required fields
SELECT scheme_code
FROM reference.ref_tax_scheme
WHERE scheme_name IS NULL 
   OR filing_frequency_code IS NULL;
```

---

## 14. Related Documents

| Document | Purpose |
|----------|---------|
| `L2-00-model-overview.md` | Model architecture and patterns |
| `ref_identifier_type.yaml` | Identifier type specification |
| `ref_tax_scheme.yaml` | Tax scheme specification |
| `party_identifier.yaml` | Multi-identifier table spec |
| `L2-reference-tables-country-code-additions.yaml` | Reference table modifications |
| `{country}-implementation-notes.md` | Country-specific implementation notes |
| `ta-rdm-mysql-ddl.sql` | Complete DDL script |

---

## 15. Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-12-10 | Initial release |
| | | - Complete 8-step implementation guide |
| | | - Malta and Sri Lanka examples |
| | | - Extension table guidance |
| | | - Validation queries and checklists |

---

*End of L2 Country Extension Guide*

*Generated: 2025-12-10 | TA-RDM Version: 2.0.0*
