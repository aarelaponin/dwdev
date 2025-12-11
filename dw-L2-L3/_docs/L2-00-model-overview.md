# TA-RDM L2 Canonical Model Overview

## Document Control

| Attribute | Value |
|-----------|-------|
| **Version** | 2.0.0 |
| **Date** | 2025-12-10 |
| **Status** | Production Ready |
| **Model Code** | TA-RDM L2 |
| **Step** | 2e of 6 (L2 Generalization) |

---

## 1. Executive Summary

### 1.1 Purpose

The Tax Administration Reference Data Model (TA-RDM) Layer 2 Canonical Model provides a **country-agnostic data schema** for storing tax administration data from any jurisdiction worldwide. It serves as the normalized operational data store between raw source system extracts and analytical dimensional models.

### 1.2 Key Characteristics

| Characteristic | Description |
|----------------|-------------|
| **12 Business Domains** | Covering all core tax administration functions |
| **~112 Tables** | Comprehensive entity coverage with relationships |
| **Country-Agnostic Core** | All country variations via reference data, not schema |
| **Temporal Support** | Historical tracking via valid_from/valid_to patterns |
| **Audit Trail** | Created/modified tracking on all transactional data |
| **Multi-Identifier** | Flexible identifier management via party_identifier |

### 1.3 Design Principles

| Principle | Description |
|-----------|-------------|
| **TTT Principle** | TIN-Tax Type-Tax Period as foundational structure for all tax operations |
| **Country-Agnostic** | All country variations through reference data configuration, not schema changes |
| **Extension Pattern** | 1:1 FK extension tables for tax-specific domains (VAT, CIT, WHT) |
| **Temporal Validity** | effective_from/to on all master data for point-in-time queries |
| **Form as Metadata** | Form structures stored as data in tax_framework, not hardcoded |
| **Multi-Identifier** | Parties can have multiple identifier types (TIN, VAT, EORI, etc.) |

### 1.4 Supported Countries

The L2 model includes reference data configurations for:

| Country | Code | Key Tax Types | Status |
|---------|------|---------------|--------|
| Malta | MLT | VAT (Art 10/11/12), CIT, PAYE, Gaming | Production |
| Sri Lanka | LKA | VAT, SVAT, CIT, PIT, WHT | Implemented |
| Moldova | MDA | VAT, CIT, PIT | Configured |
| Lebanon | LBN | VAT, CIT | Configured |
| Ukraine | UKR | VAT | Configured |
| *Universal* | NULL | All base patterns | Foundation |

---

## 2. Architecture Overview

### 2.1 Layer Context

The L2 Canonical Model sits within a multi-layer data architecture:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Data Architecture Layers                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  L0: RAW           Source system extracts (unchanged, immutable)       │
│       │                                                                 │
│       ▼                                                                 │
│  L1: STAGING       Cleaned, typed, deduplicated, conformed             │
│       │                                                                 │
│       ▼                                                                 │
│  L2: CANONICAL     TA-RDM Normalized Model  ◄── THIS DOCUMENT          │
│       │            • 3NF normalized schema                              │
│       │            • Country-agnostic core                              │
│       │            • Operational store                                  │
│       ▼                                                                 │
│  L3: WAREHOUSE     Dimensional Model (Star Schema)                     │
│       │            • Kimball methodology                                │
│       │            • Fact and dimension tables                          │
│       ▼                                                                 │
│  L4: MARTS         Subject-specific analytical views                   │
│                    • Compliance dashboards                              │
│                    • Risk analytics                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Domain Architecture

The 12 domains are organized with clear dependencies:

```
                         ┌──────────────────┐
                         │    REFERENCE     │
                         │   (09-reference) │
                         │    58 tables     │
                         └────────┬─────────┘
                                  │ Referenced by all domains
         ┌────────────────────────┼────────────────────────┐
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│      PARTY      │     │  TAX FRAMEWORK  │     │    DOCUMENT     │
│   (01-party)    │     │ (02-framework)  │     │ (08-document)   │
│   12 tables     │     │   12 tables     │     │   13 tables     │
└────────┬────────┘     └────────┬────────┘     └─────────────────┘
         │                       │
         │    ┌──────────────────┤
         │    │                  │
         ▼    ▼                  ▼
┌─────────────────┐     ┌─────────────────┐
│  REGISTRATION   │     │   FILING &      │
│ (03-registration│     │   ASSESSMENT    │
│   9 tables      │     │ (04-filing)     │
└────────┬────────┘     │   6 tables      │
         │              └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│   ACCOUNTING    │◄────│   PAYMENT &     │
│ (05-accounting) │     │    REFUND       │
│   9 tables      │     │ (06-payment)    │
└─────────────────┘     │   11 tables     │
                        └────────┬────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   COMPLIANCE    │     │  WITHHOLDING    │     │    TAX TYPE     │
│    CONTROL      │     │      TAX        │     │   EXTENSIONS    │
│(07-compliance)  │     │ (10-withholding)│     │ (11-vat,12-cit) │
│   10 tables     │     │    tables       │     │    tables       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

---

## 3. Domain Catalog

### 3.1 Summary Table

| # | Domain | Schema | Tables | Key Entities | Purpose |
|---|--------|--------|--------|--------------|---------|
| 1 | Party | party | 12 | party, individual, enterprise, party_identifier | Entity management |
| 2 | Tax Framework | tax_framework | 12 | tax_type, tax_period, tax_form, tax_rate | Tax structure |
| 3 | Registration | registration | 9 | tax_account, vat_registration, license | Tax registration |
| 4 | Filing & Assessment | filing_assessment | 6 | tax_return, assessment, penalty | Returns & assessments |
| 5 | Accounting | accounting | 9 | transaction, account, balance | Financial tracking |
| 6 | Payment & Refund | payment_refund | 11 | payment, refund, allocation | Money movement |
| 7 | Compliance & Control | compliance_control | 10 | audit_case, risk_profile, collection | Compliance management |
| 8 | Document Management | document_management | 13 | document, correspondence, workflow | Document tracking |
| 9 | Reference Data | reference | 58+ | ref_* tables | Lookup & configuration |
| 10 | Withholding Tax | withholding_tax | ~8 | employer_declaration, paye | WHT/PAYE |
| 11 | VAT | vat | ~4 | vat_return, vat_transaction | VAT extension |
| 12 | Income Tax | income_tax | ~4 | corporate_tax_return, tax_loss | CIT/PIT extension |

### 3.2 Domain Descriptions

---

#### Domain 1: Party Management (party)

**Purpose:** Manages all entities interacting with tax administration, supporting both individuals and enterprises with flexible identification.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `party` | Supertype for all parties (discriminator: party_type_code) |
| `individual` | Natural persons with personal attributes |
| `enterprise` | Legal entities (companies, partnerships, trusts) |
| `party_identifier` | **NEW** - Multi-identifier storage (TIN, VAT, EORI, etc.) |
| `party_relationship` | Relationships between parties (director, shareholder, etc.) |
| `address` | Physical and postal addresses with temporal validity |
| `contact` | Contact information (email, phone, etc.) |

**Key Patterns:**
- Party Supertype with subtype tables (individual, enterprise)
- Multi-Identifier Pattern via party_identifier junction table
- Temporal validity on all master data

**New in L2 Generalization:**
- `party_identifier` table replacing direct TIN columns
- Support for multiple identifier types per party
- Verification tracking for identifiers

---

#### Domain 2: Tax Framework (tax_framework)

**Purpose:** Defines tax structure, periods, forms, rates, and calculation rules applicable across all tax types.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `tax_type` | Tax type hierarchy (VAT, CIT, PIT, WHT, Excise, etc.) |
| `tax_period` | Tax periods (monthly, quarterly, annual) with due dates |
| `tax_period_template` | Period generation rules per tax type |
| `tax_form` | Form definitions with versioning |
| `form_field`, `form_section` | Form structure metadata |
| `tax_rate`, `paye_tax_scale` | Rate tables and progressive scales |
| `tax_calculation_rule` | Calculation logic and formulas |

**Key Patterns:**
- Form-as-metadata enabling dynamic form definitions without code changes
- Rate tables supporting flat, progressive, and differentiated rates
- Period templates for automated period generation

---

#### Domain 3: Registration (registration)

**Purpose:** Manages tax registration and obligations linking parties to tax types through the TTT principle.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `tax_account` | **Core TTT implementation** - Party + Tax Type linkage |
| `tax_account_period` | Period-level obligations and status |
| `vat_registration` | VAT-specific registration attributes |
| `vat_certificate` | VAT certificate management |
| `business_license` | Licenses and permits |
| `taxable_object` | Property, vehicles, other taxable objects |
| `taxpayer_segmentation` | Risk-based taxpayer segmentation |

**Key Pattern:** TTT (TIN-Tax Type-Tax Period) linking all tax operations

---

#### Domain 4: Filing & Assessment (filing_assessment)

**Purpose:** Handles tax returns and assessments, extending to tax-specific domains via 1:1 relationships.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `tax_return` | Return header with common attributes |
| `return_field_value` | Return line items (dynamic via form_field) |
| `assessment` | Assessment records (self, amended, official) |
| `penalty_assessment` | Penalty calculations |
| `interest_assessment` | Interest calculations |
| `tax_loss` | Loss carry-forward tracking |

**Key Pattern:** Core + Extension (e.g., tax_return → vat_return, tax_return → corporate_tax_return)

---

#### Domain 5: Accounting (accounting)

**Purpose:** Financial tracking and balance management using balance-forward accounting.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `account` | Chart of accounts structure |
| `transaction` | All financial movements |
| `transaction_line` | Transaction detail lines |
| `balance` | Period balances per tax account |
| `receivable` | Outstanding receivables |
| `journal_entry` | Manual adjustments |

**Key Pattern:** Balance-forward accounting with full audit trail

---

#### Domain 6: Payment & Refund (payment_refund)

**Purpose:** Manages payments received and refunds issued with flexible allocation rules.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `payment` | Payment records |
| `payment_allocation` | Allocation to liabilities (FIFO, specific, proportional) |
| `refund` | Refund claims and approvals |
| `refund_allocation` | Refund source allocation |
| `installment_plan` | Payment arrangement plans |
| `bank_reconciliation` | Bank statement matching |

**Key Pattern:** Allocation rules engine supporting multiple allocation methods

---

#### Domain 7: Compliance & Control (compliance_control)

**Purpose:** Audit, risk assessment, and compliance management.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `audit_case` | Audit records and findings |
| `audit_finding` | Individual audit findings |
| `collection_case` | Debt collection cases |
| `objection`, `appeal` | Dispute resolution |
| `risk_profile` | Risk factors and indicators |
| `risk_assessment_result` | Calculated risk scores |

**Key Pattern:** Risk-based compliance with monetary tax gap measurement

---

#### Domain 8: Document Management (document_management)

**Purpose:** Document and communication tracking with workflow support.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `document` | Document metadata |
| `document_version` | Version control |
| `correspondence` | Letters, notices, communications |
| `notification` | System notifications |
| `workflow`, `workflow_step` | Process automation |
| `task`, `task_assignment` | Work management |

**Key Pattern:** Document lifecycle with versioning and retention

---

#### Domain 9: Reference Data (reference)

**Purpose:** All lookup tables, configuration, and system parameters.

**Key Tables (58+ total):**

| Category | Tables | Description |
|----------|--------|-------------|
| **Identifier Types** | `ref_identifier_type` | **NEW** - TIN, VAT, EORI formats per country |
| **Tax Schemes** | `ref_tax_scheme` | **NEW** - Registration schemes with thresholds |
| **Geographic** | `ref_country`, `ref_currency` | ISO standard codes |
| **Tax Codes** | `ref_tax_category`, `ref_assessment_type` | Tax classification |
| **P&I** | `ref_penalty_type`, `ref_interest_type` | Penalty and interest codes |
| **Status Codes** | `ref_filing_status`, `ref_compliance_status` | Workflow status |
| **Payment** | `ref_payment_method` | Payment channels |
| **Document** | `ref_document_type` | Document classification |

**Key Pattern:** Country-scoped reference data with universal defaults

**New in L2 Generalization:**
- `ref_identifier_type` - Identifier format validation per country
- `ref_tax_scheme` - Tax scheme definitions with thresholds
- 8 existing tables enhanced with `country_code` column

---

#### Domain 10: Withholding Tax (withholding_tax)

**Purpose:** PAYE and third-party withholding management.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `employer_declaration` | Employer monthly/annual returns |
| `employee_withholding` | Individual employee records |
| `withholding_certificate` | WHT certificates issued |
| `payer_return` | Third-party payer returns |

**Key Pattern:** Employer-Employee-Tax Authority triangle

---

#### Domain 11: VAT (vat)

**Purpose:** VAT-specific extensions to core filing domain.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `vat_return` | Extends tax_return with VAT fields |
| `vat_transaction` | Input/Output VAT detail |
| `cross_border_supply` | Intra-community and imports |

**Key Pattern:** 1:1 extension of core filing tables

---

#### Domain 12: Income Tax (income_tax)

**Purpose:** CIT/PIT-specific extensions to core filing domain.

**Key Tables:**
| Table | Description |
|-------|-------------|
| `corporate_tax_return` | Extends tax_return with CIT fields |
| `financial_statement` | Balance sheet, P&L integration |
| `tax_loss` | Loss carry-forward tracking |
| `tax_loss_utilization` | Loss application records |

**Key Pattern:** 1:1 extension of core filing tables

---

## 4. Key Patterns

### 4.1 TTT Principle (TIN-Tax Type-Tax Period)

The foundational structure linking all tax operations:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│      PARTY      │     │    TAX TYPE     │     │   TAX PERIOD    │
│      (TIN)      │     │     (Type)      │     │    (Period)     │
│                 │     │                 │     │                 │
│ party_id        │     │ tax_type_code   │     │ tax_period_id   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      TAX_ACCOUNT        │
                    │      (TTT Link)         │
                    │                         │
                    │ party_id      ────────► │
                    │ tax_type_code ────────► │
                    │ tax_account_id          │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   TAX_ACCOUNT_PERIOD    │
                    │                         │
                    │ tax_account_id          │
                    │ tax_period_id ────────► │
                    └────────────┬────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│  TAX_RETURN   │       │   PAYMENT     │       │  ASSESSMENT   │
│               │       │               │       │               │
│ tax_account_id│       │ tax_account_id│       │ tax_account_id│
│ tax_period_id │       │ tax_period_id │       │ tax_period_id │
└───────────────┘       └───────────────┘       └───────────────┘
```

**Implementation:** Every transactional record (return, payment, assessment) links to a tax_account which establishes the Party-Tax Type relationship, and specifies the tax_period for temporal context.

---

### 4.2 Multi-Identifier Pattern

**Problem:** Different countries use different identifier types (TIN, VAT number, EORI, National ID, Social Security Number). Hardcoding these as columns creates schema proliferation.

**Solution:** Junction table with identifier type reference:

```
┌─────────────────────┐         ┌─────────────────────────┐
│        PARTY        │         │   REF_IDENTIFIER_TYPE   │
│    (party_id)       │         │                         │
│                     │         │ identifier_type_code    │
│                     │         │ country_code (nullable) │
│                     │         │ format_regex            │
│                     │         │ checksum_algorithm      │
└──────────┬──────────┘         └────────────┬────────────┘
           │                                  │
           │         ┌────────────────────────┘
           │         │
           ▼         ▼
┌─────────────────────────────────────────────────┐
│              PARTY_IDENTIFIER                   │
│                                                 │
│  party_identifier_id (PK)                       │
│  party_id (FK) ─────────────────────────────►   │
│  identifier_type_code ──────────────────────►   │
│  country_code (issuing country)                 │
│  identifier_value                               │
│  identifier_value_normalized                    │
│  is_primary (one primary per type/country)      │
│  is_verified                                    │
│  verification_status                            │
│  valid_from, valid_to (temporal)                │
└─────────────────────────────────────────────────┘
```

**Benefits:**
- Add new identifier types via reference data, not schema changes
- Runtime format validation using regex from ref_identifier_type
- Track identifier verification status
- Support multiple identifiers per party
- Maintain history of identifier changes

**Example Data:**
```sql
-- Malta enterprise with TIN, VAT, EORI
INSERT INTO party_identifier VALUES 
  (1, 2001, 'TIN', 'MLT', '987654321', TRUE, TRUE, 'VERIFIED'),
  (2, 2001, 'VAT', 'MLT', 'MT98765432', TRUE, TRUE, 'VERIFIED'),
  (3, 2001, 'EORI', 'MLT', 'MT987654321000', TRUE, TRUE, 'VERIFIED');
```

---

### 4.3 Tax-Type Extension Pattern

Core tables extended by tax-specific domains without duplication:

```
┌─────────────────────────────────────────────────────┐
│             FILING_ASSESSMENT SCHEMA                │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │            TAX_RETURN (Core)                │   │
│  │                                             │   │
│  │  tax_return_id (PK)                         │   │
│  │  tax_account_id (FK)                        │   │
│  │  tax_period_id (FK)                         │   │
│  │  filing_date                                │   │
│  │  filing_status_code                         │   │
│  │  total_tax_due                              │   │
│  │  ... common return attributes               │   │
│  └──────────────────────┬──────────────────────┘   │
└─────────────────────────┼───────────────────────────┘
                          │
                          │ 1:1 FK (tax_return_id)
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   VAT_RETURN    │ │ CORPORATE_TAX   │ │   (Future)      │
│   (vat schema)  │ │    RETURN       │ │                 │
│                 │ │ (income_tax)    │ │                 │
│ tax_return_id   │ │                 │ │                 │
│ input_vat       │ │ tax_return_id   │ │                 │
│ output_vat      │ │ taxable_income  │ │                 │
│ net_vat_due     │ │ tax_credits     │ │                 │
│ ec_sales_total  │ │ loss_utilized   │ │                 │
│ oss_indicator   │ │ account_alloc   │ │                 │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

**Benefits:**
- Common logic operates on core tables
- Tax-specific logic uses extension tables
- No code duplication for shared attributes
- Easy to add new tax types

---

### 4.4 Country-Scoped Reference Data Pattern

Reference tables with optional country_code supporting both universal and country-specific values:

```sql
-- Universal codes (country_code = NULL) - apply everywhere
INSERT INTO ref_assessment_type (code, country_code, name) VALUES
  ('SELF', NULL, 'Self-Assessment'),
  ('AUDIT', NULL, 'Audit Assessment'),
  ('AMENDED', NULL, 'Amended Assessment');

-- Country-specific codes - override or supplement universal
INSERT INTO ref_assessment_type (code, country_code, name) VALUES
  ('CFC', 'MLT', 'Malta CFC Assessment'),
  ('IMPUTATION', 'MLT', 'Imputation Credit Assessment'),
  ('SVAT_CONV', 'LKA', 'Sri Lanka SVAT Conversion');
```

**Standard Query Pattern:**
```sql
-- Get all assessment types for Malta (universal + Malta-specific)
SELECT code, COALESCE(name_local, name) as display_name
FROM ref_assessment_type 
WHERE country_code = 'MLT' OR country_code IS NULL
ORDER BY country_code NULLS LAST, display_order;
```

**Tables with Country-Scope Support:**
- ref_tax_category
- ref_assessment_type  
- ref_penalty_type
- ref_interest_type
- ref_payment_method
- ref_document_type
- ref_filing_status
- ref_compliance_status
- ref_identifier_type (NEW)
- ref_tax_scheme (NEW)

---

### 4.5 Tax Scheme Pattern

Flexible scheme management replacing hardcoded registration rules:

```
┌───────────────────────────────────────────────────────┐
│                   REF_TAX_SCHEME                      │
│                                                       │
│  scheme_code: "MLT_VAT_ART10"                        │
│  country_code: "MLT"                                  │
│  tax_type_code: "VAT"                                │
│  scheme_name: "VAT Article 10 - Standard"            │
│  registration_threshold: 35000.00                    │
│  registration_threshold_currency: "EUR"              │
│  registration_threshold_period: "12M"                │
│  exit_threshold: 28000.00                            │
│  filing_frequency_code: "QUARTERLY"                  │
│  allows_input_recovery: TRUE                         │
│  input_recovery_percentage: 100.00                   │
│  requires_tax_invoice: TRUE                          │
│  is_default: TRUE                                    │
│  is_voluntary: TRUE                                  │
│  is_mandatory: TRUE (when above threshold)           │
│  effective_from: "2004-05-01"                        │
└───────────────────────────────────────────────────────┘
```

**Malta VAT Schemes Example:**
| Scheme | Threshold | Input Recovery | Filing |
|--------|-----------|----------------|--------|
| MLT_VAT_ART10 | €35,000 | 100% | Quarterly |
| MLT_VAT_ART11 | €20,000 | 0% | Annual |
| MLT_VAT_ART12 | €10,000 (ICA) | 100% | Quarterly |
| MLT_VAT_EXEMPT | N/A | 0% | None |

---

## 5. Schema Statistics

### 5.1 Overall Statistics

| Metric | Count | Notes |
|--------|-------|-------|
| **Total Schemas** | 12 | One per business domain |
| **Total Tables** | ~112 | Including new generalization tables |
| **Reference Tables** | 58+ | Includes new ref_identifier_type, ref_tax_scheme |
| **Transactional Tables** | ~54 | Operational tables |
| **Foreign Key Relationships** | ~75 | Cross-table relationships |
| **Indexes** | ~220 | Performance optimization |

### 5.2 Tables by Domain

| Domain | Schema | Table Count | Type |
|--------|--------|-------------|------|
| Party | party | 12 | Master |
| Tax Framework | tax_framework | 12 | Configuration |
| Registration | registration | 9 | Transactional |
| Filing & Assessment | filing_assessment | 6 | Transactional |
| Accounting | accounting | 9 | Transactional |
| Payment & Refund | payment_refund | 11 | Transactional |
| Compliance & Control | compliance_control | 10 | Transactional |
| Document Management | document_management | 13 | Transactional |
| Reference Data | reference | 58+ | Reference |
| Withholding Tax | withholding_tax | ~8 | Extension |
| VAT | vat | ~4 | Extension |
| Income Tax | income_tax | ~4 | Extension |

### 5.3 New Tables (L2 Generalization)

| Table | Schema | Purpose |
|-------|--------|---------|
| party_identifier | party | Multi-identifier support |
| ref_identifier_type | reference | Identifier format definitions |
| ref_tax_scheme | reference | Tax scheme configurations |

### 5.4 Modified Tables (L2 Generalization)

8 reference tables enhanced with `country_code` column:
- ref_tax_category
- ref_assessment_type
- ref_penalty_type
- ref_interest_type
- ref_payment_method
- ref_document_type
- ref_filing_status
- ref_compliance_status

---

## 6. Implementation Guide

### 6.1 Prerequisites

| Requirement | Specification |
|-------------|---------------|
| Database | MySQL 8.0+ or PostgreSQL 14+ |
| Character Set | UTF-8 (utf8mb4 for MySQL) |
| Collation | utf8mb4_unicode_ci (MySQL) |
| Storage | Minimum 100GB for medium deployment |
| Memory | 8GB+ recommended for development |

### 6.2 Deployment Sequence

Deploy schemas and tables in dependency order:

```
Phase 1: Foundation
─────────────────────────────────────────────────────
  1.1  Create database and schemas
  1.2  Deploy reference tables (09-reference-data)
       - Load universal reference data first
       - Then country-specific reference data
  1.3  Deploy party tables (01-party)
       - party, individual, enterprise
       - party_identifier (NEW)

Phase 2: Framework
─────────────────────────────────────────────────────
  2.1  Deploy tax framework (02-tax-framework)
       - tax_type, tax_period, tax_form
       - tax_rate, calculation rules
  2.2  Deploy registration (03-registration)
       - tax_account (TTT core)
       - vat_registration, licenses

Phase 3: Operations
─────────────────────────────────────────────────────
  3.1  Deploy filing_assessment (04-filing)
  3.2  Deploy accounting (05-accounting)
  3.3  Deploy payment_refund (06-payment)
  3.4  Deploy compliance_control (07-compliance)
  3.5  Deploy document_management (08-document)

Phase 4: Extensions
─────────────────────────────────────────────────────
  4.1  Deploy withholding_tax (10-withholding)
  4.2  Deploy vat (11-vat)
  4.3  Deploy income_tax (12-income-tax)

Phase 5: Configuration
─────────────────────────────────────────────────────
  5.1  Load country-specific reference data
  5.2  Configure identifier types
  5.3  Set up tax schemes
  5.4  Configure system parameters
```

### 6.3 Country Implementation Checklist

When implementing for a new country:

- [ ] Add country to `ref_country` (if not present)
- [ ] Add identifier types to `ref_identifier_type`
- [ ] Add tax schemes to `ref_tax_scheme`
- [ ] Add country-specific codes to reference tables:
  - [ ] ref_tax_category
  - [ ] ref_assessment_type
  - [ ] ref_penalty_type
  - [ ] ref_interest_type
  - [ ] ref_payment_method (optional)
  - [ ] ref_document_type (optional)
- [ ] Configure tax types in `tax_type`
- [ ] Set up tax periods in `tax_period_template`
- [ ] Configure tax rates in `tax_rate` / `paye_tax_scale`
- [ ] Define forms in `tax_form` / `form_field`

### 6.4 Verification Queries

```sql
-- Check all schemas were created
SELECT SCHEMA_NAME 
FROM information_schema.SCHEMATA 
WHERE SCHEMA_NAME IN (
  'party', 'tax_framework', 'registration', 
  'filing_assessment', 'accounting', 'payment_refund', 
  'compliance_control', 'document_management', 'reference',
  'withholding_tax', 'vat', 'income_tax'
);

-- Count tables per schema
SELECT TABLE_SCHEMA, COUNT(*) as table_count
FROM information_schema.TABLES
WHERE TABLE_SCHEMA IN (
  'party', 'tax_framework', 'registration', 
  'filing_assessment', 'accounting', 'payment_refund', 
  'compliance_control', 'document_management', 'reference',
  'withholding_tax', 'vat', 'income_tax'
)
GROUP BY TABLE_SCHEMA
ORDER BY TABLE_SCHEMA;

-- Verify new generalization tables exist
SELECT TABLE_SCHEMA, TABLE_NAME
FROM information_schema.TABLES
WHERE (TABLE_SCHEMA = 'party' AND TABLE_NAME = 'party_identifier')
   OR (TABLE_SCHEMA = 'reference' AND TABLE_NAME IN ('ref_identifier_type', 'ref_tax_scheme'));

-- Verify country_code column added to reference tables
SELECT TABLE_NAME, COLUMN_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'reference'
  AND COLUMN_NAME = 'country_code'
ORDER BY TABLE_NAME;
```

---

## 7. Related Documents

### 7.1 Specification Documents

| Document | Purpose | Location |
|----------|---------|----------|
| ta-rdm-schema.yaml | YAML meta-schema | Project Knowledge |
| 01-tax-party.yaml | Party domain specification | Project Knowledge |
| 02-tax-framework.yaml | Tax framework specification | Project Knowledge |
| 03-registration.yaml | Registration specification | Project Knowledge |
| 04-filing-assessment.yaml | Filing specification | Project Knowledge |
| 05-accounting.yaml | Accounting specification | Project Knowledge |
| 06-payment-refund.yaml | Payment specification | Project Knowledge |
| 07-compliance-control.yaml | Compliance specification | Project Knowledge |
| 08-document-management.yaml | Document specification | Project Knowledge |
| 09-reference-data.yaml | Reference data specification | Project Knowledge |
| 10-withholding-tax.yaml | Withholding specification | Project Knowledge |
| 11-vat-tax.yaml | VAT specification | Project Knowledge |
| 12-income-tax.yaml | Income tax specification | Project Knowledge |

### 7.2 Generalization Documents (Step 2)

| Document | Purpose |
|----------|---------|
| ref_identifier_type.yaml | Identifier type definitions |
| ref_tax_scheme.yaml | Tax scheme definitions |
| party_identifier.yaml | Multi-identifier table spec |
| L2-reference-tables-country-code-additions.yaml | Reference table modifications |
| **L2-00-model-overview.md** | This document |

### 7.3 Implementation Documents

| Document | Purpose |
|----------|---------|
| ta-rdm-mysql-ddl.sql | Complete MySQL DDL script |
| README-MySQL-DDL.md | DDL usage instructions |
| L2-extension-guide.md | Country implementation guide |

### 7.4 Warehouse Layer Documents

| Document | Purpose |
|----------|---------|
| L3-00-model-overview.md | L3 warehouse model overview |
| dim_*.yaml | Dimension specifications |
| fact_*.yaml | Fact table specifications |

---

## 8. Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0.0 | 2025-12-10 | **L2 Generalization Release** |
| | | - Added multi-identifier pattern (party_identifier) |
| | | - Added ref_identifier_type for format validation |
| | | - Added ref_tax_scheme for scheme configuration |
| | | - Enhanced 8 reference tables with country_code |
| | | - Complete country-agnostic architecture |
| | | - Comprehensive overview document |
| 1.0.0 | 2025-11-15 | Initial TA-RDM release |
| | | - 109 tables across 12 schemas |
| | | - Malta-centric implementation |
| | | - Base reference data |

---

## 9. Appendix: Quick Reference

### 9.1 Standard Column Patterns

**Audit Columns (all transactional tables):**
```yaml
- created_by: VARCHAR(100) NOT NULL
- created_date: TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
- modified_by: VARCHAR(100)
- modified_date: TIMESTAMP
```

**Temporal Columns (master data):**
```yaml
- valid_from: DATE NOT NULL
- valid_to: DATE  # NULL = current
- is_current: BOOLEAN  # Derived: valid_to IS NULL
```

**Soft Delete Pattern:**
```yaml
- is_active: BOOLEAN NOT NULL DEFAULT TRUE
- deactivation_date: TIMESTAMP
- deactivation_reason: VARCHAR(200)
```

### 9.2 Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Schema | snake_case | filing_assessment |
| Table | snake_case, singular | tax_return |
| Column | snake_case | tax_period_id |
| Primary Key | {table}_id | tax_return_id |
| Foreign Key | {referenced_table}_id | party_id |
| Reference Table | ref_{name} | ref_country |
| Index | idx_{table}_{columns} | idx_party_tin |
| Unique Constraint | uk_{table}_{columns} | uk_party_identifier |
| Check Constraint | chk_{table}_{rule} | chk_valid_dates |

### 9.3 Data Types

| Purpose | MySQL | PostgreSQL |
|---------|-------|------------|
| Primary Key | BIGINT AUTO_INCREMENT | BIGSERIAL |
| UUID | CHAR(36) | UUID |
| Money | DECIMAL(19,2) | NUMERIC(19,2) |
| Percentage | DECIMAL(5,2) | NUMERIC(5,2) |
| Code | VARCHAR(20) | VARCHAR(20) |
| Name | VARCHAR(200) | VARCHAR(200) |
| Description | TEXT | TEXT |
| Date | DATE | DATE |
| Timestamp | TIMESTAMP | TIMESTAMP WITH TIME ZONE |
| Boolean | BOOLEAN | BOOLEAN |
| Country Code | CHAR(3) | CHAR(3) |
| Currency Code | CHAR(3) | CHAR(3) |

---

*End of L2 Canonical Model Overview*

*Generated: 2025-12-10 | TA-RDM Version: 2.0.0*
