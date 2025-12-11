# TA-RDM Reference Model Catalog

## Document Control

| Attribute | Value |
|-----------|-------|
| **Version** | 2.0.0 |
| **Date** | 2025-12-11 |
| **Status** | Production Ready |
| **Model** | Tax Administration Reference Data Model |
| **Scope** | L2 Canonical + L3 Warehouse (Generalized) |

---

## 1. Executive Summary

This catalog provides a complete inventory of all objects and documents comprising the Tax Administration Reference Data Model (TA-RDM) version 2.0.0. The model supports multi-country tax administration deployments with country-agnostic core patterns.

### Model Statistics

| Layer | Category | Count |
|-------|----------|------:|
| **L2 Canonical** | Schemas | 12 |
| | Tables (estimated) | ~112 |
| | Specification Files | 13 |
| | Generalization Tables | 4 |
| **L3 Warehouse** | Dimensions | 16 |
| | Fact Tables | 8 |
| | Bridge Tables | 2 |
| | Reference Tables | 1 |
| **Documentation** | Overview Documents | 4 |
| | Pattern Catalogs | 1 |
| | Guides | 2 |
| **Total Files** | | **~50** |

### Supported Countries

| Country | ISO Code | Status |
|---------|----------|--------|
| Malta | MLT | Production |
| Sri Lanka | LKA | Configured |
| Moldova | MDA | Configured |
| Ukraine | UKR | Configured |
| Lebanon | LBN | Configured |

---

## 2. L2 Canonical Model

### 2.1 Core Domain Specifications

| # | File | Schema | Tables | Description |
|---|------|--------|-------:|-------------|
| 1 | `00-ta-rdm-schema.yaml` | (meta) | — | YAML meta-schema definition |
| 2 | `01-tax-party.yaml` | party | ~12 | Party management (individuals, enterprises, addresses, relationships) |
| 3 | `02-tax-framework.yaml` | tax_framework | ~12 | Tax types, periods, forms, rates, calculation rules |
| 4 | `03-registration.yaml` | registration | ~9 | Tax accounts, VAT registration, licenses |
| 5 | `04-filing-assessment.yaml` | filing_assessment | ~6 | Tax returns, assessments, penalties |
| 6 | `05-accounting.yaml` | accounting | ~9 | Transactions, accounts, balances |
| 7 | `06-payment-refund.yaml` | payment_refund | ~11 | Payments, refunds, allocations |
| 8 | `07-compliance-control.yaml` | compliance_control | ~10 | Audit cases, risk profiles, collection |
| 9 | `08-document-management.yaml` | document_management | ~13 | Documents, correspondence, workflow |
| 10 | `09-reference-data.yaml` | reference | ~58 | All ref_* lookup tables |
| 11 | `10-withholding-tax.yaml` | withholding_tax | ~8 | PAYE, employer declarations, WHT |
| 12 | `11-vat-tax.yaml` | vat | ~4 | VAT returns, transactions, cross-border |
| 13 | `12-income-tax.yaml` | income_tax | ~4 | CIT/PIT returns, financial statements, tax loss |

### 2.2 L2 Generalization Tables (v2.0.0 Additions)

| # | File | Schema.Table | Purpose | Pattern |
|---|------|--------------|---------|---------|
| 1 | `party_identifier.yaml` | party.party_identifier | Multi-identifier storage per party | P2 |
| 2 | `ref_identifier_type.yaml` | reference.ref_identifier_type | Identifier format definitions by country | P2 |
| 3 | `ref_tax_scheme.yaml` | reference.ref_tax_scheme | Tax scheme configurations (VAT Articles, etc.) | P3 |
| 4 | `L2-reference-tables-country-code-additions.yaml` | reference.* | country_code column additions to 8 tables | P1 |

### 2.3 Reference Tables Enhanced with country_code

| # | Table | Description |
|---|-------|-------------|
| 1 | ref_tax_category | Tax classification codes |
| 2 | ref_assessment_type | Assessment type codes |
| 3 | ref_penalty_type | Penalty type definitions |
| 4 | ref_interest_type | Interest calculation rules |
| 5 | ref_payment_method | Payment channel codes |
| 6 | ref_document_type | Document classification |
| 7 | ref_filing_status | Filing workflow states |
| 8 | ref_compliance_status | Compliance state codes |

---

## 3. L3 Warehouse Model

### 3.1 Core Dimensions (Country-Aware)

| # | File | Dimension | Grain | SCD | Pattern | Status |
|---|------|-----------|-------|-----|---------|--------|
| 1 | `dim_country.yaml` | dim_country | One per country | Type 1 | P1 | **NEW v2.0** |
| 2 | `dim_date_generalized.yaml` | dim_date | One per calendar day | Type 0 | P6, P7 | **Generalized** |
| 3 | `dim_party.yaml` | dim_party | One per party/version | Type 2 | P2 | **Generalized** |
| 4 | `dim_tax_type.yaml` | dim_tax_type | One per tax type | Type 1 | — | Existing |
| 5 | `dim_tax_period_generalized.yaml` | dim_tax_period | One per period | Type 1 | P6 | **Generalized** |
| 6 | `dim_registration.yaml` | dim_registration | One per registration/version | Type 2 | P3 | Existing |
| 7 | `dim_geography.yaml` | dim_geography | One per admin division | Type 1 | P4 | **Generalized** |
| 8 | `dim_tax_scheme.yaml` | dim_tax_scheme | One per scheme/version | Type 2 | P3 | **NEW v2.0** |
| 9 | `dim_account_subtype.yaml` | dim_account_subtype | One per account type | Type 1 | P5 | **NEW v2.0** |

### 3.2 Reference Dimensions (Standalone)

| # | File | Dimension | Grain | SCD | Description |
|---|------|-----------|-------|-----|-------------|
| 10 | `dim_assessment_type.yaml` | dim_assessment_type | One per type | Type 1 | Assessment categories |
| 11 | `dim_compliance_status.yaml` | dim_compliance_status | One per status | Type 1 | Compliance states |
| 12 | `dim_customs_procedure.yaml` | dim_customs_procedure | One per procedure | Type 1 | Customs codes |
| 13 | `dim_payment_method.yaml` | dim_payment_method | One per method | Type 1 | Payment channels |
| 14 | `dim_risk_category.yaml` | dim_risk_category | One per category | Type 1 | Risk classification |
| 15 | `dim_international_country.yaml` | dim_international_country | One per ISO country | Type 1 | All ISO countries |

### 3.3 Bridge Tables

| # | File | Bridge | Grain | Purpose | Pattern | Status |
|---|------|--------|-------|---------|---------|--------|
| 1 | `bridge_party_identifier.yaml` | bridge_party_identifier | One per identifier | Party → multiple identifiers | P2 | **NEW v2.0** |
| 2 | `bridge_refund_account.yaml` | bridge_refund_account | One per account allocation | Refund → account subtypes | P5, P9 | **NEW v2.0** |

### 3.4 L3 Reference Tables

| # | File | Table | Purpose | Pattern | Status |
|---|------|-------|---------|---------|--------|
| 1 | `ref_country_holiday.yaml` | ref_country_holiday | Country-specific holidays | P7 | **NEW v2.0** |

### 3.5 Fact Tables

| # | File | Fact | Type | Grain | Est. Rows | Status |
|---|------|------|------|-------|----------:|--------|
| 1 | `fact_filing.yaml` | fact_filing | Transactional | One per tax return | 5M | Existing |
| 2 | `fact_assessment.yaml` | fact_assessment | Transactional | One per assessment | 3M | Existing |
| 3 | `fact_payment.yaml` | fact_payment | Transactional | One per payment | 10M | Existing |
| 4 | `fact_refund_generalized.yaml` | fact_refund | Transactional | One per refund claim | 500K | **Generalized** |
| 5 | `fact_account_balance.yaml` | fact_account_balance | Periodic Snapshot | One per account/month | 50M | Existing |
| 6 | `fact_customs_declaration.yaml` | fact_customs_declaration | Transactional | One per declaration | 2M | Existing |
| 7 | `fact_compliance_event.yaml` | fact_compliance_event | Factless | One per event | 2M | Existing |
| 8 | `fact_risk_score.yaml` | fact_risk_score | Periodic Snapshot | One per party/quarter | 10M | Existing |

---

## 4. Documentation

### 4.1 Overview Documents

| # | File | Layer | Purpose |
|---|------|-------|---------|
| 1 | `L2-00-model-overview.md` | L2 | Comprehensive L2 canonical model overview |
| 2 | `L3-00-model-overview.md` | L3 | Comprehensive L3 warehouse model overview |

### 4.2 Implementation Guides

| # | File | Layer | Purpose |
|---|------|-------|---------|
| 1 | `L2-extension-guide.md` | L2 | Country implementation guide for L2 |
| 2 | `TA-RDM-YAML-Guide.md` | Both | YAML schema documentation guide |

### 4.3 Pattern Catalogs

| # | File | Layer | Purpose |
|---|------|-------|---------|
| 1 | `L3-Generalization-Patterns-Catalog.md` | L3 | Reusable patterns for multi-country support |

---

## 5. Generalization Patterns Reference

| ID | Pattern Name | Components | Purpose |
|----|--------------|------------|---------|
| P1 | Country Dimension | dim_country | Central country configuration |
| P2 | Multi-Identifier Party | dim_party + bridge_party_identifier | Flexible identifier management |
| P3 | Tax Scheme Dimension | dim_tax_scheme | Country-agnostic scheme rules |
| P4 | Generic Geography Hierarchy | dim_geography | 4-level country-neutral hierarchy |
| P5 | Account Subtype Dimension | dim_account_subtype + bridge_refund_account | Country-specific breakdown |
| P6 | Configurable Fiscal Year | dim_date + dim_tax_period + dim_country | Pre-calculated fiscal years |
| P7 | Externalized Holidays | ref_country_holiday | Query-time holiday lookup |
| P8 | Localized Text | ref_localized_text | Multi-language support |
| P9 | Country-Conditional Measures | Bridge tables | Country-specific fact measures |

---

## 6. File Inventory by Category

### 6.1 All YAML Specification Files

```
L2 CANONICAL MODEL (17 files)
├── Core Domain Schemas (13)
│   ├── 00-ta-rdm-schema.yaml
│   ├── 01-tax-party.yaml
│   ├── 02-tax-framework.yaml
│   ├── 03-registration.yaml
│   ├── 04-filing-assessment.yaml
│   ├── 05-accounting.yaml
│   ├── 06-payment-refund.yaml
│   ├── 07-compliance-control.yaml
│   ├── 08-document-management.yaml
│   ├── 09-reference-data.yaml
│   ├── 10-withholding-tax.yaml
│   ├── 11-vat-tax.yaml
│   └── 12-income-tax.yaml
│
└── Generalization Additions (4)
    ├── party_identifier.yaml
    ├── ref_identifier_type.yaml
    ├── ref_tax_scheme.yaml
    └── L2-reference-tables-country-code-additions.yaml

L3 WAREHOUSE MODEL (27 files)
├── Core Dimensions - Generalized (9)
│   ├── dim_country.yaml                    [NEW]
│   ├── dim_date_generalized.yaml           [GENERALIZED]
│   ├── dim_party.yaml                      [GENERALIZED]
│   ├── dim_tax_type.yaml
│   ├── dim_tax_period_generalized.yaml     [GENERALIZED]
│   ├── dim_registration.yaml
│   ├── dim_geography.yaml                  [GENERALIZED]
│   ├── dim_tax_scheme.yaml                 [NEW]
│   └── dim_account_subtype.yaml            [NEW]
│
├── Reference Dimensions (6)
│   ├── dim_assessment_type.yaml
│   ├── dim_compliance_status.yaml
│   ├── dim_customs_procedure.yaml
│   ├── dim_payment_method.yaml
│   ├── dim_risk_category.yaml
│   └── dim_international_country.yaml
│
├── Bridge Tables (2)
│   ├── bridge_party_identifier.yaml        [NEW]
│   └── bridge_refund_account.yaml          [NEW]
│
├── Reference Tables (1)
│   └── ref_country_holiday.yaml            [NEW]
│
└── Fact Tables (8)
    ├── fact_filing.yaml
    ├── fact_assessment.yaml
    ├── fact_payment.yaml
    ├── fact_refund_generalized.yaml        [GENERALIZED]
    ├── fact_account_balance.yaml
    ├── fact_customs_declaration.yaml
    ├── fact_compliance_event.yaml
    └── fact_risk_score.yaml

DOCUMENTATION (5 files)
├── L2-00-model-overview.md
├── L2-extension-guide.md
├── L3-00-model-overview.md
├── L3-Generalization-Patterns-Catalog.md
└── TA-RDM-YAML-Guide.md
```

### 6.2 New/Modified in v2.0.0 (Generalization)

| Category | File | Change Type |
|----------|------|-------------|
| **L2 New Tables** | | |
| | party_identifier.yaml | NEW |
| | ref_identifier_type.yaml | NEW |
| | ref_tax_scheme.yaml | NEW |
| | L2-reference-tables-country-code-additions.yaml | NEW |
| **L3 New Dimensions** | | |
| | dim_country.yaml | NEW |
| | dim_tax_scheme.yaml | NEW |
| | dim_account_subtype.yaml | NEW |
| **L3 Generalized Dimensions** | | |
| | dim_date_generalized.yaml | MODIFIED |
| | dim_party.yaml | MODIFIED |
| | dim_geography.yaml | MODIFIED |
| | dim_tax_period_generalized.yaml | MODIFIED |
| **L3 New Bridges** | | |
| | bridge_party_identifier.yaml | NEW |
| | bridge_refund_account.yaml | NEW |
| **L3 New Reference** | | |
| | ref_country_holiday.yaml | NEW |
| **L3 Generalized Facts** | | |
| | fact_refund_generalized.yaml | MODIFIED |
| **Documentation** | | |
| | L2-00-model-overview.md | UPDATED to v2.0 |
| | L3-00-model-overview.md | UPDATED to v2.0 |
| | L3-Generalization-Patterns-Catalog.md | NEW |

---

## 7. Enterprise Bus Matrix Summary

### Dimension-Fact Conformance

| Dimension | Filing | Assessment | Payment | Refund | Balance | Customs | Compliance | Risk |
|-----------|:------:|:----------:|:-------:|:------:|:-------:|:-------:|:----------:|:----:|
| dim_country | ● | ● | ● | ● | ● | ● | ● | ● |
| dim_date | ●4 | ●3 | ●4 | ●4 | ●2 | ●5 | ●3 | ●1 |
| dim_party | ● | ● | ● | ● | ● | ● | ● | ● |
| dim_tax_type | ● | ● | ● | ● | ● | ○ | ○ | ○ |
| dim_tax_period | ● | ● | ○ | ○ | ● | ○ | ○ | ○ |
| dim_registration | ● | ● | ● | ● | ● | ● | ● | ○ |
| dim_geography | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ |
| dim_tax_scheme | ● | ○ | ○ | ● | ○ | ○ | ○ | ○ |
| dim_account_subtype | ○ | ○ | ○ | ●* | ○ | ○ | ○ | ○ |
| dim_assessment_type | ○ | ● | ○ | ○ | ○ | ○ | ○ | ○ |
| dim_compliance_status | ● | ○ | ● | ● | ○ | ○ | ● | ○ |
| dim_customs_procedure | ○ | ○ | ○ | ○ | ○ | ● | ○ | ○ |
| dim_payment_method | ○ | ○ | ● | ● | ○ | ○ | ○ | ○ |
| dim_risk_category | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ● |
| dim_international_country | ○ | ○ | ○ | ○ | ○ | ● | ○ | ○ |

**Legend:** ● = Required | ○ = Optional/NA | N = Role-playing count | * = Via bridge table

---

## 8. Version History

| Version | Date | Description |
|---------|------|-------------|
| 2.0.0 | 2025-12-11 | Multi-country generalization complete |
| 1.0.0 | 2025-11-15 | Initial release (Malta-centric) |

### v2.0.0 Change Summary

- **11 new components** added for multi-country support
- **5 components generalized** to remove country-specific hardcoding
- **9 patterns** documented for reusable generalization
- **5 countries** configured (MLT, LKA, MDA, UKR, LBN)
- **Full backward compatibility** maintained via views

---

## 9. Quick Reference

### Key Design Principles

| Principle | Description |
|-----------|-------------|
| **TTT** | TIN-Tax Type-Tax Period as foundational structure |
| **Country-Agnostic** | All variations via reference data, not schema |
| **Kimball Methodology** | Star schema with conformed dimensions |
| **SCD Support** | Type 1 (reference) and Type 2 (master) |
| **Multi-Identifier** | Flexible identifier management per party |

### Physical Implementation

| Platform | Primary | Compatible |
|----------|---------|------------|
| **Database** | ClickHouse | PostgreSQL, MySQL |
| **Schema** | warehouse | — |
| **Engine** | ReplacingMergeTree (SCD2), MergeTree (facts) | — |

### ETL Load Order

```
Phase 1: Static Reference (dim_country first)
Phase 2: Master Dimensions (SCD2 processing)
Phase 3: Bridge Tables
Phase 4: Fact Tables (partitioned by month)
```

---

*End of TA-RDM Reference Model Catalog*

*Generated: 2025-12-11 | Model Version: 2.0.0*
