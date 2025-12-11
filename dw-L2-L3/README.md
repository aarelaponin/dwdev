# TA-RDM: Tax Administration Reference Data Model

## Overview

The Tax Administration Reference Data Model (TA-RDM) is a comprehensive, country-agnostic data architecture framework for tax administration systems. Originally developed for Malta MTCA and generalized for international deployment, it supports multiple jurisdictions including Malta, Sri Lanka, Moldova, Ukraine, and Lebanon.

**Version:** 2.0.0  
**Last Updated:** December 2024  
**Status:** Production Ready (L2 Complete, L3 Generalization Complete)

## Repository Structure

```
_DW-L2-L3/
├── README.md                           # This file
├── TA-RDM-Reference-Model-Catalog.md   # Complete model catalog and inventory
│
├── _docs/                              # Documentation (5 files)
│   ├── L2-00-model-overview.md         # L2 canonical model overview
│   ├── L2-extension-guide.md           # Guide for extending L2 model
│   ├── L3-00-model-overview.md         # L3 data warehouse overview
│   ├── L3-Generalization-Patterns-Catalog.md  # Generalization patterns P1-P7
│   └── TA-RDM-YAML-Guide.md            # YAML specification guide
│
├── _model-L2-core/                     # L2 Canonical Domain Schemas (13 files)
│   ├── 00-ta-rdm-schema.yaml           # Schema definition & conventions
│   ├── 01-tax-party.yaml               # Party management domain
│   ├── 02-tax-framework.yaml           # Tax framework & types
│   ├── 03-registration.yaml            # Registration domain
│   ├── 04-filing-assessment.yaml       # Filing & assessment domain
│   ├── 05-accounting.yaml              # Tax accounting domain
│   ├── 06-payment-refund.yaml          # Payment & refund domain
│   ├── 07-compliance-control.yaml      # Compliance & control domain
│   ├── 08-document-management.yaml     # Document management domain
│   ├── 09-reference-data.yaml          # Reference data domain
│   ├── 10-withholding-tax.yaml         # Withholding tax extension
│   ├── 11-vat-tax.yaml                 # VAT tax extension
│   └── 12-income-tax.yaml              # Income tax extension
│
└── _model-L2-L3/                       # L2-L3 Generalization & DW (31 files)
    │
    │── # L2 Generalization Additions (4 files)
    ├── party_identifier.yaml
    ├── ref_identifier_type.yaml
    ├── ref_tax_scheme.yaml
    ├── L2-reference-tables-country-code-additions.yaml
    │
    │── # L3 Core Dimensions (12 files)
    ├── dim_country.yaml                # NEW: Central country configuration
    ├── dim_tax_scheme.yaml             # NEW: Tax registration schemes
    ├── dim_account_subtype.yaml        # NEW: Malta imputation accounts
    ├── dim_date.yaml                   # GENERALIZED: Multi-fiscal-year support
    ├── dim_party.yaml                  # GENERALIZED: Country-aware
    ├── dim_geography.yaml              # GENERALIZED: 4-level hierarchy
    ├── dim_tax_period.yaml             # GENERALIZED: Country-aware
    ├── dim_registration.yaml
    ├── dim_tax_type.yaml
    │
    │── # L3 Reference Dimensions (6 files)
    ├── dim_assessment_type.yaml
    ├── dim_compliance_status.yaml
    ├── dim_customs_procedure.yaml
    ├── dim_international_country.yaml
    ├── dim_payment_method.yaml
    ├── dim_risk_category.yaml
    │
    │── # L3 Bridge Tables (2 files)
    ├── bridge_party_identifier.yaml    # NEW: Multi-identifier support
    ├── bridge_refund_account.yaml      # NEW: Refund account breakdown
    │
    │── # L3 Reference Tables (1 file)
    ├── ref_country_holiday.yaml        # NEW: Externalized holidays
    │
    │── # L3 Fact Tables (8 files)
    ├── fact_filing.yaml
    ├── fact_assessment.yaml
    ├── fact_payment.yaml
    ├── fact_refund.yaml                # GENERALIZED: Uses bridge table
    ├── fact_account_balance.yaml
    ├── fact_customs_declaration.yaml
    ├── fact_compliance_event.yaml
    └── fact_risk_score.yaml
```

## Model Layers

### L2 - Canonical Data Model
The L2 layer defines the logical/canonical data model covering all tax administration domains:
- **Party Management** - Taxpayers, representatives, relationships
- **Registration** - Tax type registrations, obligations
- **Filing & Assessment** - Returns, declarations, assessments
- **Accounting** - Tax accounts, balance forward methodology
- **Payment & Refund** - Payments, refunds, allocations
- **Compliance & Control** - Audits, risk management, cases
- **Document Management** - Forms, templates, correspondence

### L3 - Data Warehouse (Kimball Dimensional Model)
The L3 layer implements a Kimball-style dimensional model optimized for analytics:
- **14 Dimensions** - Conformed dimensions for consistent analysis
- **8 Fact Tables** - Transaction and periodic snapshot facts
- **2 Bridge Tables** - Many-to-many relationship resolution
- **Enterprise Bus Matrix** - Cross-functional analysis capability

## Generalization Patterns

The model implements 7 generalization patterns for multi-country deployment:

| Pattern | Name | Purpose |
|---------|------|---------|
| P1 | Country Dimension Integration | Central country configuration with fiscal year settings |
| P2 | Multi-Identifier Party | Flexible party identification across jurisdictions |
| P3 | Tax Scheme Dimension | Configurable tax registration schemes |
| P4 | Generic Geography Hierarchy | 4-level location hierarchy adaptable to any country |
| P5 | Account Subtype Dimension | Externalized Malta imputation account handling |
| P6 | Configurable Fiscal Year | Pre-calculated fiscal years for different start months |
| P7 | Externalized Holidays | Country-specific holiday management |

## Supported Countries

| Country | Code | Status | Fiscal Year |
|---------|------|--------|-------------|
| Malta | MLT | Production | January (calendar) |
| Sri Lanka | LKA | In Progress | April |
| Moldova | MDA | Planned | January |
| Ukraine | UKR | Planned | January |
| Lebanon | LBN | Planned | January |

## Key Concepts

### TTT Principle
The foundational architecture pattern: **TIN-Tax Type-Tax Period**
- Every tax obligation is identified by taxpayer + tax type + period
- Enables consistent data organization across all operations
- Supports both operational and analytical workloads

### Balance Forward Accounting
Robust financial tracking methodology:
- Running balances maintained per tax account
- Supports simple and complex tax structures
- Enables point-in-time balance queries

### Compliance Risk Management
Focus on monetary tax gap measurement:
- Risk = probability × potential tax gap
- Voluntary compliance improvement as primary objective
- Sustainable behavioral change over enforcement

## Technical Implementation

### Target Platforms
- **PostgreSQL 14+** - Primary OLTP platform
- **ClickHouse** - Analytical data warehouse (L3)
- **MySQL 8+** - Alternative OLTP platform

### File Format
All model definitions use YAML with standardized structure:
- Metadata section with versioning
- Column definitions with types, constraints, descriptions
- Business rules and validation queries
- Sample data for reference

## Quick Start

1. **Review the Catalog**: Start with `TA-RDM-Reference-Model-Catalog.md` for complete inventory
2. **Understand L2**: Read `_docs/L2-00-model-overview.md` for canonical model
3. **Explore L3**: Review `_docs/L3-00-model-overview.md` for data warehouse design
4. **Check Patterns**: See `_docs/L3-Generalization-Patterns-Catalog.md` for multi-country patterns
5. **YAML Guide**: Reference `_docs/TA-RDM-YAML-Guide.md` for specification details

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0.0 | Dec 2024 | L3 generalization complete, multi-country support |
| 1.5.0 | Nov 2024 | L3 dimensional model, Malta implementation |
| 1.0.0 | Oct 2024 | Initial L2 canonical model |

## Related Resources

- **GovStack Initiative** - Government building blocks alignment
- **OECD CRM Guidelines** - Compliance risk management best practices
- **Kimball Group** - Dimensional modeling methodology

## License

CC-BY-4.0

## Authors

- Aare Laponin - Senior Tax Administration Data Architect, IMF Short-Term Expert

---

*This model is part of the Tax Administration Modernization initiative supporting developing countries in building robust, scalable tax information systems.*
