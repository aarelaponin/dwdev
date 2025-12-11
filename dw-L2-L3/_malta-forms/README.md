# Malta Tax Forms - Source Documentation for L2/L3 Data Warehouse Schema

This folder contains comprehensive YAML schema definitions for Malta Tax and Customs Administration (MTCA) tax forms, designed to support:
- ORS (Operational Reporting System) database schema design
- Legacy Informix data migration planning
- L2/L3 data warehouse dimensional modeling

## Contents

### Overview Document
| File | Description | Size |
|------|-------------|------|
| `mtca-forms.md` | Master catalogue of all Malta tax forms (2018-2025) | 32KB |

### Priority 1: Core Business Taxes (51 forms)
| File | Forms | Lines |
|------|-------|-------|
| `mtca_forms_priority1a_corporate_tax.yaml` | e-CO, TIFD, TRA 63, TRA 100/100A, TRA 107 (29 forms) | 3,579 |
| `mtca_forms_priority1b_vat_article11.yaml` | VAT Registration Form 2, VAT Return Art 11 (7 forms) | 1,330 |
| `mtca_forms_priority1c_tra_additional.yaml` | TRA 19, TRA 20, TRA 53, TRA 61 (15 forms) | 2,089 |

### Priority 2: Specialized Taxes (44 forms)
| File | Forms | Lines |
|------|-------|-------|
| `mtca_forms_priority2a_stamp_duty_property.yaml` | DDT1, Schedules B-F (18 forms) | 2,508 |
| `mtca_forms_priority2b_tax_credits_ra.yaml` | RA6-RA30 series (11 forms) | 1,925 |
| `mtca_forms_priority2c_environmental.yaml` | ECO Products/Accommodation, BCRS, VEH 007 (8 forms) | 2,572 |
| `mtca_forms_priority2d_gaming_tax.yaml` | Gaming Tax Returns (7 forms) | 1,748 |

### Priority 3: Individual/Employer (62 forms)
| File | Forms | Lines |
|------|-------|-------|
| `mtca_forms_priority3a_individual_tax.yaml` | e-Return, TA22-24, PT1, AF/AF1 (20 forms) | 3,807 |
| `mtca_forms_priority3b_employer_fss.yaml` | FS3-FS7, FB1 (16 forms) | 2,288 |
| `mtca_forms_priority3c_vat_registration.yaml` | VAT Form 1-4, Art 10 Return (12 forms) | 1,886 |
| `mtca_forms_priority3d_tax_credits_ra.yaml` | RA6, RA9, RA10, etc. (14 forms) | 2,616 |

### Priority 4: Customs (27 forms)
| File | Forms | Lines |
|------|-------|-------|
| `mtca_forms_priority4a_customs_declarations.yaml` | SAD (H1-H7), AIS, AES (10 forms) | 2,696 |
| `mtca_forms_priority4b_transit_security.yaml` | T1, T2, TIR, ENS/EXS (5 forms) | 2,037 |
| `mtca_forms_priority4c_excise_emcs.yaml` | e-AD, e-SAD, AWK applications (7 forms) | 1,839 |
| `mtca_forms_priority4d_trader_origin.yaml` | EORI, AEO, BTI, EUR.1 (5 forms) | 1,940 |

### Priority 5: International/Exchange of Information (51 forms)
| File | Forms | Lines |
|------|-------|-------|
| `mtca_forms_priority5a_crs_fatca.yaml` | CRS Report, FATCA Report (14 forms) | 3,054 |
| `mtca_forms_priority5b_cbcr.yaml` | CbCR Report, Master/Local File (14 forms) | 2,292 |
| `mtca_forms_priority5c_dac6_dac7.yaml` | DAC6/DAC7 Reporting (3 forms) | 1,456 |
| `mtca_forms_priority5d_oss_intrastat.yaml` | Union OSS, Intrastat (14 forms) | 2,496 |
| `mtca_forms_priority5e_cesop_refunds.yaml` | CESOP, VAT Refund Claims (6 forms) | 2,135 |

## Totals
- **235 form definitions**
- **46,393 lines of YAML**
- **~1.8 MB total**

## Schema Structure

Each YAML file follows a consistent structure:

```yaml
forms:
  - form:
      metadata:           # Form identification, legislative basis, filing info
      sections:           # Logical groupings of fields
        - section_id
          fields:
            - field_id
              data_type
              validation_rules
              legacy_mapping    # Informix source mapping
              ors_mapping       # ClickHouse target mapping
      attachments:        # Required/optional attachments
      related_forms:      # Cross-form relationships
      submission_workflow # Authentication, signature requirements

reference_tables:        # Code lists and lookups
migration_mapping:       # Legacy to ORS table mappings
form_relationships:      # Processing chains and dependencies
```

## Usage for L2/L3 Schema Design

### Mapping to Dimensional Model

| YAML Element | L2/L3 Model Component |
|--------------|----------------------|
| `form_code` | `dim_form.form_code` |
| `field_id` | Column definitions in fact/dim tables |
| `data_type` | ClickHouse column types |
| `legacy_mapping` | ETL source specifications |
| `ors_mapping` | Target table/column definitions |
| `validation_rules` | Data quality rules |
| `cross_form_reference` | Foreign key relationships |

### Key Reference Tables
- `dim_tax_type` → Links to `metadata.tax_type`
- `dim_party` → Links to TIN/VAT fields
- `dim_tax_period` → Links to `year_of_assessment`, `return_period`
- `fact_filing` → One row per form submission

## Data Sources

All form specifications derived from:
- CFR official forms: https://cfr.gov.mt
- Malta Customs: https://customs.gov.mt
- Malta Gaming Authority: https://mga.org.mt
- OECD XSD schemas for CRS/CbCR
- EU DAC6/DAC7 technical specifications

---
*Generated: December 2025*
*Version: 1.0*
*Contact: MTCA Digital Transformation Project*
