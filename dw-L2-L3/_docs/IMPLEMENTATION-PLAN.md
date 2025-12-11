# TA-RDM Framework Generalization - Master Implementation Plan

**Project**: Adapt ETL Framework for Generalized L2-L3 Schemas
**Version**: 1.0
**Created**: 2024-12-11
**Target**: Malta first, then Sri Lanka
**Approach**: dbt-based L2→L3 ETL (clean rebuild)
**Scope**: Full implementation of all 7 generalization patterns (P1-P7)

---

## Executive Summary

This master plan provides a comprehensive, iterative roadmap for adapting the existing ETL framework to support generalized L2-L3 schemas for multi-country deployment. The work is organized into **6 phases** with **35 small iterations**, each designed to be independently testable and completable in 1-2 focused sessions.

### Key Metrics
| Metric | Target |
|--------|--------|
| Total Iterations | 35 |
| L2 Tables to Generate | 112 (13 YAML files) |
| L3 Dimensions | 16 |
| L3 Facts | 8 |
| L3 Bridges | 2 |
| Patterns to Implement | 7 (P1-P7) |

---

## Phase Overview

| Phase | Name | Iterations | Focus |
|-------|------|------------|-------|
| 0 | Project Infrastructure | 5 | dbt setup, macros, seeds, test harness |
| 1 | Generator - Parser | 6 | YAML parsing for L2-L3 schemas |
| 2 | Generator - Model Generation | 8 | dbt SQL generation |
| 3 | Pattern Implementation | 7 | P1-P7 generalization patterns |
| 4 | Remaining L3 Tables | 5 | Reference dims, facts |
| 5 | Malta Implementation | 4 | Malta-specific deployment |
| 6 | Documentation & Deployment | 4 | Docs, CI/CD |

---

## Critical Path

```
0.1 → 0.2 → 0.3 → 2.1 → 2.4 → 3.2 (dim_party with SCD2)
                      ↘
                   2.3 → 3.1 (dim_country - P1 foundation)
                           ↘
                         3.3 → 3.5 → 4.4 (tax scheme → imputation)
                           ↘
                         3.6 → 3.7 (fiscal year → holidays)
                                  ↘
                               4.3 → 5.3 (facts → Malta data)
```

---

## Phase 0: Project Infrastructure (5 iterations)

### Iteration 0.1: dbt Project Initialization

| Attribute | Value |
|-----------|-------|
| **ID** | 0.1 |
| **Name** | Initialize dbt Project Structure |
| **Prerequisites** | None |
| **Complexity** | Low |

**Inputs**:
- `generators/yaml_to_dbt_generator.py` (reference for patterns)

**Outputs**:
- `ta-rdm-dbt/dbt_project.yml`
- `ta-rdm-dbt/profiles.yml.example`
- `ta-rdm-dbt/packages.yml`
- Directory structure: `models/`, `macros/`, `seeds/`, `tests/`, `snapshots/`

**Tasks**:
1. Create `ta-rdm-dbt/` directory
2. Generate `dbt_project.yml` with ClickHouse configuration
3. Create standard folder structure
4. Configure `packages.yml` (dbt-utils, dbt-expectations)
5. Create profiles template for ClickHouse

**Test Criteria**:
- [ ] `dbt debug` passes
- [ ] `dbt deps` installs packages successfully
- [ ] Directory structure matches specification

---

### Iteration 0.2: Core dbt Macros - Surrogate Keys and Data Types

| Attribute | Value |
|-----------|-------|
| **ID** | 0.2 |
| **Name** | Create Core Transformation Macros |
| **Prerequisites** | 0.1 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/*.yaml` (data type patterns)
- `generators/yaml_to_dbt_generator.py` (TRANSFORM_MACROS reference)

**Outputs**:
- `macros/surrogate_key.sql`
- `macros/data_type_cast.sql`
- `macros/clickhouse_types.sql`

**Tasks**:
1. Create `generate_surrogate_key` macro (ClickHouse-compatible)
2. Create `cast_to_clickhouse_type` for type conversions
3. Create data type mapping macros (Decimal64, Date32, FixedString)
4. Add unit tests for macros

**Test Criteria**:
- [ ] Macros compile without errors
- [ ] Test queries produce expected type outputs
- [ ] Surrogate key generation is deterministic

---

### Iteration 0.3: Core dbt Macros - SCD Type 2

| Attribute | Value |
|-----------|-------|
| **ID** | 0.3 |
| **Name** | Create SCD Type 2 Macro Framework |
| **Prerequisites** | 0.2 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_party.yaml` (SCD2 pattern reference)

**Outputs**:
- `macros/scd_type_2.sql`
- `macros/scd_type_1.sql`
- `macros/change_detection.sql`

**Tasks**:
1. Create SCD Type 2 macro with `effective_from/to`, `is_current`, `version_number`
2. Create SCD Type 1 macro (overwrite)
3. Create `detect_changes` macro using checksum comparison
4. Support ClickHouse ReplacingMergeTree patterns

**Test Criteria**:
- [ ] SCD2 macro correctly identifies new, changed, unchanged records
- [ ] Version increments work correctly
- [ ] Effective dates set properly for expired records

---

### Iteration 0.4: Reference Seed Data Structure

| Attribute | Value |
|-----------|-------|
| **ID** | 0.4 |
| **Name** | Create Reference Data Seed Framework |
| **Prerequisites** | 0.1 |
| **Complexity** | Low |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_country.yaml` (sample_data section)
- `dw-L2-L3/_model-L2-L3/ref_identifier_type.yaml`

**Outputs**:
- `seeds/ref_country.csv`
- `seeds/ref_identifier_type.csv`
- `seeds/schema.yml` (seed properties)

**Tasks**:
1. Extract sample_data from dim_country.yaml to CSV
2. Create identifier type reference data
3. Create seed schema with column types
4. Verify seed loading

**Test Criteria**:
- [ ] `dbt seed` loads all reference data without errors
- [ ] Row counts match expected values
- [ ] Column types are correct

---

### Iteration 0.5: Test Harness Setup

| Attribute | Value |
|-----------|-------|
| **ID** | 0.5 |
| **Name** | Create Test Harness and CI Configuration |
| **Prerequisites** | 0.1, 0.4 |
| **Complexity** | Medium |

**Inputs**: None (framework setup)

**Outputs**:
- `tests/generic/test_*.sql`
- `tests/data_quality/*.sql`
- `.github/workflows/dbt_ci.yml`

**Tasks**:
1. Create referential integrity test patterns
2. Create data quality test macros
3. Set up CI workflow for dbt test execution
4. Create test documentation template

**Test Criteria**:
- [ ] Tests execute without infrastructure errors
- [ ] CI pipeline runs successfully on commit

---

## Phase 1: Generator Extension - Parser (6 iterations)

### Iteration 1.1: L2-L3 YAML Parser - Base Structure

| Attribute | Value |
|-----------|-------|
| **ID** | 1.1 |
| **Name** | Create L2-L3 YAML Parser Base Class |
| **Prerequisites** | 0.1 |
| **Complexity** | Medium |

**Inputs**:
- `generators/yaml_to_dbt_generator.py` (FormYAMLParser class pattern)
- `dw-L2-L3/_model-L2-L3/dim_country.yaml` (sample dimension YAML)

**Outputs**:
- `generators/l2_l3_parser.py` (new file)

**Tasks**:
1. Create `L2L3YAMLParser` class with `__init__`, `parse()` methods
2. Parse metadata section (dimension_name, schema, version)
3. Parse dimension specification (type, grain, natural_key, surrogate_key)
4. Create dataclasses: `DimensionSpec`, `FactSpec`, `ColumnSpec`
5. Handle both dimension and fact YAML structures

**Test Criteria**:
- [ ] Parser reads dim_country.yaml without errors
- [ ] All metadata fields extracted correctly
- [ ] Dataclasses populated with correct values

---

### Iteration 1.2: Column Parser

| Attribute | Value |
|-----------|-------|
| **ID** | 1.2 |
| **Name** | Parse Column Definitions |
| **Prerequisites** | 1.1 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_party.yaml` (complex column definitions)

**Outputs**:
- Extended `L2L3YAMLParser` with column parsing
- `ColumnSpec` dataclass with all attributes

**Tasks**:
1. Parse column name, data_type, nullable, default
2. Parse role (SURROGATE_KEY, NATURAL_KEY, FOREIGN_KEY)
3. Parse foreign_key references (dimension, column)
4. Parse source mapping information
5. Handle `change_from_v1` migration notes

**Test Criteria**:
- [ ] All 70+ columns from dim_party.yaml parsed correctly
- [ ] Foreign key references captured with target table/column
- [ ] Data types mapped to ClickHouse equivalents

---

### Iteration 1.3: Relationship and Index Parser

| Attribute | Value |
|-----------|-------|
| **ID** | 1.3 |
| **Name** | Parse Indexes, Business Rules, and ETL Mapping |
| **Prerequisites** | 1.2 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_party.yaml` (indexes, business_rules, etl_source sections)

**Outputs**:
- `IndexSpec`, `BusinessRuleSpec`, `ETLSourceSpec` dataclasses
- Extended parser methods

**Tasks**:
1. Parse indexes section (columns, unique, where clause)
2. Parse business_rules section (rule_id, validation SQL, severity)
3. Parse etl_source section (source_tables, transformations)
4. Parse removed_columns for migration tracking

**Test Criteria**:
- [ ] All indexes from dim_party.yaml captured
- [ ] Business rules with SQL validation extracted
- [ ] ETL source transformations parsed

---

### Iteration 1.4: Fact Table Parser

| Attribute | Value |
|-----------|-------|
| **ID** | 1.4 |
| **Name** | Parse Fact Table Specifications |
| **Prerequisites** | 1.3 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/fact_filing.yaml`
- `dw-L2-L3/_model-L2-L3/fact_refund.yaml`

**Outputs**:
- `FactSpec` dataclass with dimension_keys, date_keys, measures
- Extended parser for fact tables

**Tasks**:
1. Parse dimension_keys section (FK relationships, nullable)
2. Parse date_keys section (role-playing dimensions)
3. Parse measures_additive, measures_semi_additive, measures_non_additive
4. Parse degenerate_dimensions
5. Parse flags (boolean indicators)
6. Parse partitioning specification

**Test Criteria**:
- [ ] All measures from fact_filing.yaml categorized correctly
- [ ] Date role-playing dimensions captured (4-5 date keys)
- [ ] Dimension FK relationships identified

---

### Iteration 1.5: Bridge Table Parser

| Attribute | Value |
|-----------|-------|
| **ID** | 1.5 |
| **Name** | Parse Bridge Table Specifications |
| **Prerequisites** | 1.3 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/bridge_party_identifier.yaml`
- `dw-L2-L3/_model-L2-L3/bridge_refund_account.yaml`

**Outputs**:
- `BridgeSpec` dataclass
- Extended parser for bridge tables

**Tasks**:
1. Parse bridge table structure (different from dimension/fact)
2. Parse relationship endpoints (left/right dimensions)
3. Parse bridge-specific attributes (weight, priority, effective_date)

**Test Criteria**:
- [ ] Both bridge tables parse correctly
- [ ] Relationship endpoints identified
- [ ] Bridge attributes captured

---

### Iteration 1.6: Reference Table Parser

| Attribute | Value |
|-----------|-------|
| **ID** | 1.6 |
| **Name** | Parse Reference Table Specifications |
| **Prerequisites** | 1.3 |
| **Complexity** | Low |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/ref_country_holiday.yaml`
- `dw-L2-L3/_model-L2-L3/ref_tax_scheme.yaml`

**Outputs**:
- `ReferenceTableSpec` dataclass
- Extended parser

**Tasks**:
1. Parse reference table structure
2. Extract sample_data as seed candidates
3. Parse calculation_rules for dynamic reference data

**Test Criteria**:
- [ ] All reference tables parse correctly
- [ ] Sample data extracted for seeds
- [ ] Calculation rules (e.g., Easter-based holidays) identified

---

## Phase 2: Generator Extension - Model Generation (8 iterations)

### Iteration 2.1: dbt Model Template Engine

| Attribute | Value |
|-----------|-------|
| **ID** | 2.1 |
| **Name** | Create dbt SQL Template Engine |
| **Prerequisites** | 1.1 |
| **Complexity** | Medium |

**Inputs**:
- `generators/yaml_to_dbt_generator.py` (template patterns)

**Outputs**:
- `generators/dbt_template_engine.py`
- `generators/templates/dimension.sql.j2`
- `generators/templates/fact.sql.j2`

**Tasks**:
1. Create Jinja2 template engine wrapper class
2. Create base dimension template with config block
3. Create base fact template with partitioning
4. Add header comment generation with lineage

**Test Criteria**:
- [ ] Templates render without Jinja errors
- [ ] Output SQL is syntactically valid
- [ ] Config blocks correctly formatted

---

### Iteration 2.2: Staging Model Generator

| Attribute | Value |
|-----------|-------|
| **ID** | 2.2 |
| **Name** | Generate L2 Source Staging Models |
| **Prerequisites** | 2.1, 1.2 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-core/01-tax-party.yaml` (L2 source schema)

**Outputs**:
- `generators/templates/staging.sql.j2`
- Generated `stg_party__party.sql`, etc.

**Tasks**:
1. Create staging template with incremental config
2. Generate column selections from L2 schemas
3. Add type casting per column spec
4. Generate source YAML definitions

**Test Criteria**:
- [ ] Staging models compile
- [ ] Sources resolve correctly
- [ ] Incremental logic correct

---

### Iteration 2.3: SCD Type 1 Dimension Generator

| Attribute | Value |
|-----------|-------|
| **ID** | 2.3 |
| **Name** | Generate Type 1 Dimension Models |
| **Prerequisites** | 2.1, 0.3, 1.2 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_country.yaml`

**Outputs**:
- `generators/templates/dim_type1.sql.j2`
- Generated `dim_country.sql`

**Tasks**:
1. Create Type 1 template (full overwrite)
2. Generate surrogate key assignment
3. Add ETL metadata columns (_loaded_at, _source)
4. Generate dbt tests from business_rules

**Test Criteria**:
- [ ] dim_country.sql compiles and runs
- [ ] Row counts match seed data
- [ ] Surrogate keys assigned correctly

---

### Iteration 2.4: SCD Type 2 Dimension Generator

| Attribute | Value |
|-----------|-------|
| **ID** | 2.4 |
| **Name** | Generate Type 2 Dimension Models |
| **Prerequisites** | 2.3, 0.3 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_party.yaml`

**Outputs**:
- `generators/templates/dim_type2.sql.j2`
- Generated `dim_party.sql`

**Tasks**:
1. Create Type 2 template with history tracking
2. Implement change detection using checksum
3. Generate effective_from/to, is_current, version_number logic
4. Handle ClickHouse ReplacingMergeTree specifics

**Test Criteria**:
- [ ] Dimension model handles new, changed, unchanged correctly
- [ ] Version numbers increment on change
- [ ] is_current flag maintained properly

---

### Iteration 2.5: Fact Table Generator

| Attribute | Value |
|-----------|-------|
| **ID** | 2.5 |
| **Name** | Generate Fact Table Models |
| **Prerequisites** | 2.1, 1.4 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/fact_filing.yaml`

**Outputs**:
- `generators/templates/fact.sql.j2`
- Generated `fact_filing.sql`

**Tasks**:
1. Create fact template with dimension joins
2. Generate dimension key lookups
3. Generate role-playing date key joins
4. Add measure calculations
5. Implement partitioning config

**Test Criteria**:
- [ ] Fact model compiles
- [ ] All FK joins resolve to dimensions
- [ ] Measures correctly categorized

---

### Iteration 2.6: Bridge Table Generator

| Attribute | Value |
|-----------|-------|
| **ID** | 2.6 |
| **Name** | Generate Bridge Table Models |
| **Prerequisites** | 2.1, 1.5 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/bridge_party_identifier.yaml`

**Outputs**:
- `generators/templates/bridge.sql.j2`
- Generated `bridge_party_identifier.sql`

**Tasks**:
1. Create bridge table template
2. Handle many-to-many relationships
3. Generate allocation/weighting logic

**Test Criteria**:
- [ ] Bridge table compiles
- [ ] Relationship integrity maintained
- [ ] Weight/priority columns handled

---

### Iteration 2.7: Schema YAML Generator

| Attribute | Value |
|-----------|-------|
| **ID** | 2.7 |
| **Name** | Generate dbt Schema YAML Files |
| **Prerequisites** | 2.3, 2.4, 2.5 |
| **Complexity** | Medium |

**Inputs**: Parsed specs from Phase 1

**Outputs**:
- `generators/templates/schema.yml.j2`
- Generated `schema.yml` for each model folder

**Tasks**:
1. Generate model documentation from YAML metadata
2. Generate column descriptions
3. Generate test definitions from business_rules
4. Generate relationship tests

**Test Criteria**:
- [ ] Schema YAML is valid
- [ ] Tests execute
- [ ] Documentation renders in dbt docs

---

### Iteration 2.8: Generator CLI and Orchestration

| Attribute | Value |
|-----------|-------|
| **ID** | 2.8 |
| **Name** | Create Generator Command Line Interface |
| **Prerequisites** | 2.1-2.7 |
| **Complexity** | Medium |

**Inputs**: All generator components

**Outputs**:
- `generators/generate_dbt_models.py` (CLI entry point)
- README with usage instructions

**Tasks**:
1. Create argparse CLI with --input, --output, --type options
2. Implement model type filtering (dimension, fact, bridge, all)
3. Add dry-run mode
4. Generate summary report

**Test Criteria**:
- [ ] CLI generates all model types from YAML
- [ ] Output structure matches dbt conventions
- [ ] Dry-run shows planned actions

---

## Phase 3: Pattern Implementation (7 iterations)

### Iteration 3.1: P1 - Country Dimension Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.1 |
| **Name** | Implement Country Dimension (P1) |
| **Prerequisites** | 2.3, 0.4 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_country.yaml`
- `dw-L2-L3/_docs/L3-Generalization-Patterns-Catalog.md` (P1 section)

**Outputs**:
- `models/warehouse/dimensions/dim_country.sql`
- `seeds/ref_country.csv`
- `tests/dim_country_tests.sql`

**Tasks**:
1. Generate dim_country model from YAML
2. Create seed data for 5 countries (MLT, LKA, MDA, UKR, LBN)
3. Implement fiscal year configuration columns
4. Add EU/Eurozone membership flags
5. Create validation tests

**Malta Configuration**:
```yaml
MLT:
  fiscal_year_start_month: 1
  fiscal_year_offset_years: 1  # YA = CY + 1
  fiscal_year_label_pattern: "YA{YEAR}"
  currency_code: EUR
  has_imputation_system: 1
  is_eu_member: 1
```

**Test Criteria**:
- [ ] dim_country loads 5 countries
- [ ] Malta fiscal year: 2024-01-15 → YA2025
- [ ] Sri Lanka fiscal year: 2024-05-15 → FY2024/25

---

### Iteration 3.2: P2 - Multi-Identifier Party Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.2 |
| **Name** | Implement Multi-Identifier Party (P2) |
| **Prerequisites** | 3.1, 2.4, 2.6 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_party.yaml`
- `dw-L2-L3/_model-L2-L3/bridge_party_identifier.yaml`

**Outputs**:
- `models/warehouse/dimensions/dim_party.sql`
- `models/warehouse/bridges/bridge_party_identifier.sql`
- `seeds/ref_identifier_type.csv`

**Tasks**:
1. Generate dim_party with generic identifier columns:
   - `primary_tax_id` (replaces Malta TIN)
   - `primary_tax_id_type` (TIN, IDNO, EDRPOU, IPN, NIC)
   - `secondary_id`, `secondary_id_type`
2. Implement SCD Type 2 for party history
3. Create bridge_party_identifier for all identifiers
4. Add country_key foreign key
5. Create identifier type reference data

**Identifier Types by Country**:
| Country | Primary | Secondary | Other |
|---------|---------|-----------|-------|
| Malta | TIN | VAT, EORI | National ID |
| Sri Lanka | TIN | VAT | NIC, Passport |
| Moldova | IDNO | VAT | Passport |
| Ukraine | EDRPOU | IPN | Passport |

**Test Criteria**:
- [ ] Parties have country context via country_key
- [ ] Multiple identifiers per party via bridge
- [ ] TIN format validation by country

---

### Iteration 3.3: P3 - Tax Scheme Dimension Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.3 |
| **Name** | Implement Tax Scheme Dimension (P3) |
| **Prerequisites** | 3.1 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_tax_scheme.yaml`
- `dw-L2-L3/_model-L2-L3/ref_tax_scheme.yaml`

**Outputs**:
- `models/warehouse/dimensions/dim_tax_scheme.sql`
- `seeds/ref_tax_scheme.csv`

**Tasks**:
1. Generate dim_tax_scheme from YAML
2. Create Malta VAT schemes seed data
3. Implement threshold and filing frequency columns
4. Link to dim_country and dim_tax_type

**Malta VAT Schemes**:
| Scheme | Code | Threshold | Input Recovery | Filing |
|--------|------|-----------|----------------|--------|
| Article 10 | MLT_VAT_ART10 | EUR 35,000 | 100% | Quarterly |
| Article 11 | MLT_VAT_ART11 | EUR 20,000 | 0% | Annual |
| Article 12 | MLT_VAT_ART12 | EUR 10,000 | 100% | Quarterly |

**Test Criteria**:
- [ ] Malta Article schemes loaded with correct thresholds
- [ ] Filing frequencies queryable by scheme
- [ ] Country/tax type relationship maintained

---

### Iteration 3.4: P4 - Generic Geography Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.4 |
| **Name** | Implement Generic Geography Hierarchy (P4) |
| **Prerequisites** | 3.1 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_geography.yaml`

**Outputs**:
- `models/warehouse/dimensions/dim_geography.sql`
- `seeds/ref_geography_malta.csv`
- `seeds/ref_geography_srilanka.csv`

**Tasks**:
1. Generate dim_geography with 4-level hierarchy:
   - Level 1: Region/Province
   - Level 2: District/County
   - Level 3: Municipality/City
   - Level 4: Neighborhood/Locality
2. Create Malta geography seed (68 localities)
3. Create Sri Lanka geography seed (9 provinces, 25 districts)
4. Implement hierarchy traversal (parent_geography_key)
5. Add full_path generation

**Geography Mapping**:
| Level | Malta | Sri Lanka |
|-------|-------|-----------|
| 1 | Island | Province |
| 2 | District | District |
| 3 | Locality | DS Division |
| 4 | - | GN Division |

**Test Criteria**:
- [ ] Malta 68 localities loaded with hierarchy
- [ ] Sri Lanka 9 provinces, 25 districts loaded
- [ ] Hierarchy traversal works (get all children of district)

---

### Iteration 3.5: P5 - Account Subtype Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.5 |
| **Name** | Implement Account Subtype for Imputation (P5) |
| **Prerequisites** | 3.1 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_account_subtype.yaml`
- `dw-L2-L3/_model-L2-L3/bridge_refund_account.yaml`

**Outputs**:
- `models/warehouse/dimensions/dim_account_subtype.sql`
- `models/warehouse/bridges/bridge_refund_account.sql`
- `seeds/ref_account_subtype.csv`

**Tasks**:
1. Generate dim_account_subtype
2. Create Malta imputation accounts seed
3. Create standard (STD) account for non-imputation countries
4. Generate bridge_refund_account for refund allocation
5. Implement refund rate calculations

**Malta Imputation Accounts**:
| Code | Name | Refund Rate | Is Imputation |
|------|------|-------------|---------------|
| FTA | Foreign Tax Account | 90% | Yes |
| MTA | Malta Tax Account | 85.71% (6/7) | Yes |
| FIA | Foreign Imputation Account | 0% | Yes |
| IPA | Imputation Payable Account | Variable | Yes |
| UA | Unrelieved Account | 0% | Yes |
| STD | Standard | N/A | No |

**Test Criteria**:
- [ ] Malta 5 imputation accounts with correct rates
- [ ] Non-Malta countries have STD account only
- [ ] bridge_refund_account links refund to account breakdown

---

### Iteration 3.6: P6 - Configurable Fiscal Year Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.6 |
| **Name** | Implement Configurable Fiscal Year (P6) |
| **Prerequisites** | 3.1 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_date.yaml`
- `dw-L2-L3/_model-L2-L3/dim_tax_period.yaml`

**Outputs**:
- `models/warehouse/dimensions/dim_date.sql`
- `models/warehouse/dimensions/dim_tax_period.sql`
- `macros/fiscal_year_for_country.sql`

**Tasks**:
1. Generate dim_date with multiple fiscal year columns:
   - `fiscal_year_jan` (Jan-Dec)
   - `fiscal_year_apr` (Apr-Mar, for Sri Lanka)
   - `fiscal_year_jul` (Jul-Jun)
   - `fiscal_year_jan_offset1` (Jan-Dec + 1, for Malta YA)
2. Create `fiscal_year_for_country` macro
3. Generate dim_tax_period with country_key
4. Pre-populate 50 years of dates (2000-2050)

**Fiscal Year Logic**:
```sql
-- Malta: Year of Assessment = Calendar Year + 1
fiscal_year_jan_offset1 = YEAR(calendar_date) + 1

-- Sri Lanka: April-March fiscal year
fiscal_year_apr = CASE
  WHEN MONTH(calendar_date) >= 4 THEN YEAR(calendar_date)
  ELSE YEAR(calendar_date) - 1
END
```

**Test Criteria**:
- [ ] Malta: 2024-01-15 → fiscal_year_jan_offset1 = 2025 (YA2025)
- [ ] Sri Lanka: 2024-05-15 → fiscal_year_apr = 2024 (FY2024/25)
- [ ] dim_tax_period has country context

---

### Iteration 3.7: P7 - Externalized Holidays Pattern

| Attribute | Value |
|-----------|-------|
| **ID** | 3.7 |
| **Name** | Implement Externalized Holidays (P7) |
| **Prerequisites** | 3.1, 3.6 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/ref_country_holiday.yaml`

**Outputs**:
- `models/reference/ref_country_holiday.sql`
- `seeds/ref_country_holiday_malta.csv`
- `seeds/ref_country_holiday_srilanka.csv`
- `macros/is_holiday.sql`

**Tasks**:
1. Generate ref_country_holiday from YAML
2. Create Malta holiday seed (14 public holidays, 2024-2030)
3. Create Sri Lanka holiday seed (26 holidays, 2024-2030)
4. Implement fixed vs calculated holiday logic (Easter-based)
5. Create `is_holiday` macro for query-time lookup

**Holiday Lookup Pattern**:
```sql
-- Instead of is_malta_public_holiday in dim_date
SELECT
  d.date_key,
  d.calendar_date,
  CASE WHEN h.holiday_date IS NOT NULL THEN 1 ELSE 0 END AS is_holiday,
  h.holiday_name
FROM dim_date d
LEFT JOIN ref_country_holiday h
  ON d.calendar_date = h.holiday_date
  AND h.country_code = 'MLT'
```

**Test Criteria**:
- [ ] Malta 14 holidays loaded for 2024-2030
- [ ] Easter-based holidays calculated correctly
- [ ] is_holiday macro works for any country

---

## Phase 4: Remaining L3 Tables (5 iterations)

### Iteration 4.1: Reference Dimensions (Type 1)

| Attribute | Value |
|-----------|-------|
| **ID** | 4.1 |
| **Name** | Generate All Reference Dimensions |
| **Prerequisites** | 2.3 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_assessment_type.yaml`
- `dw-L2-L3/_model-L2-L3/dim_compliance_status.yaml`
- `dw-L2-L3/_model-L2-L3/dim_customs_procedure.yaml`
- `dw-L2-L3/_model-L2-L3/dim_payment_method.yaml`
- `dw-L2-L3/_model-L2-L3/dim_risk_category.yaml`
- `dw-L2-L3/_model-L2-L3/dim_international_country.yaml`

**Outputs**:
- 6 dimension model files
- Corresponding seed files

**Tasks**:
1. Generate dim_assessment_type (SELF, DESK, FIELD, AMENDED)
2. Generate dim_compliance_status (COMPLIANT, NON_FILER, LATE, etc.)
3. Generate dim_customs_procedure (IMPORT, EXPORT, TRANSIT)
4. Generate dim_payment_method (BANK, CARD, CASH, CHECK)
5. Generate dim_risk_category (LOW, MEDIUM, HIGH, CRITICAL)
6. Generate dim_international_country (ISO 3166 countries)

**Test Criteria**:
- [ ] All 6 dimensions compile and load
- [ ] Reference data matches specifications
- [ ] Foreign keys resolve

---

### Iteration 4.2: dim_tax_type and dim_registration

| Attribute | Value |
|-----------|-------|
| **ID** | 4.2 |
| **Name** | Generate Tax Type and Registration Dimensions |
| **Prerequisites** | 3.1, 3.3, 2.4 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/dim_tax_type.yaml`
- `dw-L2-L3/_model-L2-L3/dim_registration.yaml`

**Outputs**:
- `models/warehouse/dimensions/dim_tax_type.sql`
- `models/warehouse/dimensions/dim_registration.sql`

**Tasks**:
1. Generate dim_tax_type with country_key
2. Generate dim_registration with tax_scheme_key FK
3. Add SCD Type 2 for registration changes
4. Create FK relationships

**Test Criteria**:
- [ ] Tax types have country context
- [ ] Registration links to tax_scheme correctly
- [ ] SCD2 tracks registration status changes

---

### Iteration 4.3: Core Fact Tables

| Attribute | Value |
|-----------|-------|
| **ID** | 4.3 |
| **Name** | Generate Filing, Assessment, Payment Facts |
| **Prerequisites** | 2.5, 3.1-3.7, 4.2 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/fact_filing.yaml`
- `dw-L2-L3/_model-L2-L3/fact_assessment.yaml`
- `dw-L2-L3/_model-L2-L3/fact_payment.yaml`

**Outputs**:
- `models/warehouse/facts/fact_filing.sql`
- `models/warehouse/facts/fact_assessment.sql`
- `models/warehouse/facts/fact_payment.sql`

**Tasks**:
1. Generate fact_filing with all dimension FKs
2. Generate fact_assessment
3. Generate fact_payment
4. Add country_key to ALL facts (P1 requirement)
5. Implement role-playing date dimensions

**Fact Table Structure** (fact_filing example):
```sql
-- Dimension Keys
country_key         -- P1: Country context
party_key           -- P2: Multi-identifier party
tax_type_key
tax_period_key
registration_key
geography_key

-- Date Keys (role-playing)
filing_date_key
due_date_key
received_date_key
processed_date_key

-- Measures
gross_income_amount
deductions_amount
tax_due_amount
```

**Test Criteria**:
- [ ] All facts compile with FK resolution
- [ ] Country context (country_key) on all facts
- [ ] Role-playing dates work correctly

---

### Iteration 4.4: Refund Fact with Imputation

| Attribute | Value |
|-----------|-------|
| **ID** | 4.4 |
| **Name** | Generate Refund Fact with Bridge Integration |
| **Prerequisites** | 4.3, 3.5 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/fact_refund.yaml`

**Outputs**:
- `models/warehouse/facts/fact_refund.sql`

**Tasks**:
1. Generate fact_refund with generalized structure
2. Remove Malta-specific imputation columns:
   - ~~refund_fta_amount~~
   - ~~refund_mta_amount~~
   - ~~refund_fia_amount~~
3. Integrate with bridge_refund_account
4. Add country-conditional imputation logic

**Malta Imputation Query Pattern**:
```sql
-- Get Malta refund breakdown
SELECT
  r.refund_key,
  r.claimed_amount,
  a.account_subtype_code,
  b.refund_amount,
  b.refund_rate_applied
FROM fact_refund r
JOIN bridge_refund_account b ON r.refund_source_id = b.refund_source_id
JOIN dim_account_subtype a ON b.account_subtype_key = a.account_subtype_key
WHERE r.country_key = (SELECT country_key FROM dim_country WHERE country_code = 'MLT')
```

**Test Criteria**:
- [ ] Malta refunds use bridge for imputation breakdown
- [ ] Non-Malta refunds work without imputation complexity
- [ ] Refund totals reconcile

---

### Iteration 4.5: Remaining Facts and Snapshots

| Attribute | Value |
|-----------|-------|
| **ID** | 4.5 |
| **Name** | Generate Account Balance, Compliance, Risk Facts |
| **Prerequisites** | 4.3 |
| **Complexity** | Medium |

**Inputs**:
- `dw-L2-L3/_model-L2-L3/fact_account_balance.yaml`
- `dw-L2-L3/_model-L2-L3/fact_compliance_event.yaml`
- `dw-L2-L3/_model-L2-L3/fact_customs_declaration.yaml`
- `dw-L2-L3/_model-L2-L3/fact_risk_score.yaml`

**Outputs**:
- `models/warehouse/facts/fact_account_balance.sql`
- `models/warehouse/facts/fact_compliance_event.sql`
- `models/warehouse/facts/fact_customs_declaration.sql`
- `models/warehouse/facts/fact_risk_score.sql`

**Tasks**:
1. Generate fact_account_balance (periodic snapshot - monthly)
2. Generate fact_compliance_event (factless fact)
3. Generate fact_customs_declaration
4. Generate fact_risk_score (periodic snapshot - quarterly)

**Test Criteria**:
- [ ] All facts compile
- [ ] Snapshot facts handle period-end logic
- [ ] Factless fact captures events correctly

---

## Phase 5: Malta Implementation (4 iterations)

### Iteration 5.1: Malta L2 Source Configuration

| Attribute | Value |
|-----------|-------|
| **ID** | 5.1 |
| **Name** | Configure Malta L2 Source Mappings |
| **Prerequisites** | 0.4, 2.2 |
| **Complexity** | High |

**Inputs**:
- `dw-L2-L3/_model-L2-core/` (all 13 L2 schema files)
- `ta-rdm-etl/` (existing ETL for reference)

**Outputs**:
- `models/sources/malta_l2_sources.yml`
- `models/staging/malta/` directory

**Tasks**:
1. Create Malta-specific source definitions
2. Generate staging models for party, registration, filing domains
3. Map Informix column names to canonical names
4. Handle Malta-specific data transformations

**Test Criteria**:
- [ ] Sources connect to Malta L2 database
- [ ] Staging models compile
- [ ] Column mappings correct

---

### Iteration 5.2: Malta Dimension Population

| Attribute | Value |
|-----------|-------|
| **ID** | 5.2 |
| **Name** | Load Malta-Specific Dimension Data |
| **Prerequisites** | 5.1, 3.1-3.7 |
| **Complexity** | Medium |

**Inputs**: Malta L2 database

**Outputs**:
- `models/intermediate/malta/` with Malta transforms
- Populated Malta dimensions

**Tasks**:
1. Load Malta geography (68 localities)
2. Load Malta tax scheme configurations
3. Load Malta imputation account setup
4. Load Malta identifier types
5. Load Malta holiday calendar

**Test Criteria**:
- [ ] dim_geography: 68 Malta localities
- [ ] dim_tax_scheme: Article 10, 11, 12 configured
- [ ] dim_account_subtype: 5 imputation accounts

---

### Iteration 5.3: Malta Fact Population

| Attribute | Value |
|-----------|-------|
| **ID** | 5.3 |
| **Name** | Load Malta Fact Data |
| **Prerequisites** | 5.2, 4.3, 4.4 |
| **Complexity** | High |

**Inputs**: Malta L2 filing, assessment, payment data

**Outputs**:
- Populated Malta fact tables

**Tasks**:
1. Load fact_filing from Malta tax_return
2. Load fact_assessment
3. Load fact_payment
4. Load fact_refund with imputation breakdown
5. Verify FK integrity

**Test Criteria**:
- [ ] Filing records load with dimension keys resolved
- [ ] Refund imputation bridge populated correctly
- [ ] All FK references valid

---

### Iteration 5.4: Malta Validation and Testing

| Attribute | Value |
|-----------|-------|
| **ID** | 5.4 |
| **Name** | Validate Malta Implementation |
| **Prerequisites** | 5.3 |
| **Complexity** | Medium |

**Inputs**: Loaded Malta data

**Outputs**:
- Validation reports
- Test results

**Tasks**:
1. Run all dbt tests
2. Compare row counts with existing Python ETL
3. Validate YA calculations match legacy
4. Test multi-country queries (Malta-only filter)
5. Create validation dashboard queries

**Test Criteria**:
- [ ] All dbt tests pass
- [ ] Row counts match legacy Python ETL within 1%
- [ ] YA calculation validated for sample records

---

## Phase 6: Documentation and Deployment (4 iterations)

### Iteration 6.1: Generator Documentation

| Attribute | Value |
|-----------|-------|
| **ID** | 6.1 |
| **Name** | Document Generator Usage |
| **Prerequisites** | 2.8 |
| **Complexity** | Low |

**Outputs**:
- `generators/README.md`
- `docs/generator-guide.md`

**Tasks**:
1. Document CLI usage and options
2. Document YAML format requirements
3. Create examples for each model type
4. Add troubleshooting section

**Test Criteria**:
- [ ] Documentation covers all generator features
- [ ] Examples work as documented

---

### Iteration 6.2: dbt Project Documentation

| Attribute | Value |
|-----------|-------|
| **ID** | 6.2 |
| **Name** | Document dbt Project Structure |
| **Prerequisites** | 4.5 |
| **Complexity** | Medium |

**Outputs**:
- `ta-rdm-dbt/README.md`
- `docs/model-overview.md`
- `docs/pattern-usage.md`
- Generated dbt docs site

**Tasks**:
1. Document project structure
2. Document macro usage
3. Document seed management
4. Create pattern implementation guide
5. Generate dbt docs: `dbt docs generate`

**Test Criteria**:
- [ ] `dbt docs generate` succeeds
- [ ] `dbt docs serve` shows complete lineage

---

### Iteration 6.3: Multi-Country Extension Guide

| Attribute | Value |
|-----------|-------|
| **ID** | 6.3 |
| **Name** | Create Country Extension Documentation |
| **Prerequisites** | 5.4 |
| **Complexity** | Medium |

**Outputs**:
- `docs/adding-new-country.md`
- `templates/country_seed_template.csv`

**Tasks**:
1. Document step-by-step country addition process
2. Create seed data templates for new country
3. Document required configurations
4. Create Sri Lanka implementation checklist

**Test Criteria**:
- [ ] Guide is actionable for Sri Lanka implementation
- [ ] Templates cover all required seed data

---

### Iteration 6.4: CI/CD and Deployment Configuration

| Attribute | Value |
|-----------|-------|
| **ID** | 6.4 |
| **Name** | Create CI/CD Pipeline |
| **Prerequisites** | 6.2 |
| **Complexity** | Medium |

**Outputs**:
- `.github/workflows/dbt_ci.yml`
- `scripts/deploy.sh`
- `Makefile`

**Tasks**:
1. Create GitHub Actions workflow
2. Implement test/build/deploy stages
3. Create deployment scripts
4. Document deployment process

**Test Criteria**:
- [ ] CI pipeline runs on PR
- [ ] Deployment script executes successfully

---

## Critical Files Reference

### Files to Modify
| File | Purpose |
|------|---------|
| `generators/yaml_to_dbt_generator.py` | Extend for L2-L3 generation |

### Files to Create
| File | Purpose |
|------|---------|
| `generators/l2_l3_parser.py` | New parser for schema YAMLs |
| `generators/dbt_template_engine.py` | Template rendering engine |
| `ta-rdm-dbt/` | Entire dbt project |

### Reference Files (Read-Only)
| File | Purpose |
|------|---------|
| `dw-L2-L3/_model-L2-core/*.yaml` | L2 canonical schemas (13 files) |
| `dw-L2-L3/_model-L2-L3/*.yaml` | L3 dimensional schemas (31 files) |
| `dw-L2-L3/_docs/L3-Generalization-Patterns-Catalog.md` | Pattern documentation |
| `ta-rdm-etl/etl/l2_to_l3_party.py` | Existing ETL reference |

---

## Appendix: Iteration Checklist

### Phase 0: Infrastructure
- [ ] 0.1: dbt Project Initialization
- [ ] 0.2: Core Macros - Surrogate Keys
- [ ] 0.3: Core Macros - SCD Type 2
- [ ] 0.4: Reference Seed Structure
- [ ] 0.5: Test Harness Setup

### Phase 1: Parser
- [ ] 1.1: Parser Base Structure
- [ ] 1.2: Column Parser
- [ ] 1.3: Relationship/Index Parser
- [ ] 1.4: Fact Table Parser
- [ ] 1.5: Bridge Table Parser
- [ ] 1.6: Reference Table Parser

### Phase 2: Generator
- [ ] 2.1: Template Engine
- [ ] 2.2: Staging Generator
- [ ] 2.3: SCD Type 1 Generator
- [ ] 2.4: SCD Type 2 Generator
- [ ] 2.5: Fact Generator
- [ ] 2.6: Bridge Generator
- [ ] 2.7: Schema YAML Generator
- [ ] 2.8: Generator CLI

### Phase 3: Patterns
- [ ] 3.1: P1 - Country Dimension
- [ ] 3.2: P2 - Multi-Identifier Party
- [ ] 3.3: P3 - Tax Scheme Dimension
- [ ] 3.4: P4 - Generic Geography
- [ ] 3.5: P5 - Account Subtype
- [ ] 3.6: P6 - Configurable Fiscal Year
- [ ] 3.7: P7 - Externalized Holidays

### Phase 4: L3 Tables
- [ ] 4.1: Reference Dimensions
- [ ] 4.2: Tax Type & Registration
- [ ] 4.3: Core Facts
- [ ] 4.4: Refund with Imputation
- [ ] 4.5: Remaining Facts

### Phase 5: Malta
- [ ] 5.1: L2 Source Configuration
- [ ] 5.2: Dimension Population
- [ ] 5.3: Fact Population
- [ ] 5.4: Validation

### Phase 6: Documentation
- [ ] 6.1: Generator Documentation
- [ ] 6.2: dbt Project Documentation
- [ ] 6.3: Multi-Country Guide
- [ ] 6.4: CI/CD Configuration

---

**Total Iterations**: 35
**Estimated Sessions**: 40-50 (some iterations may take multiple sessions)
**Critical Path Length**: ~20 iterations (0.1→0.2→0.3→2.1→2.4→3.2→4.3→5.3→5.4)
