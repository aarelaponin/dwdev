# TA-RDM ETL Architecture Documentation

**Version**: 1.0.0
**Date**: 2025-11-06
**Audience**: Technical Architects, System Designers, Senior Developers

---

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Data Architecture](#data-architecture)
4. [Component Architecture](#component-architecture)
5. [Technology Stack](#technology-stack)
6. [Data Flow](#data-flow)
7. [Integration Architecture](#integration-architecture)
8. [Security Architecture](#security-architecture)
9. [Performance Architecture](#performance-architecture)
10. [Deployment Architecture](#deployment-architecture)

---

## Overview

### Purpose

The TA-RDM (Tax Administration Reference Data Model) ETL system extracts operational tax data from MySQL Layer 2 (L2) database and transforms it into a dimensional data warehouse in ClickHouse Layer 3 (L3) for analytical purposes.

### System Context

```
┌─────────────────────────────────────────────────────────────────┐
│                     Tax Administration System                    │
│                                                                  │
│  ┌──────────────┐         ┌──────────────┐       ┌───────────┐ │
│  │              │         │              │       │           │ │
│  │ Operational  │ Extract │  ETL Layer   │ Load  │ Analytics │ │
│  │  Database    ├────────►│  (Python)    ├──────►│ Warehouse │ │
│  │  (MySQL L2)  │         │              │       │(ClickHouse│ │
│  │              │         │              │       │    L3)    │ │
│  └──────────────┘         └──────────────┘       └───────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Objectives

1. **Data Integration**: Consolidate tax data from MySQL operational database
2. **Data Transformation**: Convert operational data to analytical dimensional model
3. **Data Quality**: Ensure data accuracy, completeness, and consistency
4. **Performance**: Optimize for analytical query performance
5. **Maintainability**: Provide extensible, well-documented ETL framework

---

## System Architecture

### Architectural Style

The system follows a **Layered Architecture** pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Presentation Layer                        │
│              (Shell Scripts, Command Line Interface)             │
├─────────────────────────────────────────────────────────────────┤
│                         Business Logic Layer                     │
│           (ETL Scripts, Transformation Logic, Validators)        │
├─────────────────────────────────────────────────────────────────┤
│                        Data Access Layer                         │
│         (Database Utilities, Connection Management)              │
├─────────────────────────────────────────────────────────────────┤
│                         Data Layer                               │
│              (MySQL L2, ClickHouse L3, File System)              │
└─────────────────────────────────────────────────────────────────┘
```

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         TA-RDM ETL SYSTEM                                │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────┐         ┌──────────────────────────┐                  │
│  │   MySQL L2  │         │     ETL Components       │                  │
│  │             │         │                          │                  │
│  │ • Reference │────┐    │  ┌──────────────────┐   │                  │
│  │ • Party     │    │    │  │  Dimension ETLs  │   │                  │
│  │ • Tax Fwk   │    │    │  │  • dim_time      │   │                  │
│  │ • Filing    │    └───►│  │  • dim_party     │   │                  │
│  │ • Payment   │         │  │  • dim_tax_type  │   │                  │
│  │ • Accounting│         │  │  • dim_tax_period│   │                  │
│  │ • Compliance│         │  └──────────────────┘   │                  │
│  └─────────────┘         │                          │                  │
│                          │  ┌──────────────────┐   │                  │
│                          │  │   Fact ETLs      │   │                  │
│                          │  │  • Registration  │   │                  │
│                          │  │  • Filing        │   │                  │
│                          │  │  • Assessment    │   │   ┌────────────┐ │
│                          │  │  • Payment       │   │   │ ClickHouse │ │
│                          │  │  • Balance       │───┼──►│    L3      │ │
│                          │  │  • Collection    │   │   │            │ │
│                          │  │  • Refund        │   │   │ • Star     │ │
│                          │  │  • Audit         │   │   │   Schema   │ │
│                          │  │  • Objection     │   │   │ • Optimized│ │
│                          │  │  • Risk          │   │   │   for      │ │
│                          │  │  • Activity      │   │   │   Analytics│ │
│                          │  └──────────────────┘   │   └────────────┘ │
│                          │                          │                  │
│                          │  ┌──────────────────┐   │                  │
│                          │  │   Validators     │   │                  │
│                          │  │  • Row Count     │   │                  │
│                          │  │  • Integrity     │   │                  │
│                          │  │  • Quality       │   │                  │
│                          │  │  • Business Rules│   │                  │
│                          │  └──────────────────┘   │                  │
│                          └──────────────────────────┘                  │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Shared Infrastructure                         │   │
│  │  • Configuration Management  • Logging  • Error Handling         │   │
│  │  • Connection Pooling       • Monitoring • Utilities             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Architectural Principles

1. **Separation of Concerns**: Each component has a single, well-defined responsibility
2. **Modularity**: Independent ETL scripts can run separately or as a pipeline
3. **Reusability**: Shared utilities and configuration across all ETL scripts
4. **Extensibility**: Easy to add new dimensions or fact tables
5. **Testability**: Independent validation framework for data quality
6. **Configurability**: Environment-based configuration without code changes
7. **Idempotency**: ETL can be re-run safely (full reload strategy)

---

## Data Architecture

### Layered Data Architecture

The system implements a **two-layer data architecture**:

```
Layer 2 (L2)                          Layer 3 (L3)
Operational Database                  Analytical Warehouse
────────────────────                  ────────────────────

MySQL 9.0+                            ClickHouse 24.11+
Normalized (3NF)                      Dimensional (Star Schema)
OLTP Optimized                        OLAP Optimized
Row-oriented                          Column-oriented
High write volume                     High read volume
Real-time operations                  Batch analytics
7 schemas, 59 tables                  1 schema, 15 tables
```

### Data Model Evolution

```
L2 Normalized Model              L3 Dimensional Model
(Operational)                    (Analytical)

┌─────────────┐                 ┌───────────────┐
│   party     │                 │  dim_party    │
├─────────────┤                 ├───────────────┤
│ party_id    │────────────────►│ dim_party_key │
│ party_name  │      Extract    │ party_id      │
│ party_type  │      Transform  │ party_name    │
│ tin_number  │       Load      │ party_type    │
│ ...         │                 │ risk_rating   │
└─────────────┘                 │ is_current    │
                                │ valid_from    │
┌─────────────┐                 │ valid_to      │
│filing_return│                 └───────────────┘
├─────────────┤                          │
│ return_id   │                          │
│ party_id    │                          │
│ tax_type    │                          ▼
│ period_id   │                 ┌────────────────┐
│ filing_date │                 │  fact_filing   │
│ amount      │────────────────►├────────────────┤
│ ...         │                 │ dim_party_key  │
└─────────────┘                 │ dim_tax_type   │
                                │ dim_period     │
                                │ dim_date_key   │
                                │ filing_date    │
                                │ declared_amount│
                                │ is_late        │
                                │ ...            │
                                └────────────────┘
```

### Dimensional Model (Star Schema)

The L3 warehouse uses a **star schema** optimized for analytical queries:

```
                    ┌──────────────┐
                    │  dim_time    │
                    │──────────────│
                    │ date_key (PK)│
                    │ date         │
          ┌─────────│ year         │
          │         │ quarter      │
          │         │ month        │
          │         │ is_business  │
          │         └──────────────┘
          │
          │         ┌──────────────┐
          │         │  dim_party   │
          │         │──────────────│
          │         │ party_key(PK)│
          │    ┌────│ party_id     │
          │    │    │ party_name   │
          │    │    │ party_type   │
          │    │    │ risk_rating  │
          │    │    │ is_current   │
          │    │    └──────────────┘
          │    │
          │    │    ┌──────────────┐
          │    │    │dim_tax_type  │
          │    │    │──────────────│
          │    │    │tax_type_key  │
          │    │────│tax_type_code │
          │    │    │tax_type_name │
          │    │    │tax_category  │
          │    │    └──────────────┘
          │    │
          │    │    ┌──────────────┐
          │    │    │dim_tax_period│
          │    │    │──────────────│
          │    │────│period_key(PK)│
          │         │period_code   │
          │         │period_type   │
          │         │start_date    │
          │         │end_date      │
          │         └──────────────┘
          │
          │
    ┌─────▼─────────────────────────┐
    │      fact_filing               │
    │────────────────────────────────│
    │ dim_party_key (FK)             │
    │ dim_tax_type_key (FK)          │
    │ dim_tax_period_key (FK)        │
    │ dim_date_key (FK)              │
    │────────────────────────────────│
    │ filing_id                      │
    │ filing_date                    │
    │ declared_amount                │
    │ is_late                        │
    │ days_late                      │
    │ ...                            │
    └────────────────────────────────┘

    [Similar structure for other 10 fact tables]
```

### Data Grain

Each fact table has a specific **grain** (level of detail):

| Fact Table | Grain | Description |
|------------|-------|-------------|
| fact_registration | One row per tax account registration | Account setup events |
| fact_filing | One row per tax return filed | Filing submissions |
| fact_assessment | One row per tax assessment | Tax liability determinations |
| fact_payment | One row per payment transaction | Individual payments |
| fact_account_balance | One row per account per snapshot date | Daily balance snapshots |
| fact_collection | One row per enforcement action | Collection activities |
| fact_refund | One row per refund transaction | Refund requests/disbursements |
| fact_audit | One row per audit case | Audit examinations |
| fact_objection | One row per objection case | Taxpayer disputes |
| fact_risk_assessment | One row per taxpayer risk profile | Risk scoring |
| fact_taxpayer_activity | One row per taxpayer per month | Monthly aggregated activity |

---

## Component Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          ETL LAYER                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    ETL Scripts (16)                       │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────┐     │  │
│  │  │         Dimension ETLs (4)                      │     │  │
│  │  │  • generate_dim_time.py                         │     │  │
│  │  │  • l2_to_l3_party.py                            │     │  │
│  │  │  • l2_to_l3_tax_type.py                         │     │  │
│  │  │  • l2_to_l3_tax_period.py                       │     │  │
│  │  └─────────────────────────────────────────────────┘     │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────┐     │  │
│  │  │         Fact ETLs (11)                          │     │  │
│  │  │  • l2_to_l3_fact_registration.py                │     │  │
│  │  │  • l2_to_l3_fact_filing.py                      │     │  │
│  │  │  • l2_to_l3_fact_assessment.py                  │     │  │
│  │  │  • l2_to_l3_fact_payment.py                     │     │  │
│  │  │  • l2_to_l3_fact_account_balance.py             │     │  │
│  │  │  • l2_to_l3_fact_collection.py                  │     │  │
│  │  │  • l2_to_l3_fact_refund.py                      │     │  │
│  │  │  • l2_to_l3_fact_audit.py                       │     │  │
│  │  │  • l2_to_l3_fact_objection.py                   │     │  │
│  │  │  • l2_to_l3_fact_risk_assessment.py             │     │  │
│  │  │  • l2_to_l3_fact_taxpayer_activity.py           │     │  │
│  │  └─────────────────────────────────────────────────┘     │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────┐     │  │
│  │  │         Validation Script (1)                   │     │  │
│  │  │  • validate_etl.py                              │     │  │
│  │  └─────────────────────────────────────────────────┘     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                 Validation Framework                      │  │
│  │                                                           │  │
│  │  • mapping_config.py      (L2-L3 mappings)               │  │
│  │  • base_validator.py      (Base class)                   │  │
│  │  • row_count_validator.py (Count validation)             │  │
│  │  • integrity_validator.py (FK validation)                │  │
│  │  • quality_validator.py   (Data quality)                 │  │
│  │  • business_validator.py  (Business rules)               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                   Utility Layer                           │  │
│  │                                                           │  │
│  │  • db_utils.py            (MySQL connection)             │  │
│  │  • clickhouse_utils.py    (ClickHouse connection)        │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                Configuration Layer                        │  │
│  │                                                           │  │
│  │  • database_config.py     (MySQL config)                 │  │
│  │  • clickhouse_config.py   (ClickHouse config)            │  │
│  │  • .env                   (Environment variables)         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Helper Scripts                           │  │
│  │                                                           │  │
│  │  • run_full_etl.sh        (Full pipeline)                │  │
│  │  • run_validation.sh      (Validation only)              │  │
│  │  • test_connections.py    (Connection test)              │  │
│  │  • verify_package.py      (Package verification)         │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### ETL Scripts
- **Purpose**: Extract, transform, and load data for specific entities
- **Input**: MySQL L2 database
- **Output**: ClickHouse L3 database
- **Pattern**: Each script is self-contained and idempotent

#### Validation Framework
- **Purpose**: Ensure data quality and consistency
- **Input**: Both MySQL L2 and ClickHouse L3
- **Output**: Validation report (pass/fail)
- **Coverage**: 157 checks across 4 categories

#### Utility Layer
- **Purpose**: Provide reusable database operations
- **Responsibilities**:
  - Connection management
  - Query execution
  - Error handling
  - Transaction management

#### Configuration Layer
- **Purpose**: Centralize all configuration
- **Responsibilities**:
  - Database connection parameters
  - Environment-specific settings
  - Logging configuration

---

## Technology Stack

### Core Technologies

```
┌──────────────────────────────────────────────────────────────┐
│                    TECHNOLOGY STACK                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  Language & Runtime                                           │
│  • Python 3.9+           (Core ETL language)                 │
│                                                               │
│  Source Database (L2)                                         │
│  • MySQL 9.0+            (Operational database)              │
│  • InnoDB Storage        (Transaction support)               │
│                                                               │
│  Target Database (L3)                                         │
│  • ClickHouse 24.11+     (Analytical database)               │
│  • MergeTree Engine      (Optimized for analytics)           │
│                                                               │
│  Database Drivers                                             │
│  • mysql-connector-python 8.2.0+  (MySQL driver)             │
│  • clickhouse-connect 0.7.0+      (ClickHouse driver)        │
│                                                               │
│  Supporting Libraries                                         │
│  • python-dotenv 1.0.0+  (Configuration management)          │
│  • colorama 0.4.6+       (Terminal colors)                   │
│                                                               │
│  Execution Environment                                        │
│  • Bash Shell            (Pipeline orchestration)            │
│  • Cron / Airflow        (Scheduling)                        │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Technology Decisions

| Technology | Decision | Rationale |
|------------|----------|-----------|
| **Python 3.9+** | Core language | Rich ecosystem, easy maintenance, excellent database support |
| **MySQL 9.0+** | Source database | Provided operational database, ACID compliant |
| **ClickHouse 24.11+** | Target warehouse | Column-oriented, fast aggregations, excellent compression |
| **mysql-connector-python** | MySQL driver | Official driver, reliable, well-documented |
| **clickhouse-connect** | ClickHouse driver | Official driver, supports all ClickHouse features |
| **python-dotenv** | Configuration | Standard for environment variable management |
| **Full reload** | ETL strategy | Simple, reliable, suitable for data volumes |

---

## Data Flow

### End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ETL DATA FLOW                                │
└─────────────────────────────────────────────────────────────────────┘

Step 1: Extraction
┌──────────────┐
│  MySQL L2    │
│  Operational │  SELECT queries with JOINs
│  Database    ├───────────────────────────────┐
└──────────────┘                               │
                                               ▼
                                        ┌──────────────┐
                                        │  Python ETL  │
                                        │    Script    │
                                        └──────────────┘
                                               │
Step 2: Transformation                         │
                                               │
  • Dimension Key Lookup ◄───────────────────┤
  • Data Type Conversion                      │
  • Business Logic Application                │
  • Denormalization                           │
  • Aggregation (if needed)                   │
  • Data Enrichment                           │
  • SCD Type 2 Logic                          │
                                               │
                                               ▼
Step 3: Loading                         ┌──────────────┐
                                        │  In-Memory   │
                                        │  Batch       │
                                        │  (1000 rows) │
                                        └──────────────┘
                                               │
                                               │ Bulk INSERT
                                               ▼
                                        ┌──────────────┐
                                        │ ClickHouse   │
                                        │  L3 Data     │
                                        │  Warehouse   │
                                        └──────────────┘
```

### Detailed ETL Flow for Fact Tables

```
1. EXTRACT PHASE
   ┌────────────────────────────────────────────────────────┐
   │ MySQL L2                                               │
   │                                                        │
   │ Query joins multiple source tables:                   │
   │   • Main fact table                                   │
   │   • Related dimension tables (for lookups)            │
   │   • Supporting reference tables                       │
   │                                                        │
   │ Returns: List of dictionaries (rows)                  │
   └────────────────────────────────────────────────────────┘
                          ↓

2. TRANSFORM PHASE
   ┌────────────────────────────────────────────────────────┐
   │ Python Transformation Logic                            │
   │                                                        │
   │ For each row:                                         │
   │   • Lookup dimension keys                             │
   │     - dim_party_key = lookup_party(party_id)          │
   │     - dim_tax_type_key = lookup_tax_type(code)        │
   │     - dim_period_key = lookup_period(period_id)       │
   │     - dim_date_key = convert_to_date_key(date)        │
   │                                                        │
   │   • Apply business logic                              │
   │     - Calculate derived fields                        │
   │     - Compute flags (is_late, is_credit, etc.)        │
   │     - Format data                                     │
   │                                                        │
   │   • Handle NULL values and defaults                   │
   │                                                        │
   │ Returns: Transformed list of tuples                   │
   └────────────────────────────────────────────────────────┘
                          ↓

3. LOAD PHASE
   ┌────────────────────────────────────────────────────────┐
   │ ClickHouse L3 Loading                                  │
   │                                                        │
   │ Step 1: Truncate target table (if full reload)        │
   │   TRUNCATE TABLE ta_dw.fact_filing                    │
   │                                                        │
   │ Step 2: Bulk insert in batches (1000 rows)           │
   │   INSERT INTO ta_dw.fact_filing VALUES (...)          │
   │   [Batch 1: rows 1-1000]                             │
   │   [Batch 2: rows 1001-2000]                          │
   │   ...                                                 │
   │                                                        │
   │ Step 3: Verify row count                              │
   │   SELECT COUNT(*) FROM ta_dw.fact_filing              │
   │                                                        │
   └────────────────────────────────────────────────────────┘
```

### Dimension Processing Flow (SCD Type 2)

```
┌─────────────────────────────────────────────────────────────┐
│              SLOWLY CHANGING DIMENSION (Type 2)             │
│                    (dim_party example)                      │
└─────────────────────────────────────────────────────────────┘

Current State (L3):
┌──────────────────────────────────────────────────────────┐
│ party_key │ party_id │ party_name │ risk │ is_current │ │
│           │          │            │rating│            │ │
├───────────┼──────────┼────────────┼──────┼────────────┤ │
│    101    │   21     │ John Doe   │ LOW  │     1      │ │
│    102    │   22     │ ABC Corp   │ HIGH │     1      │ │
└──────────────────────────────────────────────────────────┘

New Data from L2:
┌──────────────────────────────────────────────────────────┐
│ party_id │ party_name │ risk_rating │                   │
├──────────┼────────────┼─────────────┤                   │
│   21     │ John Doe   │   MEDIUM    │  ← Risk changed  │
│   22     │ ABC Corp   │   HIGH      │  ← No change     │
│   23     │ New Party  │   LOW       │  ← New party     │
└──────────────────────────────────────────────────────────┘

ETL Processing:
1. Detect changes by comparing L2 vs L3
2. For changed records (party_id=21):
   - Expire old record: is_current=0, valid_to=today
   - Insert new record: new party_key, is_current=1
3. For new records (party_id=23):
   - Insert with is_current=1
4. For unchanged records (party_id=22):
   - No action

Result (L3 after ETL):
┌────────────────────────────────────────────────────────────────┐
│ party_key │ party_id │ risk  │ is_current │ valid_from│valid_to│
├───────────┼──────────┼───────┼────────────┼───────────┼────────┤
│    101    │   21     │ LOW   │     0      │2024-01-01 │Today   │
│    102    │   22     │ HIGH  │     1      │2024-01-01 │NULL    │
│    103    │   21     │MEDIUM │     1      │Today      │NULL    │
│    104    │   23     │ LOW   │     1      │Today      │NULL    │
└────────────────────────────────────────────────────────────────┘
```

---

## Integration Architecture

### External System Integration Points

```
┌────────────────────────────────────────────────────────────────┐
│                    INTEGRATION ARCHITECTURE                     │
└────────────────────────────────────────────────────────────────┘

                    ┌──────────────────┐
                    │   Tax Admin      │
                    │   Application    │
                    │   (Operational)  │
                    └────────┬─────────┘
                             │
                             │ Writes data
                             ▼
                    ┌──────────────────┐
                    │   MySQL L2       │◄──── Direct JDBC
                    │   Operational DB │      Connection
                    └────────┬─────────┘
                             │
                             │ Batch ETL
                             │ (nightly)
                             ▼
                    ┌──────────────────┐
                    │   ETL Process    │
                    └────────┬─────────┘
                             │
                             │ Bulk load
                             ▼
                    ┌──────────────────┐
                    │  ClickHouse L3   │◄──── BI Tools
                    │  Analytical DW   │      (ODBC/JDBC)
                    └────────┬─────────┘
                             │
                             │ Queries
                             ▼
                    ┌──────────────────┐
                    │  Analytics &     │
                    │  Reporting Tools │
                    └──────────────────┘
```

### Integration Protocols

| Integration Point | Protocol | Purpose |
|-------------------|----------|---------|
| **ETL → MySQL L2** | JDBC (mysql-connector-python) | Extract operational data |
| **ETL → ClickHouse L3** | HTTP/Native (clickhouse-connect) | Load warehouse data |
| **BI Tools → ClickHouse** | ODBC/JDBC | Query analytics |
| **Monitoring → ETL** | Log files, exit codes | Track execution |
| **Scheduler → ETL** | Shell script invocation | Trigger execution |

---

## Security Architecture

### Security Layers

```
┌────────────────────────────────────────────────────────────┐
│                    SECURITY ARCHITECTURE                    │
└────────────────────────────────────────────────────────────┘

1. NETWORK SECURITY
   ┌──────────────────────────────────────────────────────┐
   │ • Firewall rules (port restrictions)                │
   │ • VPN access for remote connections                 │
   │ • Network segmentation (DMZ)                        │
   └──────────────────────────────────────────────────────┘

2. AUTHENTICATION & AUTHORIZATION
   ┌──────────────────────────────────────────────────────┐
   │ MySQL L2:                                            │
   │   • Database user: ta_user                          │
   │   • Permissions: SELECT only on required schemas    │
   │                                                      │
   │ ClickHouse L3:                                       │
   │   • Database user: etl_writer                       │
   │   • Permissions: INSERT, TRUNCATE, SELECT on ta_dw  │
   └──────────────────────────────────────────────────────┘

3. CREDENTIAL MANAGEMENT
   ┌──────────────────────────────────────────────────────┐
   │ • .env file with 600 permissions                    │
   │ • NOT committed to version control                  │
   │ • Different credentials per environment             │
   │ • Rotate passwords regularly                        │
   └──────────────────────────────────────────────────────┘

4. DATA SECURITY
   ┌──────────────────────────────────────────────────────┐
   │ • SSL/TLS connections (optional but recommended)    │
   │ • Data at rest encryption (database level)          │
   │ • Audit logging enabled                             │
   │ • No sensitive data in logs                         │
   └──────────────────────────────────────────────────────┘

5. APPLICATION SECURITY
   ┌──────────────────────────────────────────────────────┐
   │ • SQL injection prevention (parameterized queries)  │
   │ • Input validation                                   │
   │ • Error handling (no sensitive data in errors)      │
   │ • Secure file permissions (chmod 600 for .env)      │
   └──────────────────────────────────────────────────────┘
```

### Security Best Practices

1. **Principle of Least Privilege**: ETL uses read-only access to L2, write access only to L3
2. **Defense in Depth**: Multiple security layers (network, authentication, application)
3. **Secure by Default**: All sensitive configuration in environment variables
4. **Audit Trail**: All ETL executions logged with timestamps and results

---

## Performance Architecture

### Performance Characteristics

```
┌────────────────────────────────────────────────────────────┐
│                  PERFORMANCE METRICS                        │
├────────────────────────────────────────────────────────────┤
│                                                             │
│ Current Performance (with test dataset):                   │
│   • Total data volume: ~1,700 rows                        │
│   • ETL execution time: ~23 seconds                       │
│   • Loading speed: ~75 rows/second                        │
│   • Validation time: ~5 seconds                           │
│                                                             │
│ Scalability (projected for production):                   │
│   • Expected volume: 100K-1M rows                         │
│   • Estimated time: 20-40 minutes                         │
│   • Batch size tuning: 1000-5000 rows                     │
│   • Memory usage: <2GB                                     │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

### Performance Optimization Strategies

```
┌────────────────────────────────────────────────────────────┐
│              PERFORMANCE OPTIMIZATION                       │
└────────────────────────────────────────────────────────────┘

1. DATABASE LEVEL
   ┌──────────────────────────────────────────────────────┐
   │ MySQL L2:                                            │
   │   • Indexes on join columns (party_id, tax_type_id) │
   │   • Indexes on filter columns (date ranges)         │
   │   • Query optimization (EXPLAIN plans)              │
   │                                                      │
   │ ClickHouse L3:                                       │
   │   • MergeTree table engine                          │
   │   • ORDER BY on commonly queried columns            │
   │   • Partition by date (for large tables)            │
   │   • Materialized views for aggregations             │
   └──────────────────────────────────────────────────────┘

2. ETL LEVEL
   ┌──────────────────────────────────────────────────────┐
   │ • Batch processing (1000 rows per batch)            │
   │ • Efficient dimension lookups (in-memory cache)     │
   │ • Minimize data transformations                     │
   │ • Parallel execution of independent ETLs            │
   └──────────────────────────────────────────────────────┘

3. NETWORK LEVEL
   ┌──────────────────────────────────────────────────────┐
   │ • Run ETL close to databases (minimize latency)     │
   │ • Connection pooling                                 │
   │ • Compression enabled (ClickHouse native)           │
   └──────────────────────────────────────────────────────┘
```

### Bottleneck Analysis

```
Potential Bottlenecks              Mitigation
─────────────────────              ──────────
MySQL query performance         →  Add indexes, optimize queries
Network latency                 →  Co-locate ETL with databases
ClickHouse insert speed         →  Increase batch size, use async inserts
Python transformation logic     →  Optimize algorithms, use pandas for large datasets
Memory constraints              →  Stream processing, batch chunking
```

---

## Deployment Architecture

### Deployment Options

#### Option 1: Single Server Deployment

```
┌───────────────────────────────────────────────────┐
│              Single Server Deployment              │
│                                                    │
│  ┌──────────────────────────────────────────────┐ │
│  │           Application Server                 │ │
│  │                                              │ │
│  │  ┌────────────┐  ┌────────────┐            │ │
│  │  │  MySQL L2  │  │ ClickHouse │            │ │
│  │  │            │  │     L3     │            │ │
│  │  └────────────┘  └────────────┘            │ │
│  │         ▲               ▲                   │ │
│  │         │               │                   │ │
│  │         └───────┬───────┘                   │ │
│  │                 │                           │ │
│  │         ┌───────▼───────┐                   │ │
│  │         │   ETL Process │                   │ │
│  │         │   (Python)    │                   │ │
│  │         └───────────────┘                   │ │
│  └──────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────┘

Pros: Simple setup, low cost
Cons: Single point of failure, limited scalability
Use for: Development, testing, small deployments
```

#### Option 2: Distributed Deployment (Recommended)

```
┌─────────────────────────────────────────────────────────────┐
│              Distributed Deployment                          │
│                                                              │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │  MySQL Server   │    │ ClickHouse Srv  │                │
│  │                 │    │                 │                │
│  │  ┌───────────┐  │    │  ┌───────────┐  │                │
│  │  │ MySQL L2  │  │    │  │ ClickHouse│  │                │
│  │  │ Database  │  │    │  │     L3    │  │                │
│  │  └───────────┘  │    │  └───────────┘  │                │
│  └────────┬────────┘    └────────┬────────┘                │
│           │                      │                          │
│           │   JDBC/HTTP          │   HTTP                   │
│           │                      │                          │
│  ┌────────▼──────────────────────▼────────┐                │
│  │         ETL Application Server          │                │
│  │                                         │                │
│  │  ┌───────────────────────────────────┐ │                │
│  │  │        ETL Process                │ │                │
│  │  │        (Python + Cron/Airflow)    │ │                │
│  │  └───────────────────────────────────┘ │                │
│  └─────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────┘

Pros: Isolated resources, better performance, scalable
Cons: More complex setup, higher cost
Use for: Production deployments
```

#### Option 3: Cloud Deployment

```
┌─────────────────────────────────────────────────────────────┐
│                  Cloud Deployment (AWS/GCP/Azure)           │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   RDS/Cloud  │    │   Kubernetes │    │  ClickHouse  │ │
│  │   MySQL L2   │    │     Pod      │    │    Cloud     │ │
│  │              │◄───┤  ETL Process │───►│     L3       │ │
│  │              │    │              │    │              │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│                             │                               │
│                             │                               │
│                      ┌──────▼──────┐                        │
│                      │  Cloud      │                        │
│                      │  Scheduler  │                        │
│                      │(Airflow/ECS)│                        │
│                      └─────────────┘                        │
└─────────────────────────────────────────────────────────────┘

Pros: Managed services, auto-scaling, high availability
Cons: Vendor lock-in, ongoing costs
Use for: Enterprise production deployments
```

### Deployment Checklist

- [ ] Python 3.9+ installed
- [ ] MySQL 9.0+ accessible with credentials
- [ ] ClickHouse 24.11+ accessible with credentials
- [ ] Virtual environment created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` file configured with database credentials
- [ ] Database connections tested (`python scripts/test_connections.py`)
- [ ] Initial ETL executed successfully (`./scripts/run_full_etl.sh`)
- [ ] Validation passed (`./scripts/run_validation.sh`)
- [ ] Scheduled execution configured (cron/Airflow)
- [ ] Monitoring and alerting configured
- [ ] Documentation reviewed by operations team

---

## Scalability Considerations

### Horizontal Scaling

- **Independent ETL scripts**: Can run different ETL scripts on different servers
- **Parallel execution**: Dimensions and independent facts can run in parallel
- **Load balancing**: Not applicable (batch processing, not real-time)

### Vertical Scaling

- **Memory**: Increase for larger batch sizes
- **CPU**: More cores for parallel processing
- **Disk I/O**: SSD for faster database access

### Data Volume Projections

| Data Volume | Execution Time | Recommended Batch Size | Memory Required |
|-------------|----------------|------------------------|-----------------|
| 10K rows | 2-5 minutes | 1,000 | 1GB |
| 100K rows | 15-30 minutes | 2,000 | 2GB |
| 1M rows | 2-3 hours | 5,000 | 4GB |
| 10M rows | 20-30 hours | 10,000 | 8GB |

**Note**: For very large volumes (>1M rows), consider incremental ETL instead of full reload.

---

## Monitoring and Observability

### Monitoring Points

```
┌────────────────────────────────────────────────────────────┐
│                   MONITORING ARCHITECTURE                   │
└────────────────────────────────────────────────────────────┘

1. ETL Execution Monitoring
   • Start/end timestamps
   • Execution duration
   • Success/failure status
   • Rows processed per table
   • Error messages and stack traces

2. Database Monitoring
   • MySQL connection pool status
   • ClickHouse connection status
   • Query execution times
   • Table row counts
   • Disk space usage

3. Validation Monitoring
   • Validation check results (157 checks)
   • Failed check details
   • Data quality metrics
   • Trend analysis

4. System Resource Monitoring
   • CPU usage
   • Memory usage
   • Disk I/O
   • Network throughput
```

### Log Files

```
logs/
├── etl-20251106-020000.log      # Full ETL execution log
├── etl-20251107-020000.log      # Daily logs
└── validation-20251106.log       # Validation results
```

---

## Disaster Recovery

### Backup Strategy

- **L2 Database**: Handled by operational system (not ETL responsibility)
- **L3 Database**: ClickHouse snapshots + ETL re-run capability
- **ETL Code**: Version control (Git)
- **Configuration**: Secure backup of `.env` files

### Recovery Procedures

1. **Corrupted L3 Data**: Truncate tables and re-run ETL
2. **Failed ETL**: Check logs, fix issue, re-run specific script
3. **Database Unavailable**: Wait for recovery, then resume
4. **Data Quality Issues**: Run validation, investigate, fix source data, reload

---

## Future Architecture Enhancements

### Potential Improvements

1. **Incremental ETL**: Change from full reload to delta processing
2. **Real-time Streaming**: Use Kafka for near-real-time updates
3. **Data Lake Integration**: Archive historical data to S3/HDFS
4. **Machine Learning**: Add predictive analytics layer
5. **API Layer**: REST API for programmatic data access
6. **Metadata Management**: Centralized data catalog
7. **Data Lineage**: Track data flow and transformations
8. **Advanced Monitoring**: Grafana dashboards, Prometheus metrics

---

## Conclusion

The TA-RDM ETL architecture provides a **robust, scalable, and maintainable** solution for transforming operational tax data into an analytical data warehouse. The architecture follows industry best practices and is designed for **extensibility** and **operational excellence**.

### Key Architectural Strengths

✅ **Modularity**: Independent, reusable components
✅ **Simplicity**: Clear separation of concerns, easy to understand
✅ **Reliability**: Comprehensive validation, error handling
✅ **Performance**: Optimized for analytical workloads
✅ **Security**: Multiple security layers, principle of least privilege
✅ **Maintainability**: Well-documented, standardized patterns

---

**Document Version**: 1.0.0
**Last Updated**: 2025-11-06
**Maintained By**: Development Team
