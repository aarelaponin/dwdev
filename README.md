# TA-RDM Data Warehouse - ETL Framework

**Tax Administration Reference Data Model - Complete ETL Solution**

A production-ready, metadata-driven ETL framework for the Sri Lanka Inland Revenue Department's Tax Administration Data Warehouse, implementing the full data pipeline from source systems through to analytical data marts.

---

## Overview

This repository contains two complementary ETL packages that together implement the complete **TA-RDM (Tax Administration Reference Data Model)** data warehouse pipeline:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    TA-RDM Data Warehouse Architecture                   │
└─────────────────────────────────────────────────────────────────────────┘

Source Systems          Layer 0           Layer 1         Layer 2              Layer 3
(RAMIS, etc.)          (RAW)          (STAGING)      (CANONICAL)       (DATA WAREHOUSE)
                                                        TA-RDM Model      Analytics Model
      │                  │                │                │                    │
      │                  │                │                │                    │
      ▼                  ▼                ▼                ▼                    ▼
┌──────────┐       ┌──────────┐     ┌──────────┐     ┌──────────┐       ┌──────────┐
│  RAMIS   │       │   RAW    │     │ STAGING  │     │  party   │       │  dim_    │
│  SQL     │──────▶│ Landing  │────▶│ Transform│────▶│  party   │──────▶│  party   │
│  Server  │       │  Zone    │     │  Layer   │     │          │       │          │
└──────────┘       └──────────┘     └──────────┘     └──────────┘       └──────────┘
      │                                                   │                    │
┌──────────┐                                         ┌──────────┐       ┌──────────┐
│  Other   │                                         │  account │       │  fact_   │
│  Source  │                                         │  address │       │  tax_    │
│  Systems │                                         │  ...     │       │  payment │
└──────────┘                                         └──────────┘       └──────────┘

      └────────────────────────────────────────┬──────────────────────────────┘
                                               │
                  ┌────────────────────────────┴────────────────────────────┐
                  │                                                          │
         ta-rdm-source-ingestion                                    ta-rdm-etl
         Source → L2 Canonical                                   L2 → L3 Warehouse
         (This ingests source data)                            (This builds analytics)
```

---

## Package Overview

### 1. **ta-rdm-source-ingestion** - Source to Canonical (L0 → L1 → L2)

**Purpose**: Ingest data from source tax administration systems into the TA-RDM Layer 2 canonical model.

**Data Flow**: `Source Systems → RAW (L0) → STAGING (L1) → CANONICAL (L2)`

**Key Features**:
- Extracts data from source systems (SQL Server, MySQL, etc.)
- Transforms using metadata-driven column mappings
- Validates data quality with configurable rules
- Loads to TA-RDM canonical model (party, account, address, etc.)
- Handles dependencies and table load order

**Use When**:
- Loading data from RAMIS or other source systems
- Creating/updating canonical party, account, address data
- Initial data ingestion into the data warehouse
- Incremental updates from operational systems

📁 **Location**: `ta-rdm-source-ingestion/`
📖 **Documentation**: [ta-rdm-source-ingestion/README.md](ta-rdm-source-ingestion/README.md)

---

### 2. **ta-rdm-etl** - Canonical to Data Warehouse (L2 → L3)

**Purpose**: Transform TA-RDM canonical model (L2) into analytical data warehouse (L3) with dimensions and facts.

**Data Flow**: `CANONICAL (L2) → DATA WAREHOUSE (L3 Dimensions & Facts)`

**Key Features**:
- Builds Type 2 slowly changing dimensions
- Creates fact tables for analytics
- Implements conformed dimensions
- Supports incremental updates with watermarking
- ClickHouse and MySQL support

**Use When**:
- Building analytical dimensions (dim_party, dim_date, etc.)
- Creating fact tables (fact_tax_payment, fact_assessment, etc.)
- Implementing slowly changing dimensions (SCD Type 2)
- Preparing data for BI tools and reporting

📁 **Location**: `ta-rdm-etl/`
📖 **Documentation**: [ta-rdm-etl/README.md](ta-rdm-etl/README.md)

---

## Complete Data Flow

### End-to-End Pipeline

```
STEP 1: Source Ingestion (ta-rdm-source-ingestion)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RAMIS.dbo.TAXPAYER (Source)
    ↓ EXTRACT
    ↓ TRANSFORM (UPPER, TRIM, Lookups)
    ↓ VALIDATE (NOT_NULL, LENGTH, PATTERN)
    ↓ LOAD
ta_rdm_l2.party.party (Canonical)

STEP 2: Warehouse ETL (ta-rdm-etl)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ta_rdm_l2.party.party (Canonical)
    ↓ EXTRACT
    ↓ TRANSFORM (SCD Type 2, Surrogate Keys)
    ↓ LOAD
ta_dw.dim_party (Data Warehouse Dimension)

ta_rdm_l2.payment.payment (Canonical)
    ↓ EXTRACT
    ↓ TRANSFORM (Lookup Dimension Keys)
    ↓ LOAD
ta_dw.fact_tax_payment (Data Warehouse Fact)
```

### Layer Architecture

| Layer | Name | Purpose | Package | Database |
|-------|------|---------|---------|----------|
| **L0** | RAW Landing | Untouched source data | ta-rdm-source-ingestion | MySQL/staging |
| **L1** | Staging | Intermediate transformations | ta-rdm-source-ingestion | MySQL/staging |
| **L2** | Canonical | TA-RDM normalized model | ta-rdm-source-ingestion | MySQL/ta_rdm_l2 |
| **L3** | Data Warehouse | Dimensions & Facts | ta-rdm-etl | ClickHouse/ta_dw |

---

## Quick Start - Complete System

### Prerequisites

- Python 3.9+
- MySQL 8.0+ (for L2 canonical, staging, config)
- ClickHouse 23.0+ (for L3 data warehouse) *or MySQL*
- SQL Server (for RAMIS source - optional)

### Step 1: Clone Repository

```bash
# Clone or navigate to the repository
cd ta-rdm-etl-framework/
```

### Step 2: Setup Source Ingestion Package

```bash
# Navigate to source ingestion package
cd ta-rdm-source-ingestion

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
nano .env  # Edit with your database credentials

# Setup configuration database
python scripts/setup_config_db.py

# Import RAMIS mappings
python scripts/import_mappings.py --config metadata/mappings/ramis_example.yaml

# Test connection
python scripts/test_connections.py
```

### Step 3: Run Source Ingestion (Source → L2)

```bash
# Still in ta-rdm-source-ingestion/

# Dry run first (preview without commits)
python scripts/run_ingestion.py --source RAMIS --dry-run

# Run actual ingestion
python scripts/run_ingestion.py --source RAMIS

# Expected output:
# ================================================================================
# Executing source system: RAMIS
# ================================================================================
# Found 5 mappings for RAMIS
#
# PHASE 1: EXTRACT - Extracted 10,000 rows from dbo.TAXPAYER
# PHASE 2: TRANSFORM - Transformed 10,000 rows
# PHASE 3: VALIDATE - Validated 10,000 rows (9,950 passed)
# PHASE 4: LOAD - Loaded 9,950 rows to party.party
#
# ✓ Mapping completed successfully
```

### Step 4: Setup Warehouse ETL Package

```bash
# Navigate to warehouse ETL package
cd ../ta-rdm-etl

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
nano .env  # Edit with your database credentials

# Setup ClickHouse schema (or MySQL)
clickhouse-client < docs/ta_dw_clickhouse_schema.sql
```

### Step 5: Run Warehouse ETL (L2 → L3)

```bash
# Still in ta-rdm-etl/

# Load dimensions first
python scripts/run_full_etl.sh dimensions

# Expected output:
# ================================================================================
# Loading Dimension: dim_party
# ================================================================================
# Extracted 9,950 rows from party.party
# Applied SCD Type 2 logic
# Loaded 9,950 rows (500 updates, 9,450 new records)
# ✓ Dimension load complete

# Then load facts
python scripts/run_full_etl.sh facts

# Expected output:
# ================================================================================
# Loading Fact: fact_tax_payment
# ================================================================================
# Extracted 50,000 rows from payment.payment
# Resolved dimension keys
# Loaded 50,000 rows
# ✓ Fact load complete
```

### Step 6: Verify Complete Pipeline

```bash
# Check L2 canonical data
mysql -h localhost -P 3308 -u ta_user -p ta_rdm_l2
mysql> SELECT COUNT(*) FROM party.party;
mysql> SELECT * FROM party.party LIMIT 10;

# Check L3 warehouse data
clickhouse-client
SELECT COUNT(*) FROM ta_dw.dim_party;
SELECT COUNT(*) FROM ta_dw.fact_tax_payment;

# Check data quality
mysql -h localhost -P 3308 -u ta_user -p ta_rdm_config
mysql> SELECT * FROM data_quality_log ORDER BY violation_time DESC LIMIT 20;
```

---

## Architecture Principles

### 1. Metadata-Driven

Both packages use metadata-driven architecture:
- **Source Ingestion**: Mappings stored in configuration database, not hardcoded
- **Warehouse ETL**: L2→L3 mappings defined in YAML, imported to config

### 2. Separation of Concerns

Each package has a clear responsibility:
- **Source Ingestion**: Operational data → Canonical model
- **Warehouse ETL**: Canonical model → Analytical model

### 3. Idempotent & Incremental

Both packages support:
- **Full Refresh**: Complete reload of data
- **Incremental**: Load only changed data using watermarks
- **Idempotent**: Can be re-run safely without duplicates

### 4. Data Quality First

Quality checks at every stage:
- **L1 Staging**: Source data validation
- **L2 Canonical**: Business rule validation
- **L3 Warehouse**: Referential integrity checks

### 5. Observability

Comprehensive logging and monitoring:
- Execution logs (etl_execution_log)
- Data quality violations (data_quality_log)
- Performance metrics
- File-based logs

---

## Common Workflows

### Daily Incremental Update

```bash
# 1. Ingest new/updated source data (L0 → L2)
cd ta-rdm-source-ingestion
python scripts/run_ingestion.py --source RAMIS

# 2. Update warehouse dimensions (L2 → L3 dimensions)
cd ../ta-rdm-etl
python scripts/run_full_etl.sh dimensions --incremental

# 3. Load new facts (L2 → L3 facts)
python scripts/run_full_etl.sh facts --incremental
```

### Full Refresh (Historical Load)

```bash
# 1. Full source ingestion
cd ta-rdm-source-ingestion
python scripts/run_ingestion.py --source RAMIS --batch-size 50000

# 2. Full warehouse build
cd ../ta-rdm-etl
python scripts/run_full_etl.sh all --full-refresh
```

### Add New Source System

```bash
# 1. Create YAML mapping configuration
cd ta-rdm-source-ingestion
nano metadata/mappings/new_source.yaml

# 2. Import mappings
python scripts/import_mappings.py --config metadata/mappings/new_source.yaml

# 3. Run ingestion
python scripts/run_ingestion.py --source NEW_SOURCE

# 4. Warehouse ETL automatically picks up new L2 data
cd ../ta-rdm-etl
python scripts/run_full_etl.sh all
```

### Add New Dimension/Fact

```bash
# 1. Create L2→L3 mapping
cd ta-rdm-etl
nano config/mappings/dim_new_dimension.yaml

# 2. Import mapping
python scripts/import_l2_l3_mapping.py --config config/mappings/dim_new_dimension.yaml

# 3. Run ETL
python scripts/run_full_etl.sh dim_new_dimension
```

---

## Package Comparison

| Feature | ta-rdm-source-ingestion | ta-rdm-etl |
|---------|------------------------|------------|
| **Purpose** | Source → Canonical | Canonical → Warehouse |
| **Input** | RAMIS, other sources | TA-RDM L2 (MySQL) |
| **Output** | TA-RDM L2 (MySQL) | TA-DW L3 (ClickHouse) |
| **Transformations** | Column mappings, lookups | SCD Type 2, dimension keys |
| **Data Quality** | 8 rule types | Referential integrity |
| **Batch Size** | 10,000 rows (default) | 100,000 rows (default) |
| **Dependencies** | Table load order | Dimension before facts |
| **Metadata** | Config database (MySQL) | YAML + Config DB |
| **CLI** | `run_ingestion.py` | `run_full_etl.sh` |

---

## Package Status

### ta-rdm-source-ingestion

**Status**: ✅ **Production Ready**

**Features**:
- ✅ Foundation utilities (database, logging)
- ✅ Metadata-driven configuration layer
- ✅ Complete ETL pipeline (extract, transform, validate, load)
- ✅ Orchestration with dependency management
- ✅ Comprehensive testing (145+ tests, ~85% coverage)
- ✅ Full documentation and examples

### ta-rdm-etl

**Status**: ✅ **Production Ready**

**Features**:
- ✅ L2 → L3 Pipeline complete
- ✅ SCD Type 2 implementation
- ✅ ClickHouse and MySQL support
- ✅ Incremental loads with watermarking
- ✅ Comprehensive documentation

---

## Project Structure

```
dwdev/
├── ta-rdm-source-ingestion/          # Source → L2 Canonical
│   ├── config/                        # Database configurations
│   ├── metadata/                      # Mapping configurations
│   │   ├── schema/                    # Config DB schema
│   │   └── mappings/                  # YAML mappings
│   ├── extractors/                    # Source extractors
│   ├── transformers/                  # Column transformations
│   ├── validators/                    # Data quality rules
│   ├── loaders/                       # L2 loaders
│   ├── orchestration/                 # Pipeline orchestration
│   ├── utils/                         # Shared utilities
│   ├── scripts/                       # CLI scripts
│   ├── tests/                         # Unit & integration tests
│   ├── docs/                          # Documentation
│   └── README.md                      # Package documentation
│
├── ta-rdm-etl/                        # L2 → L3 Warehouse
│   ├── config/                        # L2→L3 mappings
│   ├── etl/                           # ETL components
│   │   ├── extractors/                # L2 extractors
│   │   ├── transformers/              # SCD Type 2, etc.
│   │   └── loaders/                   # L3 loaders
│   ├── utils/                         # Database utilities
│   ├── scripts/                       # ETL execution scripts
│   ├── docs/                          # Documentation
│   └── README.md                      # Package documentation
│
├── ta-rdm-testdata/                   # Test data generator (optional)
│   └── generate_test_data.py
│
└── README.md                          # This file - overarching documentation
```

---

## Technical Stack

### Databases
- **MySQL 8.0+**: L2 canonical model, staging, configuration
- **ClickHouse 23.0+**: L3 data warehouse (columnar, analytical)
- **SQL Server**: RAMIS source system

### Python
- **Python 3.9+**
- **Key Libraries**:
  - `mysql-connector-python` - MySQL connectivity
  - `pymssql` - SQL Server connectivity
  - `clickhouse-driver` - ClickHouse connectivity
  - `pandas` - Data transformation
  - `pyyaml` - Configuration management
  - `pytest` - Testing framework

### Tools
- **Git**: Version control
- **Docker**: Optional containerization
- **Apache Airflow**: Optional scheduling (future)

---

## Testing

### ta-rdm-source-ingestion

```bash
cd ta-rdm-source-ingestion

# Run all tests with coverage
pytest --cov=. --cov-report=html

# Run specific tests
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests only

# View coverage
open htmlcov/index.html
```

**Test Statistics**:
- 145+ test cases
- ~85% code coverage
- Unit and integration tests

### ta-rdm-etl

```bash
cd ta-rdm-etl

# Run validation tests
python scripts/run_validation.sh

# Test database connections
python scripts/test_connections.py
```

---

## Documentation

### Quick References

**Source Ingestion**:
- [Quick Start Guide](ta-rdm-source-ingestion/docs/QUICKSTART.md) - 15-minute setup
- [User Guide](ta-rdm-source-ingestion/docs/USER-GUIDE.md) - Complete manual
- [Package README](ta-rdm-source-ingestion/README.md) - Technical overview

**Warehouse ETL**:
- [Architecture](ta-rdm-etl/docs/ARCHITECTURE.md)
- [Configuration Guide](ta-rdm-etl/docs/CONFIGURATION.md)
- [ETL Execution](ta-rdm-etl/docs/ETL-EXECUTION.md)
- [L2-L3 Mapping](ta-rdm-etl/docs/L2-L3-Mapping.md)

### API Documentation

Both packages include comprehensive docstrings:
```python
from metadata.catalog import MetadataCatalog
help(MetadataCatalog)

from orchestration.pipeline import IngestionPipeline
help(IngestionPipeline)
```

---

## Monitoring & Operations

### Execution Logs

Both packages log to configuration database:

```sql
-- Source ingestion executions
USE ta_rdm_config;
SELECT * FROM etl_execution_log
WHERE execution_status = 'FAILED'
ORDER BY start_time DESC;

-- Data quality violations
SELECT rule_violated, COUNT(*)
FROM data_quality_log
GROUP BY rule_violated;
```

### Log Files

```bash
# Source ingestion logs
tail -f ta-rdm-source-ingestion/logs/ingestion.log

# Warehouse ETL logs
tail -f ta-rdm-etl/logs/etl.log
```

### Performance Monitoring

```sql
-- Average execution time by mapping
SELECT
    mapping_id,
    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_seconds,
    COUNT(*) as execution_count
FROM etl_execution_log
WHERE execution_status = 'SUCCESS'
GROUP BY mapping_id;
```

---

## Deployment

### Development Environment

```bash
# Both packages can run on a single development machine
# with MySQL and ClickHouse installed locally

# ta-rdm-source-ingestion uses:
# - Source: RAMIS SQL Server (remote or local)
# - Target: MySQL L2 (local)

# ta-rdm-etl uses:
# - Source: MySQL L2 (local)
# - Target: ClickHouse L3 (local)
```

### Production Environment

Recommended deployment:

```
┌─────────────────────────────────────────────────────────┐
│  Production Environment                                 │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  App Server (ETL Execution)                             │
│  ├── ta-rdm-source-ingestion (Python)                   │
│  └── ta-rdm-etl (Python)                                │
│                                                          │
│  Database Server(s)                                      │
│  ├── MySQL (L2 Canonical, Staging, Config)             │
│  └── ClickHouse (L3 Data Warehouse)                     │
│                                                          │
│  Scheduler (Optional)                                    │
│  └── Apache Airflow / Cron                              │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### Scheduling

Example cron schedule:

```bash
# Daily source ingestion at 2 AM
0 2 * * * cd /path/to/ta-rdm-source-ingestion && \
    source venv/bin/activate && \
    python scripts/run_ingestion.py --source RAMIS

# Daily warehouse update at 4 AM (after ingestion completes)
0 4 * * * cd /path/to/ta-rdm-etl && \
    source venv/bin/activate && \
    ./scripts/run_full_etl.sh all --incremental
```

---

## Troubleshooting

### Common Issues

#### Issue: Connection Failed

```bash
# Test database connectivity
cd ta-rdm-source-ingestion
python scripts/test_connections.py

cd ../ta-rdm-etl
python scripts/test_connections.py
```

#### Issue: No Data in L3

```bash
# Check L2 data exists
mysql> USE ta_rdm_l2;
mysql> SELECT COUNT(*) FROM party.party;

# Check L2→L3 mapping is active
mysql> USE ta_rdm_config;
mysql> SELECT * FROM l2_l3_table_mappings WHERE is_active = 1;

# Re-run with full refresh
cd ta-rdm-etl
./scripts/run_full_etl.sh all --full-refresh
```

#### Issue: Data Quality Violations

```sql
-- Check violations
SELECT * FROM ta_rdm_config.data_quality_log
ORDER BY violation_time DESC LIMIT 100;

-- Fix source data or adjust rules
-- Then re-run ingestion
```

---

## Contributing

### Coding Standards

Both packages follow:
- PEP 8 style guidelines
- Type hints for function signatures
- Comprehensive docstrings
- Clear error messages
- Unit tests for new features

### Development Workflow

1. Create feature branch
2. Implement changes with tests
3. Run test suite
4. Update documentation
5. Submit for review

---

## Support

### Documentation
- Source Ingestion: [ta-rdm-source-ingestion/docs/](ta-rdm-source-ingestion/docs/)
- Warehouse ETL: [ta-rdm-etl/docs/](ta-rdm-etl/docs/)

### Contact
- Development Team
- Sri Lanka Inland Revenue Department

---

## License

Internal use for Sri Lanka Inland Revenue Department Tax Administration Data Warehouse.

---

## Summary

This TA-RDM Data Warehouse framework provides:

✅ **Complete Pipeline**: Source systems → L0 → L1 → L2 → L3 → Analytics
✅ **Two Specialized Packages**: Source ingestion + Warehouse ETL
✅ **Metadata-Driven**: Configurable without code changes
✅ **Production-Ready**: Comprehensive testing and error handling
✅ **Well-Documented**: Quick starts, user guides, API docs
✅ **Enterprise-Grade**: Logging, monitoring, data quality
✅ **Incremental & Full**: Support for both load strategies
✅ **Tested**: 145+ tests with ~85% coverage

**Start with**: [ta-rdm-source-ingestion Quick Start](ta-rdm-source-ingestion/docs/QUICKSTART.md)

---

**Last Updated**: 2025-11-07

**Overall Status**: ✅ **PRODUCTION READY**
