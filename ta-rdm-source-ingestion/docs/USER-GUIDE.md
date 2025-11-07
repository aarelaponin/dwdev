# TA-RDM Source Ingestion - User Guide

Complete guide for using the TA-RDM Source Ingestion package.

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Configuration](#configuration)
4. [Creating Mappings](#creating-mappings)
5. [Running Ingestion](#running-ingestion)
6. [Data Quality Rules](#data-quality-rules)
7. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
8. [Best Practices](#best-practices)
9. [API Reference](#api-reference)

---

## Introduction

The TA-RDM Source Ingestion package is a metadata-driven ETL framework that ingests data from source systems into the TA-RDM Layer 2 canonical model.

### Key Features

- **Metadata-Driven**: All mappings stored in configuration database
- **Multi-Source Support**: SQL Server, MySQL, and extensible to other sources
- **3-Layer Architecture**: RAW (L0) → STAGING (L1) → CANONICAL (L2)
- **Data Quality Framework**: Built-in validation rules
- **Dependency Management**: Automatic resolution of table load order
- **Transaction Management**: Automatic rollback on errors
- **Batch Processing**: Configurable batch sizes for efficiency

### When to Use This Package

Use this package to:
- Ingest data from source tax administration systems (RAMIS, etc.)
- Transform source data to TA-RDM canonical model
- Validate data quality before loading
- Track and audit all ETL executions
- Maintain metadata-driven, configurable ETL processes

---

## Architecture Overview

### 3-Layer Data Flow

```
Source System (RAMIS)
    ↓ EXTRACT
RAW Layer (L0) - Optional landing zone
    ↓ TRANSFORM
STAGING Layer (L1) - Intermediate transformations
    ↓ VALIDATE & LOAD
CANONICAL Layer (L2) - TA-RDM model
```

### ETL Pipeline Phases

Each mapping execution flows through 4 phases:

#### 1. EXTRACT
- Connects to source database
- Applies filter conditions
- Retrieves data in batches
- Supports full and incremental loads

#### 2. TRANSFORM
- Applies column mappings (metadata-driven)
- Executes SQL expressions (UPPER, TRIM, etc.)
- Resolves lookup values
- Supports custom transformation functions

#### 3. VALIDATE
- Applies data quality rules
- Validates NOT_NULL, LENGTH, PATTERN, RANGE, etc.
- Logs violations to data_quality_log
- Supports REJECT, LOG, or SKIP actions

#### 4. LOAD
- Loads to staging table (truncate-and-load)
- Loads to canonical table (UPSERT)
- Handles key-based merging
- Commits or rolls back on error

### Components

- **Extractors**: Pull data from source systems
- **Transformers**: Apply column mappings and transformations
- **Validators**: Enforce data quality rules
- **Loaders**: Write to staging and canonical layers
- **Orchestrator**: Coordinates end-to-end pipeline
- **Dependency Manager**: Resolves table load order

---

## Configuration

### Environment Variables

All configuration is managed through `.env` file:

```bash
# L2 Canonical Database
L2_DB_HOST=localhost
L2_DB_PORT=3308
L2_DB_USER=ta_user
L2_DB_PASSWORD=secret
L2_DB_DATABASE=ta_rdm_l2

# Configuration Database
CONFIG_DB_HOST=localhost
CONFIG_DB_PORT=3308
CONFIG_DB_USER=ta_user
CONFIG_DB_PASSWORD=secret
CONFIG_DB_DATABASE=ta_rdm_config

# Staging Database
STAGING_DB_HOST=localhost
STAGING_DB_PORT=3308
STAGING_DB_USER=ta_user
STAGING_DB_PASSWORD=secret
STAGING_DB_DATABASE=ta_rdm_staging

# RAMIS Source Database
RAMIS_DB_HOST=ramis-server
RAMIS_DB_PORT=1433
RAMIS_DB_USER=ramis_reader
RAMIS_DB_PASSWORD=secret
RAMIS_DB_DATABASE=RAMIS

# ETL Configuration
ETL_BATCH_SIZE=10000              # Rows per batch
ETL_DRY_RUN=false                 # Dry run mode
ETL_ROLLBACK_ON_ERROR=true        # Rollback on error
ETL_CONTINUE_ON_MAPPING_FAILURE=false

# Data Quality
DQ_VALIDATION_ENABLED=true
DQ_REJECT_ON_ERROR=true           # Reject rows with ERROR severity
DQ_REJECT_ON_WARNING=false        # Don't reject rows with WARNING
DQ_LOG_ALL_VIOLATIONS=true
DQ_MAX_VIOLATIONS_TO_LOG=1000

# Logging
LOG_LEVEL=INFO
LOG_FILE_PATH=logs/ingestion.log
LOG_COLORED_OUTPUT=true
LOG_ROTATION_SIZE_MB=50
LOG_RETENTION_DAYS=30
```

### Database Setup

```bash
# 1. Create configuration database schema
python scripts/setup_config_db.py

# 2. Verify schema
python scripts/setup_config_db.py --verify

# 3. Drop and recreate (use with caution!)
python scripts/setup_config_db.py --drop --yes-i-am-sure
```

---

## Creating Mappings

### YAML Configuration Format

Mappings are defined in YAML format and imported into the configuration database.

### Example: Complete Mapping Configuration

```yaml
# metadata/mappings/ramis_taxpayer.yaml

source_system:
  code: "RAMIS"
  name: "Revenue Administration Management Information System"
  description: "Core tax administration system"
  database_type: "SQL_SERVER"
  connection_string_key: "RAMIS_DB"
  is_active: true

table_mappings:
  - mapping_code: "RAMIS_TAXPAYER_TO_PARTY"
    mapping_name: "RAMIS Taxpayer to Party"
    description: "Map RAMIS taxpayers to TA-RDM party entity"

    # Source configuration
    source_schema: "dbo"
    source_table: "TAXPAYER"
    source_filter_condition: "WHERE is_active = 1 AND registration_date >= '2020-01-01'"

    # Target configuration
    target_schema: "party"
    target_table: "party"
    target_key_columns: ["party_id"]

    # Load strategy
    load_strategy: "FULL"          # or "INCREMENTAL"
    merge_strategy: "UPSERT"       # or "INSERT", "UPDATE"
    incremental_column: null       # e.g., "last_updated_date"
    incremental_value: null

    # Column mappings
    column_mappings:
      # Direct mapping
      - source_column: "taxpayer_id"
        target_column: "party_id"
        transformation_type: "DIRECT"
        data_type: "VARCHAR(50)"
        is_nullable: false
        is_key: true

      # Expression transformation
      - source_column: "taxpayer_name"
        target_column: "party_name"
        transformation_type: "EXPRESSION"
        transformation_logic: "UPPER(TRIM({taxpayer_name}))"
        data_type: "VARCHAR(255)"
        is_nullable: false

      # Lookup transformation
      - source_column: "taxpayer_type"
        target_column: "party_type"
        transformation_type: "LOOKUP"
        data_type: "VARCHAR(20)"
        is_nullable: false
        lookups:
          - source_value: "I"
            target_value: "INDIVIDUAL"
            description: "Individual taxpayer"
          - source_value: "C"
            target_value: "COMPANY"
            description: "Company taxpayer"
          - source_value: "P"
            target_value: "PARTNERSHIP"
            description: "Partnership"

      # Conditional transformation
      - source_column: "registration_date"
        target_column: "registration_date"
        transformation_type: "EXPRESSION"
        transformation_logic: "CAST({registration_date} AS DATE)"
        data_type: "DATE"
        is_nullable: true

      # Constant value
      - source_column: null
        target_column: "source_system"
        transformation_type: "CONSTANT"
        transformation_logic: "'RAMIS'"
        data_type: "VARCHAR(50)"
        default_value: "RAMIS"

    # Data quality rules
    data_quality_rules:
      - rule_code: "PARTY_ID_NOT_NULL"
        rule_name: "Party ID Not Null"
        rule_type: "NOT_NULL"
        column_name: "party_id"
        severity: "ERROR"
        is_active: true

      - rule_code: "PARTY_NAME_MIN_LENGTH"
        rule_name: "Party Name Minimum Length"
        rule_type: "LENGTH"
        column_name: "party_name"
        rule_parameters:
          min_length: 2
          max_length: 255
        severity: "WARNING"
        is_active: true

      - rule_code: "PARTY_TYPE_VALID_VALUES"
        rule_name: "Party Type Valid Values"
        rule_type: "IN_LIST"
        column_name: "party_type"
        rule_parameters:
          allowed_values:
            - "INDIVIDUAL"
            - "COMPANY"
            - "PARTNERSHIP"
        severity: "ERROR"
        is_active: true

      - rule_code: "EMAIL_FORMAT"
        rule_name: "Valid Email Format"
        rule_type: "PATTERN"
        column_name: "email"
        rule_parameters:
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        severity: "WARNING"
        is_active: true

    # Dependencies (this mapping depends on others)
    dependencies: []
```

### Import Mappings

```bash
# Import YAML configuration
python scripts/import_mappings.py \
    --config metadata/mappings/ramis_taxpayer.yaml

# Dry run (preview without importing)
python scripts/import_mappings.py \
    --config metadata/mappings/ramis_taxpayer.yaml \
    --dry-run

# Force update existing mappings
python scripts/import_mappings.py \
    --config metadata/mappings/ramis_taxpayer.yaml \
    --update
```

### Transformation Types

#### 1. DIRECT
Copy value as-is from source to target:
```yaml
- source_column: "taxpayer_id"
  target_column: "party_id"
  transformation_type: "DIRECT"
```

#### 2. EXPRESSION
Apply SQL expression:
```yaml
- source_column: "taxpayer_name"
  target_column: "party_name"
  transformation_type: "EXPRESSION"
  transformation_logic: "UPPER(TRIM({taxpayer_name}))"
```

Supported SQL functions:
- `UPPER({column})` - Convert to uppercase
- `LOWER({column})` - Convert to lowercase
- `TRIM({column})` - Remove leading/trailing spaces
- `COALESCE({column}, 'default')` - Provide default value
- `CAST({column} AS DATE)` - Type conversion
- `CONCAT({col1}, ' ', {col2})` - String concatenation
- `SUBSTRING({column}, 1, 10)` - Extract substring

#### 3. LOOKUP
Map source values to target values:
```yaml
- source_column: "taxpayer_type"
  target_column: "party_type"
  transformation_type: "LOOKUP"
  lookups:
    - {source_value: "I", target_value: "INDIVIDUAL"}
    - {source_value: "C", target_value: "COMPANY"}
```

#### 4. CONSTANT
Use constant value:
```yaml
- source_column: null
  target_column: "source_system"
  transformation_type: "CONSTANT"
  default_value: "RAMIS"
```

---

## Running Ingestion

### Command-Line Interface

#### Run All Mappings for a Source

```bash
python scripts/run_ingestion.py --source RAMIS
```

#### Run Specific Mapping by Code

```bash
python scripts/run_ingestion.py --mapping RAMIS_TAXPAYER_TO_PARTY
```

#### Run Specific Mapping by ID

```bash
python scripts/run_ingestion.py --mapping-id 1
```

#### Dry Run Mode

```bash
# Preview without committing to database
python scripts/run_ingestion.py --source RAMIS --dry-run
```

#### Custom Batch Size

```bash
# Process 5,000 rows per batch instead of default 10,000
python scripts/run_ingestion.py --source RAMIS --batch-size 5000
```

#### Continue on Error

```bash
# Don't stop if one mapping fails
python scripts/run_ingestion.py --source RAMIS --continue-on-error
```

#### List Available Mappings

```bash
# List all mappings for a source
python scripts/run_ingestion.py --list-mappings --source RAMIS

# List all mappings
python scripts/run_ingestion.py --list-mappings
```

### Programmatic Usage

```python
from utils.db_utils import DatabaseConnection, DatabaseType
from metadata.catalog import MetadataCatalog
from orchestration.pipeline import IngestionPipeline
from config.database_config import (
    RAMIS_DB_CONFIG, STAGING_DB_CONFIG, L2_DB_CONFIG
)

# Connect to databases
source_db = DatabaseConnection(DatabaseType.SQL_SERVER, RAMIS_DB_CONFIG)
staging_db = DatabaseConnection(DatabaseType.MYSQL, STAGING_DB_CONFIG)
canonical_db = DatabaseConnection(DatabaseType.MYSQL, L2_DB_CONFIG)

source_db.connect()
staging_db.connect()
canonical_db.connect()

# Initialize metadata catalog
catalog = MetadataCatalog()
catalog.connect()

# Create pipeline
pipeline = IngestionPipeline(
    source_db=source_db,
    staging_db=staging_db,
    canonical_db=canonical_db,
    catalog=catalog,
    batch_size=10000,
    dry_run=False
)

# Execute source system
stats = pipeline.execute_source_system('RAMIS')

print(f"Total mappings: {stats['total_mappings']}")
print(f"Successful: {stats['successful_mappings']}")
print(f"Failed: {stats['failed_mappings']}")
print(f"Rows loaded: {stats['total_rows_loaded']:,}")

# Cleanup
source_db.disconnect()
staging_db.disconnect()
canonical_db.disconnect()
catalog.disconnect()
```

---

## Data Quality Rules

### Supported Rule Types

#### 1. NOT_NULL
Ensure column has a value:
```yaml
- rule_type: "NOT_NULL"
  column_name: "party_id"
  severity: "ERROR"
```

#### 2. LENGTH
Validate string length:
```yaml
- rule_type: "LENGTH"
  column_name: "party_name"
  rule_parameters:
    min_length: 2
    max_length: 255
  severity: "WARNING"
```

#### 3. PATTERN
Validate against regex pattern:
```yaml
- rule_type: "PATTERN"
  column_name: "email"
  rule_parameters:
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
  severity: "WARNING"
```

#### 4. RANGE
Validate numeric range:
```yaml
- rule_type: "RANGE"
  column_name: "age"
  rule_parameters:
    min_value: 0
    max_value: 120
  severity: "ERROR"
```

#### 5. IN_LIST
Validate against allowed values:
```yaml
- rule_type: "IN_LIST"
  column_name: "party_type"
  rule_parameters:
    allowed_values: ["INDIVIDUAL", "COMPANY", "PARTNERSHIP"]
  severity: "ERROR"
```

#### 6. UNIQUE
Ensure uniqueness (within batch):
```yaml
- rule_type: "UNIQUE"
  column_name: "party_id"
  severity: "ERROR"
```

#### 7. REFERENCE
Validate foreign key reference:
```yaml
- rule_type: "REFERENCE"
  column_name: "country_code"
  rule_parameters:
    reference_table: "reference.country"
    reference_column: "country_code"
  severity: "ERROR"
```

#### 8. CUSTOM_SQL
Custom SQL validation:
```yaml
- rule_type: "CUSTOM_SQL"
  column_name: "tax_id"
  rule_parameters:
    sql: "SELECT COUNT(*) FROM party.party WHERE party_id = {party_id}"
    expected_value: 0
  severity: "WARNING"
```

### Severity Levels

- **INFO**: Informational, row is not rejected
- **WARNING**: Warning logged, row is not rejected (by default)
- **ERROR**: Row is rejected (by default)
- **CRITICAL**: Row is rejected and execution may stop

### Action Modes

- **REJECT**: Rows with ERROR or CRITICAL violations are rejected
- **LOG**: Violations are logged but rows are not rejected
- **SKIP**: Validation is performed but no action taken

---

## Monitoring and Troubleshooting

### Execution Logs

#### Check Execution History

```sql
USE ta_rdm_config;

-- Recent executions
SELECT
    execution_id,
    mapping_id,
    execution_status,
    start_time,
    end_time,
    rows_extracted,
    rows_loaded,
    rows_rejected
FROM etl_execution_log
ORDER BY start_time DESC
LIMIT 20;

-- Failed executions
SELECT * FROM etl_execution_log
WHERE execution_status = 'FAILED'
ORDER BY start_time DESC;

-- Execution duration
SELECT
    mapping_id,
    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_duration_sec,
    MAX(TIMESTAMPDIFF(SECOND, start_time, end_time)) as max_duration_sec
FROM etl_execution_log
WHERE execution_status = 'SUCCESS'
GROUP BY mapping_id;
```

#### Check Data Quality Violations

```sql
-- Recent violations
SELECT
    v.violation_time,
    v.rule_violated,
    v.column_name,
    v.error_message,
    v.action_taken
FROM data_quality_log v
ORDER BY v.violation_time DESC
LIMIT 100;

-- Violations by rule
SELECT
    rule_violated,
    COUNT(*) as violation_count
FROM data_quality_log
WHERE violation_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY rule_violated
ORDER BY violation_count DESC;

-- Violations by mapping
SELECT
    e.mapping_id,
    COUNT(v.violation_id) as violation_count
FROM data_quality_log v
JOIN etl_execution_log e ON v.execution_id = e.execution_id
GROUP BY e.mapping_id
ORDER BY violation_count DESC;
```

### Log Files

```bash
# View recent logs
tail -n 100 logs/ingestion.log

# Follow logs in real-time
tail -f logs/ingestion.log

# Search for errors
grep "ERROR" logs/ingestion.log

# Search for specific mapping
grep "RAMIS_TAXPAYER" logs/ingestion.log
```

### Common Issues

#### Issue: Connection Timeout

**Symptom**: `Connection timeout` or `Can't connect to database`

**Solution**:
```bash
# Test connection
python scripts/test_connections.py

# Check network connectivity
ping ramis-server
telnet ramis-server 1433

# Verify credentials
mysql -h localhost -P 3308 -u ta_user -p
```

#### Issue: No Rows Extracted

**Symptom**: `Extracted 0 rows from source`

**Solution**:
```bash
# Check filter condition in mapping
# Verify source table has data
# Check source database permissions
```

#### Issue: All Rows Rejected

**Symptom**: `Validated 0 rows (all rejected)`

**Solution**:
```sql
# Check data quality violations
SELECT * FROM data_quality_log
WHERE execution_id = [last_execution_id]
ORDER BY violation_time;

# Review and fix DQ rules
# Or temporarily disable strict rules
```

---

## Best Practices

### 1. Always Use Dry Run First

```bash
# Preview before running
python scripts/run_ingestion.py --source RAMIS --dry-run
```

### 2. Start with Small Batch Sizes

```bash
# Use smaller batches initially
python scripts/run_ingestion.py --source RAMIS --batch-size 1000
```

### 3. Monitor Execution Logs

```bash
# Regularly check execution history
tail -f logs/ingestion.log
```

### 4. Test Mappings Incrementally

```bash
# Test one mapping at a time
python scripts/run_ingestion.py --mapping RAMIS_TAXPAYER_TO_PARTY
```

### 5. Use Version Control for YAML Configs

```bash
# Track changes to mapping configurations
git add metadata/mappings/
git commit -m "Updated RAMIS taxpayer mapping"
```

### 6. Schedule Regular Ingestion

```bash
# Cron example (daily at 2 AM)
0 2 * * * cd /path/to/ta-rdm-source-ingestion && \
    source venv/bin/activate && \
    python scripts/run_ingestion.py --source RAMIS
```

---

## API Reference

See individual module documentation:
- [Database Utilities](../utils/db_utils.py)
- [Metadata Catalog](../metadata/catalog.py)
- [Ingestion Pipeline](../orchestration/pipeline.py)
- [Dependency Manager](../orchestration/dependency_manager.py)

---

## Support

For additional help:
- [Quick Start Guide](QUICKSTART.md)
- [Architecture Documentation](ARCHITECTURE.md)
- Contact: Development Team

---

**Last Updated**: 2025-11-07
