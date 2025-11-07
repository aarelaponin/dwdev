# TA-RDM Source Ingestion - Quick Start Guide

Get up and running with the TA-RDM Source Ingestion package in 15 minutes.

## Overview

This guide will walk you through:
1. Installing the package and dependencies
2. Configuring database connections
3. Setting up the configuration database
4. Loading sample mappings
5. Running your first ingestion

## Prerequisites

- Python 3.9 or higher
- MySQL 8.0+ (for L2 canonical, staging, and config databases)
- SQL Server (for RAMIS source - if applicable)
- Network access to all required databases

## Step 1: Installation (3 minutes)

### Clone and Setup

```bash
# Navigate to the source ingestion package directory
cd ta-rdm-source-ingestion

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Verify Installation

```bash
# Check Python version
python --version  # Should be 3.9+

# Test imports
python -c "import mysql.connector; import pymssql; print('✓ Dependencies OK')"
```

## Step 2: Configuration (5 minutes)

### Create Environment File

```bash
# Copy template
cp .env.example .env

# Edit with your database credentials
nano .env  # or use your preferred editor
```

### Configure Database Connections

Edit `.env` with your actual database details:

```bash
# L2 Canonical Database (MySQL)
L2_DB_HOST=localhost
L2_DB_PORT=3308
L2_DB_USER=ta_user
L2_DB_PASSWORD=your_password
L2_DB_DATABASE=ta_rdm_l2

# Configuration Database (MySQL - same instance as L2)
CONFIG_DB_HOST=localhost
CONFIG_DB_PORT=3308
CONFIG_DB_USER=ta_user
CONFIG_DB_PASSWORD=your_password
CONFIG_DB_DATABASE=ta_rdm_config

# Staging Database (MySQL)
STAGING_DB_HOST=localhost
STAGING_DB_PORT=3308
STAGING_DB_USER=ta_user
STAGING_DB_PASSWORD=your_password
STAGING_DB_DATABASE=ta_rdm_staging

# RAMIS Source Database (SQL Server)
RAMIS_DB_HOST=ramis-server
RAMIS_DB_PORT=1433
RAMIS_DB_USER=ramis_reader
RAMIS_DB_PASSWORD=your_password
RAMIS_DB_DATABASE=RAMIS

# ETL Configuration
ETL_BATCH_SIZE=10000
ETL_DRY_RUN=false
ETL_ROLLBACK_ON_ERROR=true

# Logging
LOG_LEVEL=INFO
LOG_FILE_PATH=logs/ingestion.log
LOG_COLORED_OUTPUT=true
```

### Test Connections

```bash
# Test database connectivity
python scripts/test_connections.py

# Expected output:
# ✓ L2 database connection successful
# ✓ Config database connection successful
# ✓ Staging database connection successful
# ✓ RAMIS database connection successful
```

## Step 3: Setup Configuration Database (3 minutes)

### Create Schema

```bash
# Initialize configuration database
python scripts/setup_config_db.py

# Expected output:
# ================================================================================
# TA-RDM Configuration Database Setup
# ================================================================================
# Connected to: localhost:3308/ta_rdm_config
# Creating tables...
#   ✓ source_systems
#   ✓ table_mappings
#   ✓ column_mappings
#   ...
# ✓ Configuration database setup complete
```

### Verify Schema

```bash
# Connect to MySQL and verify
mysql -h localhost -P 3308 -u ta_user -p ta_rdm_config

# Run verification query
mysql> SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = 'ta_rdm_config';

# Expected tables:
# source_systems
# table_mappings
# column_mappings
# lookup_mappings
# data_quality_rules
# staging_tables
# table_dependencies
# etl_execution_log
# data_quality_log
```

## Step 4: Load Sample Mappings (2 minutes)

### Import RAMIS Mappings

```bash
# Import example RAMIS mappings
python scripts/import_mappings.py \
    --config metadata/mappings/ramis_example.yaml

# Expected output:
# ================================================================================
# Import Mappings from YAML
# ================================================================================
# File: metadata/mappings/ramis_example.yaml
#
# Importing source system: RAMIS...
#   ✓ Source system ID: 1
#
# Importing mappings...
#   [1/5] RAMIS_TAXPAYER_TO_PARTY...
#     ✓ Table mapping ID: 1
#     ✓ Column mappings: 15
#     ✓ Lookup mappings: 3
#     ✓ DQ rules: 5
#   [2/5] RAMIS_ADDRESS_TO_ADDRESS...
#     ...
# ✓ Import complete
# Total: 5 mappings, 75 column mappings, 12 lookup mappings, 25 DQ rules
```

### Verify Mappings

```bash
# List available mappings
python scripts/run_ingestion.py --list-mappings --source RAMIS

# Expected output:
# ================================================================================
# Available Mappings
# ================================================================================
# Source System: RAMIS
# Total: 5
# --------------------------------------------------------------------------------
#
# ID: 1
# Code: RAMIS_TAXPAYER_TO_PARTY
# Name: RAMIS Taxpayer to Party
# Source: dbo.TAXPAYER
# Target: party.party
# Strategy: FULL
# Merge: UPSERT
# Columns: 15
# DQ Rules: 5
# ...
```

## Step 5: Run Your First Ingestion (2 minutes)

### Option A: Dry Run (Safe Preview)

```bash
# Run in dry-run mode (no database commits)
python scripts/run_ingestion.py \
    --mapping RAMIS_TAXPAYER_TO_PARTY \
    --dry-run

# Expected output:
# ================================================================================
# TA-RDM Source Ingestion Pipeline
# ================================================================================
# Started: 2025-11-07 14:30:15
# Batch size: 10000
# Dry run: True
# ================================================================================
#
# Connecting to databases...
#   ✓ Connected to source database
#   ✓ Connected to staging database
#   ✓ Connected to canonical database
#
# ================================================================================
# Executing mapping: RAMIS_TAXPAYER_TO_PARTY (ID: 1)
# Source: dbo.TAXPAYER
# Target: party.party
# ================================================================================
#
# PHASE 1: EXTRACT
# --------------------------------------------------------------------------------
# Extracted 10,000 rows from source
# --------------------------------------------------------------------------------
#
# PHASE 2: TRANSFORM
# --------------------------------------------------------------------------------
# Transformed 10,000 rows
# --------------------------------------------------------------------------------
#
# PHASE 3: VALIDATE
# --------------------------------------------------------------------------------
# Validated 10,000 rows
#   Passed: 9,950
#   Rejected: 50
#   Violations: 50
# --------------------------------------------------------------------------------
#
# PHASE 4: LOAD
# --------------------------------------------------------------------------------
# DRY RUN mode - skipping actual load
# --------------------------------------------------------------------------------
#
# ✓ Mapping completed successfully
```

### Option B: Full Execution

```bash
# Run actual ingestion (with database commits)
python scripts/run_ingestion.py \
    --mapping RAMIS_TAXPAYER_TO_PARTY

# This will extract, transform, validate, and load data to:
# - staging.stg_party (staging table)
# - party.party (canonical table)
```

### Option C: Full Source System

```bash
# Run all mappings for RAMIS source system
python scripts/run_ingestion.py --source RAMIS

# This will execute all 5 RAMIS mappings in dependency order
```

## Next Steps

Now that you have the basics working:

### 1. Explore the Data

```bash
# Check loaded data in canonical database
mysql -h localhost -P 3308 -u ta_user -p ta_rdm_l2

mysql> SELECT COUNT(*) FROM party.party;
mysql> SELECT * FROM party.party LIMIT 10;
```

### 2. Review Execution Logs

```bash
# Check execution history
mysql> USE ta_rdm_config;
mysql> SELECT * FROM etl_execution_log
       ORDER BY start_time DESC LIMIT 10;

# Check data quality violations
mysql> SELECT * FROM data_quality_log
       ORDER BY violation_time DESC LIMIT 20;
```

### 3. View Logs

```bash
# Check detailed logs
cat logs/ingestion.log

# Or tail for live monitoring
tail -f logs/ingestion.log
```

### 4. Create Custom Mappings

See the [User Guide](USER-GUIDE.md) for:
- Creating new source system configurations
- Defining table and column mappings
- Setting up data quality rules
- Configuring dependencies
- Scheduling with cron/Airflow

## Common Commands Reference

```bash
# List all mappings for a source
python scripts/run_ingestion.py --list-mappings --source RAMIS

# Run specific mapping by ID
python scripts/run_ingestion.py --mapping-id 1

# Run with custom batch size
python scripts/run_ingestion.py --source RAMIS --batch-size 5000

# Dry run with specific mapping
python scripts/run_ingestion.py --mapping RAMIS_TAXPAYER_TO_PARTY --dry-run

# Continue on error (don't stop if one mapping fails)
python scripts/run_ingestion.py --source RAMIS --continue-on-error
```

## Troubleshooting

### Connection Failed

```bash
# Error: Can't connect to MySQL server
# Solution: Check database is running and credentials are correct
mysql -h localhost -P 3308 -u ta_user -p

# Error: Can't connect to SQL Server
# Solution: Verify SQL Server is accessible
# Test with: telnet ramis-server 1433
```

### Import Errors

```bash
# Error: No module named 'mysql'
# Solution: Reinstall dependencies
pip install -r requirements.txt

# Error: ModuleNotFoundError
# Solution: Ensure you're in the correct directory and venv is activated
pwd  # Should be in ta-rdm-source-ingestion
which python  # Should point to venv/bin/python
```

### No Data Extracted

```bash
# Check source table exists
python -c "
from utils.db_utils import DatabaseConnection, DatabaseType
from config.database_config import RAMIS_DB_CONFIG
db = DatabaseConnection(DatabaseType.SQL_SERVER, RAMIS_DB_CONFIG)
db.connect()
result = db.fetch_one('SELECT COUNT(*) as cnt FROM dbo.TAXPAYER')
print(f'Row count: {result}')
"
```

## Support

For additional help:
- Review [User Guide](USER-GUIDE.md) for detailed documentation
- Check [Architecture](ARCHITECTURE.md) for system design
- See [Troubleshooting](../ta-rdm-etl/docs/TROUBLESHOOTING.md) for common issues
- Contact: Development Team

---

**Congratulations!** You've completed the quick start guide. The TA-RDM Source Ingestion package is now ready for production use.

**Last Updated**: 2025-11-07
