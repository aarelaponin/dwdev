# TA-RDM ETL Execution Guide

Comprehensive guide for running the TA-RDM ETL pipeline.

## Overview

The TA-RDM ETL pipeline extracts data from MySQL L2 (operational database) and loads it into ClickHouse L3 (data warehouse) using a dimensional data model.

**Pipeline Components:**
- **4 Dimension ETLs**: Load dimension tables (party, tax type, tax period, time)
- **11 Base Fact ETLs**: Load transactional and analytical facts
- **1 Derived Fact ETL**: Compute aggregate taxpayer activity metrics
- **1 Validation Script**: Verify data quality and completeness

## Execution Methods

### Method 1: Full Pipeline (Recommended)

Run all ETL scripts in correct dependency order using the automation script:

```bash
# Activate virtual environment
source venv/bin/activate

# Run full pipeline
./scripts/run_full_etl.sh
```

**What it does:**
1. Validates environment configuration (.env file exists)
2. Runs all ETL scripts in 5 phases (dimensions → base facts → compliance facts → analytical facts → validation)
3. Stops on first error with detailed error message
4. Reports success/failure for each ETL step
5. Runs comprehensive validation at the end
6. Displays execution summary and next steps

**Expected output:**
```
========================================
TA-RDM Full ETL Pipeline
Started: 2025-11-06 10:30:00
========================================

=== Phase 1: Loading Dimensions ===
----------------------------------------
Running: Time Dimension
Script: etl/generate_dim_time.py
----------------------------------------
✓ Success: Time Dimension

... (continues for all phases)

=== Phase 5: Running Validation ===
✓ Validation passed - All checks successful

========================================
ETL Pipeline Completed Successfully
Finished: 2025-11-06 10:35:23
========================================
```

### Method 2: Individual Scripts

Run ETL scripts individually when you need to:
- Re-run a specific ETL that failed
- Load only certain fact tables
- Test a single ETL script
- Debug issues with specific domains

**Always activate virtual environment first:**
```bash
source venv/bin/activate
```

#### Phase 1: Dimensions (Run First)

Dimensions must be loaded before facts because facts reference dimension keys.

```bash
# 1. Time Dimension (2020-2030 calendar)
python etl/generate_dim_time.py

# 2. Party Dimension (taxpayers)
python etl/l2_to_l3_party.py

# 3. Tax Type Dimension (VAT, CIT, PIT, etc.)
python etl/l2_to_l3_tax_type.py

# 4. Tax Period Dimension (monthly/quarterly/annual periods)
python etl/l2_to_l3_tax_period.py
```

#### Phase 2: Base Facts

Core transactional facts representing business events.

```bash
# Registration facts (tax account registrations)
python etl/l2_to_l3_fact_registration.py

# Filing facts (tax return submissions)
python etl/l2_to_l3_fact_filing.py

# Assessment facts (tax liability determinations)
python etl/l2_to_l3_fact_assessment.py

# Payment facts (payment transactions)
python etl/l2_to_l3_fact_payment.py

# Account balance facts (balance snapshots)
python etl/l2_to_l3_fact_account_balance.py
```

#### Phase 3: Compliance Facts

Facts related to compliance and enforcement activities.

```bash
# Collection facts (enforcement and recovery)
python etl/l2_to_l3_fact_collection.py

# Refund facts (refund transactions)
python etl/l2_to_l3_fact_refund.py

# Audit facts (audit cases and findings)
python etl/l2_to_l3_fact_audit.py

# Objection facts (disputes and appeals)
python etl/l2_to_l3_fact_objection.py
```

#### Phase 4: Analytical Facts

Derived and analytical fact tables.

```bash
# Risk assessment facts (taxpayer risk profiles)
python etl/l2_to_l3_fact_risk_assessment.py

# Taxpayer activity facts (aggregate activity metrics)
python etl/l2_to_l3_fact_taxpayer_activity.py
```

#### Validation Only

Run validation without loading data:

```bash
python etl/validate_etl.py
```

### Method 3: Selective Loading

Load specific domains or fact tables as needed:

```bash
# Load only dimensions
python etl/generate_dim_time.py
python etl/l2_to_l3_party.py
python etl/l2_to_l3_tax_type.py
python etl/l2_to_l3_tax_period.py

# Load only filing and assessment domains
python etl/l2_to_l3_fact_filing.py
python etl/l2_to_l3_fact_assessment.py

# Validate specific domain
python etl/validate_etl.py
```

## Execution Order and Dependencies

### Dependency Graph

```
Phase 1: Dimensions (independent, can run in any order)
├── generate_dim_time.py
├── l2_to_l3_party.py
├── l2_to_l3_tax_type.py
└── l2_to_l3_tax_period.py

Phase 2: Base Facts (depend on Phase 1 dimensions)
├── l2_to_l3_fact_registration.py (depends on: party, tax_type, tax_period, time)
├── l2_to_l3_fact_filing.py (depends on: party, tax_type, tax_period, time)
├── l2_to_l3_fact_assessment.py (depends on: party, tax_type, tax_period, time)
├── l2_to_l3_fact_payment.py (depends on: party, tax_type, time)
└── l2_to_l3_fact_account_balance.py (depends on: party, tax_type, time)

Phase 3: Compliance Facts (depend on Phase 1 dimensions)
├── l2_to_l3_fact_collection.py (depends on: party, tax_type, time)
├── l2_to_l3_fact_refund.py (depends on: party, tax_type, time)
├── l2_to_l3_fact_audit.py (depends on: party, tax_type, time)
└── l2_to_l3_fact_objection.py (depends on: party, tax_type, time)

Phase 4: Analytical Facts
├── l2_to_l3_fact_risk_assessment.py (depends on: party, time)
└── l2_to_l3_fact_taxpayer_activity.py (depends on: ALL Phase 2-3 facts!)

Phase 5: Validation (depends on all previous phases)
└── validate_etl.py
```

**Critical Dependencies:**
- All fact tables depend on dimensions being loaded first
- `fact_taxpayer_activity` aggregates from other fact tables, so it must run LAST
- Validation should run after all data is loaded

## Load Strategies

### Full Reload (Current Implementation)

All ETL scripts use a **full reload** strategy:

1. TRUNCATE target L3 table
2. Extract all data from L2
3. Transform and enrich
4. Bulk INSERT into L3

**Characteristics:**
- Simple and reliable
- Ensures data consistency
- No incremental complexity
- Suitable for medium datasets
- Takes 3-5 minutes for full pipeline

**Use when:**
- Running nightly batch loads
- Dataset size < 10M records per table
- Data consistency is critical
- Simplicity is preferred

### Incremental Load (Future Enhancement)

For larger datasets, you may want to implement incremental loading:

```python
# Pseudo-code for incremental pattern
last_load_date = get_last_load_date()
new_records = extract_where(f"modified_date > '{last_load_date}'")
upsert_to_l3(new_records)
```

**Note:** Current implementation does not support incremental loads. Modify ETL scripts if needed.

## Configuration Options

### Environment Variables

Configure ETL behavior via `.env` file:

```env
# Database connections
DB_HOST=your-mysql-host
DB_PORT=3308
CH_HOST=your-clickhouse-host
CH_PORT=8123

# ETL behavior
ETL_BATCH_SIZE=1000          # Records per batch insert
ETL_AUTO_VALIDATE=true       # Run validation after ETL
LOG_LEVEL=INFO               # DEBUG, INFO, WARNING, ERROR
```

### Command-Line Options

Some scripts support command-line arguments:

```bash
# Example: Generate time dimension for specific years
python etl/generate_dim_time.py --start-year 2020 --end-year 2030

# Example: Run validation in verbose mode
LOG_LEVEL=DEBUG python etl/validate_etl.py
```

## Monitoring and Logging

### Log Output

All ETL scripts produce structured log output:

```
2025-11-06 10:30:15 - INFO - Starting ETL: Party Dimension (L2 → L3)
2025-11-06 10:30:15 - INFO - MySQL L2 Connected: localhost:3308
2025-11-06 10:30:15 - INFO - ClickHouse L3 Connected: localhost:8123
2025-11-06 10:30:16 - INFO - Extracted 5 party records from L2
2025-11-06 10:30:16 - INFO - Transformed 5 records
2025-11-06 10:30:16 - INFO - Loaded 5 records to L3
2025-11-06 10:30:16 - INFO - ETL Completed Successfully
2025-11-06 10:30:16 - INFO - Duration: 1.23 seconds
```

### Log Levels

Control logging verbosity:

```bash
# Minimal output (errors only)
LOG_LEVEL=ERROR python etl/l2_to_l3_party.py

# Normal output (default)
LOG_LEVEL=INFO python etl/l2_to_l3_party.py

# Detailed output (for debugging)
LOG_LEVEL=DEBUG python etl/l2_to_l3_party.py
```

### Success Indicators

**Successful ETL run:**
```
ETL Completed Successfully
Duration: X.XX seconds
Loaded N records to L3
```

**Failed ETL run:**
```
ERROR - ETL Failed: [error message]
Traceback (most recent call last):
  ...
```

## Validation

### Running Validation

Validation should be run after data loading:

```bash
# Run comprehensive validation
python etl/validate_etl.py
```

### Validation Checks

The validation framework performs 157 checks across 4 categories:

1. **Row Count Validation** (15 checks)
   - Verifies L2 → L3 record counts match expected ratios
   - Example: fact_filing count should match L2 filing_return count

2. **Referential Integrity** (50 checks)
   - Validates all foreign keys reference existing dimension records
   - Example: Every fact_filing.dim_party_key exists in dim_party

3. **Mandatory Field Validation** (78 checks)
   - Ensures critical fields are not NULL
   - Example: fact_payment.payment_amount must not be NULL

4. **Dimension Key Lookup** (14 checks)
   - Verifies dimension lookups succeeded
   - Example: No -1 keys (lookup failures) in fact tables

### Interpreting Validation Results

**All checks pass:**
```
✓ VALIDATION PASSED - All checks successful (157/157)
Duration: 2.34 seconds
```

**Some checks fail:**
```
✗ VALIDATION FAILED
Passed: 155/157 checks
Failed: 2 checks

Failed Checks:
1. Row Count: fact_filing (expected: 69, actual: 70, ratio: 1.01)
2. Mandatory Field: fact_payment.payment_amount has 1 NULL values
```

**What to do when validation fails:**
1. Review failed check details
2. Investigate root cause in L2 data or ETL logic
3. Fix issues and re-run affected ETL scripts
4. Re-run validation to confirm fixes

## Troubleshooting

### Common Issues

#### Issue 1: Connection Errors

**Symptom:**
```
ERROR - MySQL connection failed: Can't connect to MySQL server
```

**Solutions:**
- Verify database is running: `mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p`
- Check `.env` file has correct credentials
- Test connections: `python scripts/test_connections.py`
- Verify network connectivity and firewall rules

#### Issue 2: Dimension Not Loaded

**Symptom:**
```
ERROR - No records found in dim_party
```

**Solutions:**
- Load dimensions first: `python etl/l2_to_l3_party.py`
- Check L2 source data exists
- Verify ETL completed successfully

#### Issue 3: Validation Failures

**Symptom:**
```
✗ Referential Integrity: fact_filing.dim_party_key has 5 orphaned records
```

**Solutions:**
- Re-run party dimension ETL first
- Re-run fact_filing ETL
- Check L2 data quality (orphaned references)

#### Issue 4: Performance Issues

**Symptom:**
ETL takes longer than expected (> 10 minutes)

**Solutions:**
- Check database indexes exist
- Reduce ETL_BATCH_SIZE in `.env`
- Check network latency between MySQL and ClickHouse
- Monitor database resource usage (CPU, memory)

### Error Messages

| Error Message | Cause | Solution |
|--------------|-------|----------|
| ModuleNotFoundError: No module named 'mysql' | Missing dependencies | `pip install -r requirements.txt` |
| Access denied for user | Wrong credentials | Check DB_USER and DB_PASSWORD in `.env` |
| Table doesn't exist | Schema not created | Run DDL scripts to create L2/L3 schemas |
| Duplicate entry | Primary key violation | TRUNCATE L3 table and re-run ETL |
| Connection timeout | Network issue | Check firewall, increase timeout in config |

## Performance Optimization

### Batch Size Tuning

Adjust batch size for optimal performance:

```env
# Small batches (safer, slower)
ETL_BATCH_SIZE=500

# Medium batches (balanced - default)
ETL_BATCH_SIZE=1000

# Large batches (faster, more memory)
ETL_BATCH_SIZE=5000
```

### Parallel Execution

Run independent ETL scripts in parallel:

```bash
# Run dimension ETLs in parallel
python etl/generate_dim_time.py &
python etl/l2_to_l3_party.py &
python etl/l2_to_l3_tax_type.py &
python etl/l2_to_l3_tax_period.py &
wait

# Run fact ETLs in parallel
python etl/l2_to_l3_fact_registration.py &
python etl/l2_to_l3_fact_filing.py &
python etl/l2_to_l3_fact_assessment.py &
wait
```

**Note:** Only parallelize scripts with no dependencies between them.

### Database Optimization

**MySQL L2 Indexes:**
```sql
-- Ensure indexes exist on frequently joined columns
CREATE INDEX idx_party_id ON party.party(party_id);
CREATE INDEX idx_filing_date ON filing_assessment.filing_return(filing_date);
```

**ClickHouse L3 Optimization:**
```sql
-- Ensure MergeTree table order is optimized
-- ORDER BY should include columns used in WHERE clauses
```

## Next Steps

After successful ETL execution:

1. **Verify Data in ClickHouse:**
   ```sql
   SELECT COUNT(*) FROM ta_dw.dim_party;
   SELECT COUNT(*) FROM ta_dw.fact_filing;
   ```

2. **Run Sample Queries:**
   ```sql
   SELECT
       p.party_name,
       COUNT(*) as filing_count,
       SUM(f.assessed_amount) as total_assessed
   FROM ta_dw.fact_filing f
   JOIN ta_dw.dim_party p ON f.dim_party_key = p.dim_party_key
   GROUP BY p.party_name;
   ```

3. **Schedule Regular Loads:**
   - See [SCHEDULING.md](SCHEDULING.md) for automation options

4. **Monitor Data Quality:**
   - Run validation daily
   - Set up alerts for validation failures

## See Also

- [INSTALLATION.md](INSTALLATION.md) - Setup and installation
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration options
- [ETL-Validation-Guide.md](ETL-Validation-Guide.md) - Validation details
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Detailed troubleshooting
- [SCHEDULING.md](SCHEDULING.md) - Automation and scheduling
