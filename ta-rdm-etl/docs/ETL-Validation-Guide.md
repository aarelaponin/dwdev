# ETL Validation Utility - User Guide

## Overview

The ETL Validation Utility is a comprehensive tool for verifying data consistency and integrity across the TA-RDM L2→L3 pipeline. It validates that data flows correctly from MySQL Layer 2 (operational database) to ClickHouse Layer 3 (data warehouse) without loss or corruption.

**Primary Purpose**: Compare **input** (L2 MySQL) vs **output** (L3 ClickHouse) to ensure ETL pipeline correctness.

---

## Quick Start

### Basic Usage

```bash
# Validate entire ETL pipeline
python etl/validate_etl.py

# Validate specific table
python etl/validate_etl.py --table dim_party

# Generate JSON report
python etl/validate_etl.py --format json --output validation.json

# Generate HTML report
python etl/validate_etl.py --format html --output validation.html
```

### Exit Codes

- **0**: All validations passed
- **1**: One or more validations failed

This makes it easy to use in automation/CI/CD:

```bash
python etl/validate_etl.py || echo "Validation failed!"
```

---

## Validation Categories

The utility runs 4 types of validation checks:

### 1. Row Count Validation ⭐ (PRIMARY)

**Purpose**: Verify no data loss during ETL by comparing L2 input vs L3 output row counts.

**What it checks**:
- Number of rows extracted from L2 MySQL source tables
- Number of rows loaded into L3 ClickHouse target tables
- Expected ratio (usually 1:1)
- Difference and percentage match

**Example Output**:
```
✓ Row Count: dim_party
    L2 Source:    5 rows (from party.party)
    L3 Target:    5 rows (in ta_dw.dim_party)
    Expected:     5 rows
    Difference:   +0
    Ratio:        100.00%
    Description:  Party dimension with individuals and enterprises
```

**Tables Validated**:
| L3 Table | L2 Source | Expected Ratio |
|----------|-----------|----------------|
| dim_party | party.party + party.individual + compliance_control.taxpayer_risk_profile | 1:1 |
| dim_tax_type | tax_framework.tax_type (WHERE is_active=TRUE) | 1:1 |
| dim_time | Generated (2023-01-01 to 2025-12-31) | 1096 rows |
| fact_registration | registration.tax_account (WHERE account_status_code='ACTIVE') | 1:1 |

### 2. Referential Integrity Validation

**Purpose**: Ensure foreign key relationships are valid.

**What it checks**:
- Fact table dimension foreign keys exist in dimension tables
- Natural key uniqueness (no duplicates)
- Surrogate key generation correctness

**Example Output**:
```
✓ FK Integrity: fact_registration.dim_party_key → dim_party
    fact_table: fact_registration
    dimension_table: dim_party
    fk_column: dim_party_key
    orphan_records: 0

✓ Natural Key Uniqueness: dim_party.party_id
    table: dim_party
    natural_key: party_id
    duplicate_keys: 0
```

### 3. Data Quality Validation

**Purpose**: Validate data quality rules and constraints.

**What it checks**:
- Mandatory fields are not NULL or empty
- Data type conversions are correct
- Value ranges are valid (e.g., risk_score 0-100)
- Format validation (e.g., TIN format)

**Example Output**:
```
✓ Mandatory Field: dim_party.party_id
    table: dim_party
    field: party_id
    null_or_empty_count: 0

✓ Data Quality: Party TIN Format
    invalid_tin_count: 0
    rule: TIN must be 9, 10, or 12 characters

✓ Data Quality: Risk Score Range
    invalid_score_count: 0
    rule: Risk score must be between 0 and 100
```

### 4. Business Rule Validation

**Purpose**: Verify business logic rules are correctly implemented.

**What it checks**:
- SCD Type 2 consistency (is_current flags, valid_to dates)
- Date ranges are within expected bounds
- Calculated fields are correct
- Cross-table business rules

**Example Output**:
```
✓ SCD Type 2: is_current=1 → valid_to IS NULL
    violation_count: 0
    rule: Current records must have NULL valid_to

✓ Business Rule: Registration Dates in dim_time Range
    invalid_date_count: 0
    rule: All registration dates must exist in dim_time
```

---

## Output Formats

### Console (Default)

Human-readable output with colored status indicators (✓, ✗, ⚠).

```bash
python etl/validate_etl.py
```

**Sample Output**:
```
====================================================================================================
ETL VALIDATION REPORT
====================================================================================================
Validation Time: 2025-11-06 12:16:21
====================================================================================================

OVERALL SUMMARY
----------------------------------------------------------------------------------------------------
Total Checks:     31
Passed:           31 (100.0%)
Failed:           0
Warnings:         0

ROW COUNT
----------------------------------------------------------------------------------------------------
✓ Row Count: dim_party
    L2 Source:    5 rows
    L3 Target:    5 rows
    Expected:     5 rows
    Difference:   +0
    Ratio:        100.00%
    Description:  Party dimension with individuals and enterprises
...

====================================================================================================
✓ VALIDATION PASSED - All checks successful
====================================================================================================
```

### JSON

Machine-readable JSON for programmatic processing or CI/CD integration.

```bash
python etl/validate_etl.py --format json --output validation.json
```

**Sample Output**:
```json
{
  "validation_timestamp": "2025-11-06 12:16:43",
  "table_filter": null,
  "summary": {
    "total_checks": 31,
    "passed": 31,
    "failed": 0,
    "warnings": 0,
    "pass_rate": 100.0
  },
  "validators": {
    "row_count": [
      {
        "check_name": "Row Count: dim_party",
        "status": "PASS",
        "details": {
          "l2_source_count": 5,
          "l3_target_count": 5,
          "expected_count": 5,
          "difference": 0,
          "ratio": 1.0
        }
      }
    ]
  }
}
```

### HTML

Rich HTML report with tables and styling for sharing or archiving.

```bash
python etl/validate_etl.py --format html --output validation.html
```

Opens a formatted HTML page with:
- Summary section with pass/fail counts
- Color-coded status indicators
- Detailed tables for each validator
- Expandable details for failed checks

---

## Command-Line Options

```bash
python etl/validate_etl.py [OPTIONS]

Options:
  --table TABLE         Validate specific table only (e.g., dim_party, fact_registration)
  --format FORMAT       Report output format: console, json, html (default: console)
  --output FILE         Output file path for report (stdout if not specified)
  -h, --help           Show help message
```

### Examples

```bash
# Validate entire pipeline with console output
python etl/validate_etl.py

# Validate only dim_party table
python etl/validate_etl.py --table dim_party

# Validate and save JSON report
python etl/validate_etl.py --format json --output reports/validation_$(date +%Y%m%d).json

# Validate and save HTML report
python etl/validate_etl.py --format html --output reports/validation.html

# Validate in CI/CD pipeline
python etl/validate_etl.py --format json --output validation.json
if [ $? -eq 0 ]; then
    echo "✓ ETL validation passed"
else
    echo "✗ ETL validation failed - check validation.json"
    exit 1
fi
```

---

## Integration with ETL Pipeline

### Manual Workflow

```bash
# 1. Run ETL scripts
python etl/l2_to_l3_party.py
python etl/l2_to_l3_tax_type.py
python etl/generate_dim_time.py
python etl/l2_to_l3_fact_registration.py

# 2. Validate ETL results
python etl/validate_etl.py

# 3. If validation passes, proceed with downstream processing
```

### Automated Workflow (Shell Script)

```bash
#!/bin/bash
# run_etl_with_validation.sh

set -e  # Exit on error

echo "Starting ETL pipeline..."

# Run ETL scripts
python etl/l2_to_l3_party.py
python etl/l2_to_l3_tax_type.py
python etl/generate_dim_time.py
python etl/l2_to_l3_fact_registration.py

echo "ETL complete. Running validation..."

# Validate with JSON output
python etl/validate_etl.py --format json --output validation.json

# Check result
if [ $? -eq 0 ]; then
    echo "✓ ETL validation PASSED"
    exit 0
else
    echo "✗ ETL validation FAILED"
    echo "Check validation.json for details"
    exit 1
fi
```

### CI/CD Integration (GitHub Actions)

```yaml
# .github/workflows/etl_pipeline.yml
name: ETL Pipeline with Validation

on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM
  workflow_dispatch:

jobs:
  etl_and_validate:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run ETL
        run: |
          python etl/l2_to_l3_party.py
          python etl/l2_to_l3_tax_type.py
          python etl/generate_dim_time.py
          python etl/l2_to_l3_fact_registration.py

      - name: Validate ETL
        id: validate
        run: python etl/validate_etl.py --format json --output validation.json

      - name: Upload validation report
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: validation-report
          path: validation.json
          retention-days: 30

      - name: Parse validation results
        run: |
          FAILED=$(jq '.summary.failed' validation.json)
          if [ "$FAILED" -gt 0 ]; then
            echo "::error::ETL validation failed with $FAILED check(s)"
            exit 1
          fi
          echo "::notice::ETL validation passed"
```

---

## Interpreting Results

### Status Indicators

- **✓ PASS**: Check passed successfully
- **✗ FAIL**: Check failed - data issue detected
- **⚠ WARNING**: Check passed with warnings - review recommended

### Common Failure Scenarios

#### 1. Row Count Mismatch

```
✗ Row Count: fact_registration
    L2 Source:    10 rows
    L3 Target:    9 rows
    Expected:     10 rows
    Difference:   -1
    Ratio:        90.00%
```

**What it means**: 1 record was lost during ETL.

**How to investigate**:
1. Check ETL script logs for errors
2. Verify L2 source data hasn't changed
3. Check L3 load for constraints violations
4. Re-run ETL for the missing record

#### 2. Foreign Key Orphans

```
✗ FK Integrity: fact_registration.dim_party_key → dim_party
    fact_table: fact_registration
    dimension_table: dim_party
    orphan_records: 2
```

**What it means**: 2 fact records reference party_key values that don't exist in dim_party.

**How to investigate**:
1. Check if dim_party was loaded before fact_registration
2. Verify dimension key lookup logic in ETL
3. Check for placeholder/default keys (0, -1)

#### 3. Mandatory Field NULL

```
✗ Mandatory Field: dim_party.tin
    table: dim_party
    field: tin
    null_or_empty_count: 1
```

**What it means**: 1 party record has NULL TIN.

**How to investigate**:
1. Check L2 source data for NULL TINs
2. Verify ETL transformation logic
3. Check business rules (are NULLs allowed?)

---

## Current Validation Coverage

Based on Phase B implementation:

| Validation Type | Checks | Status |
|-----------------|--------|--------|
| Row Count | 4 tables | ✅ Implemented |
| Referential Integrity | 6 checks | ✅ Implemented |
| Data Quality | 18 checks | ✅ Implemented |
| Business Rules | 3 checks | ✅ Implemented |
| **TOTAL** | **31 checks** | **✅ Complete** |

---

## Extending the Validator

### Adding New L2→L3 Mappings

Edit `etl/validators/mapping_config.py`:

```python
ETL_MAPPINGS['dim_new_dimension'] = {
    'l2_sources': [
        {
            'schema': 'my_schema',
            'table': 'my_table',
            'join_type': 'main',
            'key_column': 'id'
        }
    ],
    'l3_target': 'ta_dw.dim_new_dimension',
    'natural_key': 'id',
    'surrogate_key': 'new_dimension_key',
    'filters': {},
    'expected_ratio': 1.0,
    'mandatory_fields': ['id', 'name'],
    'description': 'My new dimension'
}
```

### Adding Custom Validation Rules

Create a method in the appropriate validator class:

```python
# In quality_validator.py
def _validate_custom_rule(self):
    """Custom validation rule."""
    query = """
        SELECT COUNT(*) as violation_count
        FROM ta_dw.dim_party
        WHERE your_custom_condition
    """

    result = self.clickhouse_client.query(query)
    violation_count = result.result_rows[0][0]

    status = 'PASS' if violation_count == 0 else 'FAIL'

    self.add_result(
        check_name='Custom Rule: My Rule Name',
        status=status,
        details={
            'violation_count': violation_count,
            'rule': 'Description of the rule'
        }
    )
```

Then call it in the `validate()` method.

---

## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'etl.validators'`

**Solution**: Run with PYTHONPATH:
```bash
PYTHONPATH=. python3 etl/validate_etl.py
```

**Issue**: Database connection errors

**Solution**:
- Check `.env` file has correct credentials
- Verify MySQL and ClickHouse are running
- Test connections manually

**Issue**: Validation takes too long

**Solution**:
- Use `--table` flag to validate specific tables
- Optimize queries in validator classes
- Index L2 and L3 tables appropriately

---

## Performance Considerations

- **Runtime**: ~3-5 seconds for current dataset (5 parties, 1096 time records, 10 facts)
- **Scalability**: Linear with data volume
- **Optimization tips**:
  - Use table filters when debugging specific issues
  - Run validation in parallel for independent tables
  - Cache reference data lookups

---

## Best Practices

1. **Run after every ETL execution**: Catch issues immediately
2. **Archive validation reports**: Keep audit trail (JSON format recommended)
3. **Monitor trends**: Track validation metrics over time
4. **Alert on failures**: Integrate with monitoring systems
5. **Use in CI/CD**: Make validation mandatory before deployment

---

## Related Documentation

- [Phase B Completion Report](../STATUS-PHASE-B.md)
- [L2→L3 Mapping Documentation](L2-L3-Mapping.md)
- [TA-RDM MySQL DDL](ta-rdm-mysql-ddl.sql)
- [ClickHouse Schema](ta_dw_clickhouse_schema.sql)

---

**Last Updated**: 2025-11-06
**Version**: 1.0
**Author**: TA-RDM Development Team
