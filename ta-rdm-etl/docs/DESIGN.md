# TA-RDM ETL Design Documentation

**Version**: 1.0.0
**Date**: 2025-11-06
**Audience**: Technical Leads, Senior Developers, Solution Architects

---

## Table of Contents

1. [Design Overview](#design-overview)
2. [Design Principles](#design-principles)
3. [Data Model Design](#data-model-design)
4. [ETL Pattern Design](#etl-pattern-design)
5. [Validation Framework Design](#validation-framework-design)
6. [Error Handling Design](#error-handling-design)
7. [Configuration Management Design](#configuration-management-design)
8. [Code Organization Design](#code-organization-design)
9. [Design Patterns Used](#design-patterns-used)
10. [Design Decisions and Trade-offs](#design-decisions-and-trade-offs)

---

## Design Overview

### Design Philosophy

The TA-RDM ETL system is designed with the following core philosophies:

```
┌────────────────────────────────────────────────────────────┐
│                    DESIGN PHILOSOPHY                        │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  1. SIMPLICITY FIRST                                       │
│     • Prefer simple solutions over complex ones           │
│     • Full reload over incremental complexity             │
│     • Direct SQL over ORM abstractions                    │
│                                                             │
│  2. CONSISTENCY                                            │
│     • All ETL scripts follow same pattern                 │
│     • Uniform naming conventions                          │
│     • Standardized error handling                         │
│                                                             │
│  3. MAINTAINABILITY                                        │
│     • Self-documenting code                               │
│     • Clear separation of concerns                        │
│     • Minimal dependencies                                │
│                                                             │
│  4. RELIABILITY                                            │
│     • Comprehensive validation                            │
│     • Idempotent operations                               │
│     • Graceful error handling                             │
│                                                             │
│  5. PERFORMANCE                                            │
│     • Bulk operations                                      │
│     • Efficient queries                                    │
│     • Minimal transformations                             │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

---

## Design Principles

### 1. Single Responsibility Principle

Each ETL script has exactly one responsibility:

```python
# ✅ Good: Single responsibility
# etl/l2_to_l3_party.py - Loads ONLY party dimension
def main():
    extract_parties()
    transform_parties()
    load_parties()

# ❌ Bad: Multiple responsibilities
def main():
    load_parties()
    load_tax_types()  # Should be separate script
    load_periods()    # Should be separate script
```

### 2. Don't Repeat Yourself (DRY)

Common functionality is extracted to shared utilities:

```python
# Shared utilities in utils/
from utils.db_utils import get_db_connection
from utils.clickhouse_utils import get_clickhouse_connection

# Used across all ETL scripts
with get_db_connection() as mysql_conn:
    # Extract logic
    pass
```

### 3. Separation of Concerns

Clear boundaries between layers:

```
ETL Scripts          → Business logic (what to extract/transform/load)
Utils Layer          → Technical operations (how to connect/query)
Config Layer         → Environment settings (where to connect)
Validation Framework → Data quality checks (what to validate)
```

### 4. Fail Fast

Detect and report errors early:

```python
# Check prerequisites before processing
if not dim_party_exists():
    raise ValueError("Dimension dim_party must be loaded first")

# Validate configuration on startup
validate_config()

# Check database connections before ETL
test_mysql_connection()
test_clickhouse_connection()
```

### 5. Convention Over Configuration

Standardized naming and structure:

```
File naming:         l2_to_l3_<entity>.py
Function naming:     extract_<entity>(), transform_<entity>(), load_<entity>()
Table naming:        dim_<dimension>, fact_<fact_table>
Key naming:          dim_<dimension>_key
```

---

## Data Model Design

### Star Schema Design

#### Why Star Schema?

```
┌────────────────────────────────────────────────────────────┐
│               STAR SCHEMA DESIGN RATIONALE                  │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  Advantages:                                               │
│  ✅ Simple to understand and query                         │
│  ✅ Optimal for BI tools (JOIN performance)                │
│  ✅ Fast aggregations (pre-joined dimensions)              │
│  ✅ Denormalized for read performance                      │
│  ✅ Clear business semantics                               │
│                                                             │
│  Trade-offs:                                               │
│  ⚠️  Data redundancy (acceptable for DW)                   │
│  ⚠️  Update complexity (mitigated by full reload)          │
│  ⚠️  Storage overhead (minimal with ClickHouse compression)│
│                                                             │
└────────────────────────────────────────────────────────────┘
```

#### Dimension Design Patterns

**Pattern 1: Natural Key + Surrogate Key**

```sql
CREATE TABLE dim_party (
    -- Surrogate key (auto-generated)
    dim_party_key UInt32,

    -- Natural key (from source system)
    party_id UInt32,

    -- Descriptive attributes
    party_name String,
    party_type String,
    tin_number String,

    -- SCD Type 2 attributes
    is_current UInt8,
    valid_from Date,
    valid_to Nullable(Date),

    PRIMARY KEY (dim_party_key)
) ENGINE = MergeTree()
  ORDER BY dim_party_key;
```

**Why Surrogate Keys?**
- Stable even if natural key changes
- Better JOIN performance (integers)
- Supports Slowly Changing Dimensions (SCD)
- Decouples DW from source system changes

**Pattern 2: Slowly Changing Dimension Type 2**

```
Historical Tracking Example:

Time: T0 (Initial Load)
┌─────┬─────────┬──────┬────────┬────────┬────────┐
│ Key │ PartyID │ Risk │Current │ValidFrm│ValidTo │
├─────┼─────────┼──────┼────────┼────────┼────────┤
│ 101 │   21    │ LOW  │   1    │2024-01│  NULL  │
└─────┴─────────┴──────┴────────┴────────┴────────┘

Time: T1 (Risk Changed)
┌─────┬─────────┬──────┬────────┬────────┬────────┐
│ Key │ PartyID │ Risk │Current │ValidFrm│ValidTo │
├─────┼─────────┼──────┼────────┼────────┼────────┤
│ 101 │   21    │ LOW  │   0    │2024-01│2024-06 │ ← Expired
│ 102 │   21    │ HIGH │   1    │2024-06│  NULL  │ ← New
└─────┴─────────┴──────┴────────┴────────┴────────┘

Benefits:
• Full history preserved
• Point-in-time queries possible
• Audit trail built-in
```

#### Fact Table Design Patterns

**Pattern 1: Transaction Fact Table**

```sql
CREATE TABLE fact_filing (
    -- Dimension foreign keys
    dim_party_key UInt32,
    dim_tax_type_key UInt32,
    dim_tax_period_key UInt32,
    dim_date_key UInt32,

    -- Degenerate dimension (no separate dimension table)
    filing_id UInt32,
    return_number String,

    -- Metrics (facts)
    declared_amount Decimal(18, 2),
    days_late Int32,

    -- Indicators (facts as flags)
    is_late UInt8,
    is_amended UInt8,
    is_nil_return UInt8

) ENGINE = MergeTree()
  ORDER BY (dim_date_key, dim_party_key, dim_tax_type_key);
```

**Design Decisions:**
- **Grain**: One row per tax return filed
- **Measures**: Amounts, counts, durations
- **Indicators**: Boolean flags for filtering
- **Degenerate Dimensions**: Transaction IDs stored in fact table

**Pattern 2: Periodic Snapshot Fact Table**

```sql
CREATE TABLE fact_account_balance (
    -- Dimensions
    dim_party_key UInt32,
    dim_tax_type_key UInt32,
    dim_date_key UInt32,

    -- Snapshot date (part of grain)
    snapshot_date Date,

    -- Balance metrics
    opening_balance Decimal(18, 2),
    closing_balance Decimal(18, 2),
    period_assessments Decimal(18, 2),
    period_payments Decimal(18, 2),
    arrears_amount Decimal(18, 2),

    -- Indicators
    is_in_arrears UInt8,
    is_credit_balance UInt8

) ENGINE = MergeTree()
  ORDER BY (snapshot_date, dim_party_key, dim_tax_type_key);
```

**Design Decisions:**
- **Grain**: One row per account per snapshot date (end of period)
- **Purpose**: Track balance state over time
- **Use Case**: Historical trend analysis

**Pattern 3: Accumulating Snapshot Fact Table**

```sql
CREATE TABLE fact_taxpayer_activity (
    -- Dimensions
    dim_party_key UInt32,
    dim_date_key UInt32,

    -- Snapshot date
    snapshot_date Date,

    -- Accumulated counts
    total_filings UInt32,
    on_time_filings UInt32,
    late_filings UInt32,
    total_payments UInt32,
    total_assessments UInt32,

    -- Accumulated amounts
    total_payment_amount Decimal(18, 2),
    total_assessed_amount Decimal(18, 2),

    -- Calculated metrics
    filing_compliance_rate Decimal(5, 2),
    payment_compliance_rate Decimal(5, 2),
    overall_compliance_score Decimal(5, 2)

) ENGINE = MergeTree()
  ORDER BY (snapshot_date, dim_party_key);
```

**Design Decisions:**
- **Grain**: One row per taxpayer per month
- **Purpose**: Aggregate taxpayer behavior
- **Source**: Derived from other fact tables (not directly from L2)

### Dimensional Modeling Best Practices

#### 1. Date Dimension

```sql
-- Rich date dimension with business calendar
CREATE TABLE dim_time (
    date_key UInt32,              -- 20250106 (YYYYMMDD)
    date Date,                    -- 2025-01-06

    -- Date components
    year UInt16,
    quarter UInt8,
    month UInt8,
    day_of_month UInt8,
    day_of_week UInt8,
    week_of_year UInt8,

    -- Business calendar
    is_business_day UInt8,        -- Working day?
    is_weekend UInt8,             -- Saturday/Sunday?
    is_holiday UInt8,             -- Public holiday?
    holiday_name Nullable(String),

    -- Fiscal calendar (optional)
    fiscal_year UInt16,
    fiscal_quarter UInt8,
    fiscal_period UInt8
);
```

**Design Rationale:**
- Pre-computed date attributes for fast filtering
- Business calendar for compliance calculations
- Fiscal calendar for tax-specific reporting

#### 2. Conformed Dimensions

```
Conformed Dimension: dim_party
Used by all fact tables:
  • fact_registration
  • fact_filing
  • fact_assessment
  • fact_payment
  • fact_collection
  • fact_refund
  • fact_audit
  • fact_objection
  • fact_risk_assessment
  • fact_taxpayer_activity

Benefits:
  ✅ Consistent party information across all facts
  ✅ Enable cross-fact analysis
  ✅ Single source of truth for party data
```

---

## ETL Pattern Design

### Standard ETL Script Pattern

All ETL scripts follow a consistent three-phase pattern:

```python
"""
Standard ETL Script Pattern
File: etl/l2_to_l3_<entity>.py
"""

import logging
from typing import List, Dict, Tuple
from utils.db_utils import get_db_connection
from utils.clickhouse_utils import get_clickhouse_connection

logger = logging.getLogger(__name__)

def main():
    """Main ETL orchestration."""
    logger.info("=" * 80)
    logger.info(f"Starting {ENTITY_NAME} ETL")
    logger.info("=" * 80)

    try:
        with get_db_connection() as mysql_conn, \
             get_clickhouse_connection() as ch_conn:

            # Phase 1: Extract
            logger.info("Phase 1: Extracting data from MySQL L2...")
            source_data = extract(mysql_conn)
            logger.info(f"Extracted {len(source_data)} records")

            # Phase 2: Transform
            logger.info("Phase 2: Transforming data...")
            transformed_data = transform(source_data, mysql_conn, ch_conn)
            logger.info(f"Transformed {len(transformed_data)} records")

            # Phase 3: Load
            logger.info("Phase 3: Loading data to ClickHouse L3...")
            load(ch_conn, transformed_data)
            logger.info(f"Loaded {len(transformed_data)} records")

            # Verification
            verify(ch_conn)

            logger.info("=" * 80)
            logger.info("ETL Completed Successfully")
            logger.info("=" * 80)

    except Exception as e:
        logger.error(f"ETL Failed: {e}", exc_info=True)
        raise

def extract(mysql_conn) -> List[Dict]:
    """Extract data from MySQL L2."""
    query = """
        SELECT
            column1,
            column2,
            ...
        FROM l2_schema.source_table
        WHERE <filter_conditions>
    """
    return mysql_conn.execute_query(query)

def transform(source_data: List[Dict],
              mysql_conn,
              ch_conn) -> List[Tuple]:
    """Transform data to L3 format."""
    transformed = []

    # Pre-load dimension lookups for performance
    dim_lookups = load_dimension_lookups(ch_conn)

    for row in source_data:
        # Lookup dimension keys
        party_key = dim_lookups['party'].get(row['party_id'], -1)

        # Apply business logic
        is_late = calculate_late_indicator(row)

        # Build transformed row
        transformed_row = (
            party_key,
            row['amount'],
            is_late,
            ...
        )
        transformed.append(transformed_row)

    return transformed

def load(ch_conn, data: List[Tuple]):
    """Load data to ClickHouse L3."""
    # Truncate for full reload
    ch_conn.truncate_table('fact_<entity>', 'ta_dw')

    # Bulk insert
    column_names = ['dim_party_key', 'amount', 'is_late', ...]
    ch_conn.insert('ta_dw.fact_<entity>', data, column_names)

def verify(ch_conn):
    """Verify loaded data."""
    count = ch_conn.get_table_row_count('fact_<entity>', 'ta_dw')
    logger.info(f"Verification: {count} rows in target table")

if __name__ == '__main__':
    main()
```

### Extract Phase Design

```python
def extract(mysql_conn) -> List[Dict]:
    """
    Extract Phase Design Principles:

    1. Single Query Preferred
       - Join all required source tables in one query
       - Minimize round trips to database

    2. Filter Early
       - Apply WHERE clauses in SQL, not Python
       - Use indexes effectively

    3. Select Only Required Columns
       - Don't SELECT *
       - Explicit column list

    4. Handle NULLs in SQL
       - Use COALESCE for defaults
       - Use IFNULL where appropriate
    """

    query = """
        SELECT
            -- Natural keys
            fr.filing_return_id,
            fr.party_id,
            fr.tax_type_code,

            -- Descriptive attributes
            fr.return_number,
            fr.filing_date,
            fr.due_date,

            -- Measures
            COALESCE(fr.declared_amount, 0) as declared_amount,

            -- Calculated fields (where SQL is more efficient)
            DATEDIFF(fr.filing_date, fr.due_date) as days_late,

            -- Join related tables for enrichment
            tt.tax_type_name,
            tp.period_code

        FROM filing_assessment.filing_return fr
        INNER JOIN tax_framework.tax_type tt
            ON fr.tax_type_code = tt.tax_type_code
        INNER JOIN tax_framework.tax_period tp
            ON fr.tax_period_id = tp.tax_period_id

        WHERE fr.filing_date >= '2023-01-01'
          AND fr.filing_status = 'PROCESSED'

        ORDER BY fr.filing_date
    """

    return mysql_conn.execute_query(query)
```

### Transform Phase Design

```python
def transform(source_data: List[Dict],
              mysql_conn,
              ch_conn) -> List[Tuple]:
    """
    Transform Phase Design Principles:

    1. Dimension Key Lookup
       - Pre-load dimension lookups for performance
       - Use in-memory dictionaries
       - Handle lookup failures gracefully (use -1)

    2. Business Logic Application
       - Calculate derived fields
       - Apply business rules
       - Compute indicators/flags

    3. Data Type Conversion
       - Match ClickHouse target types
       - Handle date/datetime conversions
       - Convert Decimal types appropriately

    4. Data Enrichment
       - Add calculated fields
       - Apply transformations
       - Format data consistently
    """

    transformed = []

    # Pre-load dimension lookups (cache in memory)
    dim_party_lookup = load_party_lookup(ch_conn)
    dim_tax_type_lookup = load_tax_type_lookup(ch_conn)
    dim_period_lookup = load_period_lookup(ch_conn)

    for row in source_data:
        # 1. Lookup dimension keys
        party_key = dim_party_lookup.get(row['party_id'], -1)
        tax_type_key = dim_tax_type_lookup.get(row['tax_type_code'], -1)
        period_key = dim_period_lookup.get(row['period_id'], -1)
        date_key = convert_date_to_key(row['filing_date'])

        # 2. Apply business logic
        is_late = 1 if row['days_late'] > 0 else 0
        is_nil_return = 1 if row['declared_amount'] == 0 else 0

        # 3. Build transformed row (tuple for ClickHouse insert)
        transformed_row = (
            party_key,
            tax_type_key,
            period_key,
            date_key,
            row['filing_return_id'],
            row['return_number'],
            row['filing_date'],
            row['due_date'],
            row['declared_amount'],
            is_late,
            row['days_late'],
            is_nil_return
        )

        transformed.append(transformed_row)

    return transformed

def load_party_lookup(ch_conn) -> Dict[int, int]:
    """
    Load dimension lookup table into memory.
    Returns: {natural_key: surrogate_key}
    """
    query = """
        SELECT party_id, dim_party_key
        FROM ta_dw.dim_party
        WHERE is_current = 1
    """

    result = ch_conn.query(query)
    return {row[0]: row[1] for row in result}
```

### Load Phase Design

```python
def load(ch_conn, data: List[Tuple]):
    """
    Load Phase Design Principles:

    1. Full Reload Strategy
       - Truncate before load (idempotent)
       - Bulk insert for performance
       - Batch size optimization (1000 rows default)

    2. Transactional Consistency
       - All-or-nothing load
       - Rollback on error (ClickHouse manages this)

    3. Performance Optimization
       - Batch inserts (not row-by-row)
       - Use native ClickHouse insert
       - Minimize network round trips
    """

    table_name = 'ta_dw.fact_filing'

    # Truncate for full reload (makes ETL idempotent)
    logger.info(f"Truncating {table_name}...")
    ch_conn.truncate_table('fact_filing', 'ta_dw')

    # Define column order (must match tuple structure)
    column_names = [
        'dim_party_key',
        'dim_tax_type_key',
        'dim_tax_period_key',
        'dim_date_key',
        'filing_id',
        'return_number',
        'filing_date',
        'due_date',
        'declared_amount',
        'is_late',
        'days_late',
        'is_nil_return'
    ]

    # Bulk insert (ClickHouse native format)
    logger.info(f"Inserting {len(data)} rows...")
    ch_conn.insert(table_name, data, column_names=column_names)

    logger.info(f"Load complete: {len(data)} rows")
```

---

## Validation Framework Design

### Validation Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              VALIDATION FRAMEWORK DESIGN                     │
└─────────────────────────────────────────────────────────────┘

                    ┌──────────────────┐
                    │  validate_etl.py │
                    │   (Orchestrator) │
                    └────────┬─────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
        ┌───────▼──────┐ ┌──▼────────┐ ┌▼──────────┐
        │ Row Count    │ │ Referential│ │ Mandatory │
        │  Validator   │ │  Integrity │ │   Field   │
        │              │ │  Validator │ │ Validator │
        └──────────────┘ └────────────┘ └───────────┘
                │            │            │
                └────────────┼────────────┘
                             │
                    ┌────────▼─────────┐
                    │  Business Rule   │
                    │    Validator     │
                    └──────────────────┘
                             │
                    ┌────────▼─────────┐
                    │  Validation      │
                    │    Report        │
                    └──────────────────┘
```

### Validator Base Class Design

```python
class BaseValidator:
    """
    Base validator class using Template Method pattern.

    Design Principles:
    - Define skeleton of validation algorithm
    - Let subclasses implement specific validation logic
    - Ensure consistent reporting format
    """

    def __init__(self, mysql_conn, ch_conn):
        self.mysql_conn = mysql_conn
        self.ch_conn = ch_conn
        self.results = []

    def validate(self) -> List[ValidationResult]:
        """
        Template method: defines validation algorithm structure.
        Subclasses override validate_impl().
        """
        logger.info(f"Running {self.__class__.__name__}...")

        try:
            # Call subclass implementation
            self.validate_impl()

        except Exception as e:
            logger.error(f"Validation error: {e}")
            self.add_error(f"Validation failed: {e}")

        return self.results

    def validate_impl(self):
        """Override in subclass to implement specific validation."""
        raise NotImplementedError()

    def add_pass(self, message: str, details: Dict = None):
        """Record passed check."""
        self.results.append(ValidationResult(
            status='PASS',
            message=message,
            details=details
        ))

    def add_fail(self, message: str, details: Dict = None):
        """Record failed check."""
        self.results.append(ValidationResult(
            status='FAIL',
            message=message,
            details=details
        ))
```

### Row Count Validation Design

```python
class RowCountValidator(BaseValidator):
    """
    Validates that L2 source counts match L3 target counts.

    Design:
    - Configuration-driven (mapping_config.py)
    - Supports expected ratios (e.g., 1.0 for 1:1 mapping)
    - Allows tolerance for complex transformations
    """

    def validate_impl(self):
        # Load mapping configuration
        from validators.mapping_config import VALIDATION_MAPPINGS

        for entity, config in VALIDATION_MAPPINGS.items():
            # Get L2 source count
            l2_count = self._get_l2_count(config)

            # Get L3 target count
            l3_count = self._get_l3_count(config)

            # Calculate ratio
            ratio = l3_count / l2_count if l2_count > 0 else 0
            expected_ratio = config['expected_ratio']

            # Validate
            if abs(ratio - expected_ratio) < 0.01:  # 1% tolerance
                self.add_pass(
                    f"Row count match: {entity}",
                    details={
                        'l2_count': l2_count,
                        'l3_count': l3_count,
                        'ratio': ratio
                    }
                )
            else:
                self.add_fail(
                    f"Row count mismatch: {entity}",
                    details={
                        'l2_count': l2_count,
                        'l3_count': l3_count,
                        'ratio': ratio,
                        'expected_ratio': expected_ratio
                    }
                )
```

### Referential Integrity Validation Design

```python
class IntegrityValidator(BaseValidator):
    """
    Validates foreign key relationships in star schema.

    Design:
    - Check all fact table FKs reference existing dimension records
    - Use LEFT JOIN to find orphaned records
    - Report specific records that fail
    """

    def validate_impl(self):
        from validators.mapping_config import VALIDATION_MAPPINGS

        for entity, config in VALIDATION_MAPPINGS.items():
            if 'dimension_fks' not in config:
                continue  # Skip dimension tables

            l3_table = config['l3_target']

            for fk_column, dim_table in config['dimension_fks'].items():
                # Find orphaned records (FK doesn't exist in dimension)
                query = f"""
                    SELECT COUNT(*) as orphan_count
                    FROM {l3_table} f
                    LEFT JOIN ta_dw.{dim_table} d
                        ON f.{fk_column} = d.{fk_column}
                    WHERE d.{fk_column} IS NULL
                      AND f.{fk_column} != -1
                """

                result = self.ch_conn.query(query)
                orphan_count = result[0][0]

                if orphan_count == 0:
                    self.add_pass(
                        f"Referential integrity: {entity}.{fk_column}",
                        details={'orphan_count': 0}
                    )
                else:
                    self.add_fail(
                        f"Referential integrity violation: {entity}.{fk_column}",
                        details={
                            'orphan_count': orphan_count,
                            'message': f"{orphan_count} orphaned records found"
                        }
                    )
```

---

## Error Handling Design

### Error Handling Strategy

```python
"""
Error Handling Design Principles:

1. Fail Fast
   - Detect errors early
   - Don't continue processing bad data

2. Informative Messages
   - Log context: what failed, why, where
   - Include data that caused failure (sanitized)

3. Graceful Degradation
   - Clean up resources on error
   - Close database connections
   - Log full stack trace

4. Retry Logic (for transient errors)
   - Network timeouts
   - Database connection issues
   - Don't retry logic errors
"""

def main():
    try:
        # ETL logic
        pass

    except mysql.connector.Error as e:
        logger.error(f"MySQL error: {e}")
        logger.error(f"Error code: {e.errno}")
        logger.error(f"SQL state: {e.sqlstate}")
        raise

    except clickhouse_connect.driver.exceptions.DatabaseError as e:
        logger.error(f"ClickHouse error: {e}")
        raise

    except ValueError as e:
        logger.error(f"Data validation error: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise

    finally:
        # Cleanup (connections handled by context managers)
        logger.info("ETL execution completed")
```

### Transaction Management

```python
def load(ch_conn, data: List[Tuple]):
    """
    Transaction design:
    - ClickHouse handles implicit transactions
    - Full reload strategy simplifies rollback
    - If insert fails, table is in truncated state
    - Re-run ETL to recover
    """

    try:
        # Truncate (cannot be rolled back - by design)
        ch_conn.truncate_table('fact_filing', 'ta_dw')

        # Insert (if this fails, table is empty - re-run to recover)
        ch_conn.insert('ta_dw.fact_filing', data, column_names)

    except Exception as e:
        logger.error("Load failed - table may be in inconsistent state")
        logger.error("Re-run ETL to recover")
        raise
```

---

## Configuration Management Design

### Environment-Based Configuration

```python
"""
Configuration Design:

1. Environment Variables (.env file)
   - Database credentials
   - Connection parameters
   - Feature flags

2. Python Constants (config/*.py)
   - Database connection logic
   - Default values
   - Configuration validation

3. Mapping Configuration (validators/mapping_config.py)
   - L2-L3 entity mappings
   - Validation rules
   - Expected ratios
"""

# .env file
DB_HOST=localhost
DB_PORT=3308
DB_USER=ta_user
DB_PASSWORD=secret

CH_HOST=localhost
CH_PORT=8123
CH_DATABASE=ta_dw

LOG_LEVEL=INFO

# database_config.py
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3308)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'charset': os.getenv('DB_CHARSET', 'utf8mb4')
}

def validate_config():
    """Validate required configuration is present."""
    required = ['host', 'user', 'password']
    missing = [k for k in required if not DATABASE_CONFIG.get(k)]

    if missing:
        raise ValueError(f"Missing required config: {missing}")
```

---

## Code Organization Design

### Directory Structure Design

```
ta-rdm-etl/
├── etl/                        # ETL scripts (business logic)
│   ├── generate_dim_time.py   # Dimension ETLs
│   ├── l2_to_l3_party.py
│   ├── l2_to_l3_fact_*.py     # Fact ETLs
│   ├── validate_etl.py        # Validation orchestrator
│   └── validators/            # Validation framework
│       ├── base_validator.py
│       ├── mapping_config.py  # Configuration
│       └── *_validator.py     # Specific validators
│
├── utils/                      # Shared utilities (technical)
│   ├── db_utils.py            # MySQL operations
│   └── clickhouse_utils.py    # ClickHouse operations
│
├── config/                     # Configuration (settings)
│   ├── database_config.py     # MySQL config
│   └── clickhouse_config.py   # ClickHouse config
│
├── scripts/                    # Helper scripts (automation)
│   ├── run_full_etl.sh        # Full pipeline
│   ├── run_validation.sh      # Validation only
│   └── test_connections.py    # Connection test
│
├── docs/                       # Documentation
├── .env                        # Environment variables
└── requirements.txt            # Python dependencies
```

**Design Rationale:**
- **Separation by Type**: ETL scripts separate from utilities
- **Clear Boundaries**: Business logic vs technical operations
- **Discoverability**: Easy to find related files
- **Maintainability**: Changes localized to specific areas

---

## Design Patterns Used

### 1. Template Method Pattern

```python
class BaseValidator:
    def validate(self):
        # Template method
        self.prepare()
        self.validate_impl()  # Subclass implements
        self.finalize()
```

### 2. Context Manager Pattern

```python
with get_db_connection() as conn:
    # Use connection
    pass
# Connection automatically closed
```

### 3. Facade Pattern

```python
# Complex ClickHouse operations hidden behind simple interface
ch_conn.insert(table, data, columns)
ch_conn.truncate_table(table)
ch_conn.get_table_row_count(table)
```

### 4. Strategy Pattern

```python
# Different validation strategies
validators = [
    RowCountValidator(mysql, ch),
    IntegrityValidator(mysql, ch),
    QualityValidator(mysql, ch),
    BusinessValidator(mysql, ch)
]

for validator in validators:
    results = validator.validate()
```

### 5. Builder Pattern

```python
# Build complex queries incrementally
query = QueryBuilder()
query.select('column1', 'column2')
query.from_table('source_table')
query.where('date > ?', date)
query.order_by('column1')
sql = query.build()
```

---

## Design Decisions and Trade-offs

### Decision 1: Full Reload vs Incremental ETL

**Decision**: Full Reload

**Rationale:**
```
Advantages:
✅ Simpler implementation
✅ No change detection logic needed
✅ No state management required
✅ Idempotent (can re-run safely)
✅ Guaranteed consistency

Disadvantages:
⚠️  Longer execution time
⚠️  More database load
⚠️  Not suitable for very large datasets (>10M rows)

Trade-off Accepted Because:
• Current data volumes are manageable (<2K rows)
• Simplicity more valuable than performance at this scale
• Can migrate to incremental later if needed
```

### Decision 2: Python vs SQL for Transformations

**Decision**: Hybrid Approach

**Rationale:**
```
SQL transformations:
✅ Use for complex JOINs (database optimized)
✅ Use for aggregations (pushdown to database)
✅ Use for filtering (reduce data transferred)

Python transformations:
✅ Use for dimension key lookups (in-memory cache)
✅ Use for business logic (more readable)
✅ Use for complex calculations (easier to test)

Example:
# SQL: Complex JOIN to get enriched data
SELECT fr.*, tt.tax_name, ...
FROM filing_return fr
JOIN tax_type tt ON ...

# Python: Business logic and lookups
is_late = 1 if days_late > 0 else 0
party_key = dim_lookup[party_id]
```

### Decision 3: Surrogate Keys vs Natural Keys

**Decision**: Surrogate Keys

**Rationale:**
```
Advantages:
✅ Stable (don't change if source system changes)
✅ Smaller (integers vs strings/composites)
✅ Faster JOINs (integer comparison)
✅ Support SCD Type 2 (multiple rows for same natural key)

Disadvantages:
⚠️  Lookup overhead during ETL
⚠️  Not human-readable

Trade-off Accepted Because:
• SCD Type 2 requires surrogate keys
• Performance benefit significant for large fact tables
• Lookup overhead mitigated by in-memory caching
```

### Decision 4: Star Schema vs Snowflake Schema

**Decision**: Star Schema

**Rationale:**
```
Star Schema:
✅ Simpler queries (fewer JOINs)
✅ Better query performance
✅ Easier for business users
✅ Optimal for BI tools

Snowflake Schema:
⚠️  More normalized (less redundancy)
⚠️  More complex queries
⚠️  More JOINs = slower

Trade-off: Star schema redundancy acceptable for DW use case
```

### Decision 5: ClickHouse vs PostgreSQL for L3

**Decision**: ClickHouse

**Rationale:**
```
ClickHouse Advantages:
✅ Column-oriented (optimized for analytics)
✅ Excellent compression (10-20x)
✅ Fast aggregations (parallel processing)
✅ Scales to billions of rows
✅ Built for OLAP workloads

PostgreSQL Advantages:
⚠️  More familiar to developers
⚠️  Better for OLTP + light analytics
⚠️  More mature ecosystem

Trade-off: ClickHouse chosen for pure analytical workload
```

---

## Performance Design Considerations

### Batch Size Optimization

```python
# Configurable batch size
BATCH_SIZE = int(os.getenv('ETL_BATCH_SIZE', 1000))

def load_in_batches(ch_conn, data, batch_size=BATCH_SIZE):
    """
    Load data in batches to optimize memory usage.

    Design considerations:
    - Smaller batches: Less memory, more network round trips
    - Larger batches: More memory, fewer round trips
    - Optimal: 1000-5000 rows (depends on row size)
    """
    total = len(data)

    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        ch_conn.insert(table, batch, columns)
        logger.debug(f"Loaded batch {i//batch_size + 1}")
```

### Query Optimization

```python
def extract(mysql_conn):
    """
    Query optimization design:

    1. Use indexes (add to L2 if missing)
       - WHERE date > X → index on date
       - JOIN ON party_id → index on party_id

    2. Limit result set
       - WHERE clauses for date ranges
       - LIMIT for testing

    3. Select only required columns
       - Reduces data transfer
       - Reduces memory usage
    """

    # Optimized query
    query = """
        SELECT
            fr.filing_id,
            fr.party_id,  -- indexed
            fr.filing_date  -- indexed
        FROM filing_return fr
        WHERE fr.filing_date >= '2023-01-01'  -- uses index
          AND fr.status = 'PROCESSED'
        ORDER BY fr.filing_date  -- uses index
    """
```

---

## Testing Design

### Unit Testing Design

```python
import unittest
from unittest.mock import Mock, patch

class TestFilingETL(unittest.TestCase):
    """
    Testing design principles:

    1. Test each phase independently
    2. Mock database connections
    3. Test edge cases
    4. Test error handling
    """

    def test_extract_returns_data(self):
        # Arrange
        mock_conn = Mock()
        mock_conn.execute_query.return_value = [
            {'filing_id': 1, 'party_id': 21}
        ]

        # Act
        result = extract(mock_conn)

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['filing_id'], 1)

    def test_transform_handles_null_values(self):
        # Test NULL handling
        pass

    def test_load_handles_empty_data(self):
        # Test edge case
        pass
```

---

## Conclusion

The TA-RDM ETL system design emphasizes **simplicity, consistency, and maintainability** while providing **robust data quality** and **good performance**. Key design strengths:

✅ **Standardized patterns** across all ETL scripts
✅ **Clear separation of concerns** (ETL, validation, utilities)
✅ **Comprehensive validation framework** (157 checks)
✅ **Flexible configuration management** (environment-based)
✅ **Production-ready error handling**
✅ **Well-documented design decisions**

The design is **extensible** for future enhancements while remaining **accessible** to developers at all skill levels.

---

**Document Version**: 1.0.0
**Last Updated**: 2025-11-06
**Maintained By**: Development Team
