# TA-RDM ETL Developer Guide

**Version**: 1.0.0
**Date**: 2025-11-06
**Audience**: Developers, ETL Engineers, Data Engineers

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Environment Setup](#development-environment-setup)
3. [Adding a New Dimension ETL](#adding-a-new-dimension-etl)
4. [Adding a New Fact Table ETL](#adding-a-new-fact-table-etl)
5. [Modifying Existing ETL Scripts](#modifying-existing-etl-scripts)
6. [Adding Custom Transformations](#adding-custom-transformations)
7. [Adding New Validation Rules](#adding-new-validation-rules)
8. [Testing Your Changes](#testing-your-changes)
9. [Debugging ETL Issues](#debugging-etl-issues)
10. [Performance Tuning](#performance-tuning)
11. [Best Practices](#best-practices)
12. [Common Pitfalls](#common-pitfalls)

---

## Getting Started

### Prerequisites for Development

```bash
# Required software
- Python 3.9 or higher
- MySQL 9.0+ (L2 database access)
- ClickHouse 24.11+ (L3 database access)
- Git (for version control)
- Text editor or IDE (VS Code, PyCharm, etc.)

# Optional but recommended
- DBeaver or MySQL Workbench (database browsing)
- ClickHouse client (for testing queries)
```

### Understanding the Codebase

```
ta-rdm-etl/
├── etl/                    # ← YOU WILL WORK HERE
│   ├── generate_dim_time.py
│   ├── l2_to_l3_party.py
│   ├── l2_to_l3_fact_*.py
│   ├── validate_etl.py
│   └── validators/         # ← FOR VALIDATION CHANGES
│
├── utils/                  # ← SHARED UTILITIES (rarely change)
│   ├── db_utils.py
│   └── clickhouse_utils.py
│
├── config/                 # ← CONFIGURATION (rarely change)
│   ├── database_config.py
│   └── clickhouse_config.py
│
└── scripts/                # ← HELPER SCRIPTS (rarely change)
```

**Most development work happens in `etl/` directory.**

---

## Development Environment Setup

### Step 1: Clone and Setup

```bash
# Navigate to project directory
cd ta-rdm-etl

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Install development dependencies (optional)
pip install pytest pytest-cov black pylint
```

### Step 2: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your database credentials
nano .env  # or use your preferred editor
```

**Important**: Use development/test database credentials, NOT production!

### Step 3: Test Your Setup

```bash
# Test database connections
python scripts/test_connections.py

# Expected output:
# ✓ MySQL L2 connection: OK
# ✓ ClickHouse L3 connection: OK
```

### Step 4: Run Existing ETL (Baseline)

```bash
# Run a simple dimension ETL
python etl/l2_to_l3_tax_type.py

# If successful, you're ready to develop!
```

---

## Adding a New Dimension ETL

### Scenario

You need to add a new dimension: `dim_region` to track geographic regions.

### Step 1: Create L3 Schema

First, create the dimension table in ClickHouse:

```sql
-- Execute in ClickHouse
CREATE TABLE IF NOT EXISTS ta_dw.dim_region (
    -- Surrogate key
    dim_region_key UInt32,

    -- Natural key
    region_id UInt32,

    -- Descriptive attributes
    region_code String,
    region_name String,
    region_type String,
    parent_region_id Nullable(UInt32),

    -- SCD Type 2 attributes (if needed)
    is_current UInt8 DEFAULT 1,
    valid_from Date DEFAULT today(),
    valid_to Nullable(Date),

    -- Metadata
    created_date DateTime DEFAULT now(),
    modified_date DateTime DEFAULT now()

) ENGINE = MergeTree()
  ORDER BY dim_region_key;
```

### Step 2: Create ETL Script

Create new file: `etl/l2_to_l3_region.py`

```python
#!/usr/bin/env python3
"""
L2 to L3 ETL: Region Dimension

Extracts region data from MySQL L2 reference schema and loads to ClickHouse L3
dimensional model (dim_region).

Usage:
    python etl/l2_to_l3_region.py

Author: Your Name
Date: 2025-11-06
"""

import logging
import sys
from typing import List, Dict, Tuple
from datetime import datetime

from utils.db_utils import get_db_connection
from utils.clickhouse_utils import get_clickhouse_connection
from config.database_config import get_connection_string as get_mysql_conn_str
from config.clickhouse_config import get_connection_string as get_ch_conn_str

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract(mysql_conn) -> List[Dict]:
    """
    Extract region data from MySQL L2.

    Returns:
        List[Dict]: Region records from L2
    """
    logger.info("Extracting region data from MySQL L2...")

    query = """
        SELECT
            region_id,
            region_code,
            region_name,
            region_type,
            parent_region_id
        FROM reference.ref_region
        WHERE is_active = 1
        ORDER BY region_code
    """

    result = mysql_conn.execute_query(query)
    logger.info(f"Extracted {len(result)} region record(s)")

    return result


def transform(source_data: List[Dict], ch_conn) -> List[Tuple]:
    """
    Transform region data for L3 dimensional model.

    Args:
        source_data: Region data from L2
        ch_conn: ClickHouse connection for dimension key generation

    Returns:
        List[Tuple]: Transformed region records
    """
    logger.info("Transforming region data...")

    transformed = []

    # Get next available dimension key
    max_key = ch_conn.get_max_value('dim_region', 'dim_region_key', 'ta_dw')
    next_key = (max_key if max_key else 0) + 1

    for idx, row in enumerate(source_data):
        dim_region_key = next_key + idx

        transformed_row = (
            dim_region_key,           # dim_region_key
            row['region_id'],         # region_id
            row['region_code'],       # region_code
            row['region_name'],       # region_name
            row['region_type'],       # region_type
            row['parent_region_id'],  # parent_region_id
            1,                        # is_current
            datetime.now().date(),    # valid_from
            None                      # valid_to
        )

        transformed.append(transformed_row)

    logger.info(f"Transformed {len(transformed)} region record(s)")

    return transformed


def load(ch_conn, data: List[Tuple]):
    """
    Load region data to ClickHouse L3.

    Args:
        ch_conn: ClickHouse connection
        data: Transformed region records
    """
    logger.info("Loading region data to ClickHouse L3...")

    table_name = 'dim_region'
    database = 'ta_dw'

    # Truncate for full reload
    logger.info(f"Truncating {database}.{table_name}...")
    ch_conn.truncate_table(table_name, database)

    # Bulk insert
    column_names = [
        'dim_region_key',
        'region_id',
        'region_code',
        'region_name',
        'region_type',
        'parent_region_id',
        'is_current',
        'valid_from',
        'valid_to'
    ]

    logger.info(f"Inserting {len(data)} rows...")
    ch_conn.insert(f'{database}.{table_name}', data, column_names=column_names)

    logger.info(f"Loaded {len(data)} region record(s) to {database}.{table_name}")


def verify(ch_conn):
    """
    Verify loaded region data.

    Args:
        ch_conn: ClickHouse connection
    """
    logger.info("Verifying loaded data...")

    # Count rows
    count = ch_conn.get_table_row_count('dim_region', 'ta_dw')
    logger.info(f"Total rows in dim_region: {count}")

    # Sample data
    query = """
        SELECT
            dim_region_key,
            region_code,
            region_name,
            region_type
        FROM ta_dw.dim_region
        ORDER BY region_code
        LIMIT 10
    """

    result = ch_conn.query(query)

    logger.info("Sample records:")
    logger.info("=" * 80)
    logger.info(f"{'Key':<10} {'Code':<15} {'Name':<30} {'Type':<15}")
    logger.info("-" * 80)

    for row in result:
        logger.info(f"{row[0]:<10} {row[1]:<15} {row[2]:<30} {row[3]:<15}")

    logger.info("=" * 80)


def main():
    """Main ETL orchestration."""
    logger.info("=" * 80)
    logger.info("Starting Region Dimension ETL (L2 → L3)")
    logger.info("=" * 80)
    logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Source (L2): {get_mysql_conn_str()}")
    logger.info(f"Target (L3): {get_ch_conn_str()}")
    logger.info("=" * 80)
    logger.info("")

    try:
        with get_db_connection() as mysql_conn, \
             get_clickhouse_connection() as ch_conn:

            # Phase 1: Extract
            source_data = extract(mysql_conn)

            if not source_data:
                logger.warning("No data extracted from L2. Exiting.")
                return 0

            # Phase 2: Transform
            transformed_data = transform(source_data, ch_conn)

            # Phase 3: Load
            load(ch_conn, transformed_data)

            # Verification
            verify(ch_conn)

            logger.info("")
            logger.info("=" * 80)
            logger.info("✅ ETL Completed Successfully")
            logger.info(f"Loaded {len(transformed_data)} region records to dim_region")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ ETL Failed")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        logger.exception(e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
```

### Step 3: Test Your New ETL

```bash
# Run the new ETL script
python etl/l2_to_l3_region.py

# Verify data loaded
python -c "
from utils.clickhouse_utils import get_clickhouse_connection
with get_clickhouse_connection() as ch:
    count = ch.get_table_row_count('dim_region', 'ta_dw')
    print(f'Loaded {count} regions')
"
```

### Step 4: Add to Pipeline

Edit `scripts/run_full_etl.sh` to include your new dimension:

```bash
# Phase 1: Dimensions
echo "=== Phase 1: Loading Dimensions ==="
run_etl "etl/generate_dim_time.py" "Time Dimension"
run_etl "etl/l2_to_l3_party.py" "Party Dimension"
run_etl "etl/l2_to_l3_tax_type.py" "Tax Type Dimension"
run_etl "etl/l2_to_l3_tax_period.py" "Tax Period Dimension"
run_etl "etl/l2_to_l3_region.py" "Region Dimension"  # ← ADD HERE
```

### Step 5: Add Validation

Update `etl/validators/mapping_config.py`:

```python
VALIDATION_MAPPINGS = {
    # ... existing mappings ...

    'dim_region': {
        'l2_sources': [
            {
                'schema': 'reference',
                'table': 'ref_region',
                'join_type': 'main',
                'key_column': 'region_id'
            }
        ],
        'l3_target': 'ta_dw.dim_region',
        'natural_key': 'region_id',
        'filters': {'is_active': 1},
        'expected_ratio': 1.0,
        'mandatory_fields': [
            'dim_region_key',
            'region_id',
            'region_code',
            'region_name'
        ],
        'description': 'Geographic region dimension'
    },

    # ... rest of mappings ...
}
```

---

## Adding a New Fact Table ETL

### Scenario

You need to add a new fact table: `fact_license` to track business license issuance.

### Step 1: Create L3 Schema

```sql
CREATE TABLE IF NOT EXISTS ta_dw.fact_license (
    -- Dimension foreign keys
    dim_party_key UInt32,
    dim_region_key UInt32,
    dim_date_key UInt32,

    -- Degenerate dimensions
    license_id UInt32,
    license_number String,

    -- Date attributes
    issue_date Date,
    expiry_date Date,
    renewal_date Nullable(Date),

    -- Measures
    license_fee Decimal(18, 2),
    penalty_amount Decimal(18, 2),
    renewal_fee Decimal(18, 2),

    -- Indicators
    is_expired UInt8,
    is_renewed UInt8,
    is_revoked UInt8,

    -- Descriptive attributes
    license_type String,
    license_status String,
    business_activity String

) ENGINE = MergeTree()
  ORDER BY (dim_date_key, dim_party_key);
```

### Step 2: Create ETL Script

Create new file: `etl/l2_to_l3_fact_license.py`

```python
#!/usr/bin/env python3
"""
L2 to L3 ETL: License Fact Table

Loads business license issuance facts from MySQL L2 to ClickHouse L3.

Author: Your Name
Date: 2025-11-06
"""

import logging
import sys
from typing import List, Dict, Tuple
from datetime import datetime

from utils.db_utils import get_db_connection
from utils.clickhouse_utils import get_clickhouse_connection
from config.database_config import get_connection_string as get_mysql_conn_str
from config.clickhouse_config import get_connection_string as get_ch_conn_str

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract(mysql_conn) -> List[Dict]:
    """Extract license data from MySQL L2."""
    logger.info("Extracting license data from MySQL L2...")

    query = """
        SELECT
            l.license_id,
            l.license_number,
            l.party_id,
            l.region_id,
            l.issue_date,
            l.expiry_date,
            l.renewal_date,
            l.license_fee,
            l.penalty_amount,
            l.renewal_fee,
            l.license_type,
            l.license_status,
            l.business_activity,

            -- Calculated fields
            CASE
                WHEN l.expiry_date < CURDATE() THEN 1
                ELSE 0
            END as is_expired,

            CASE
                WHEN l.renewal_date IS NOT NULL THEN 1
                ELSE 0
            END as is_renewed

        FROM business_registration.business_license l
        WHERE l.issue_date >= '2023-01-01'
        ORDER BY l.issue_date
    """

    result = mysql_conn.execute_query(query)
    logger.info(f"Extracted {len(result)} license record(s)")

    return result


def load_dimension_lookups(ch_conn) -> Dict:
    """Load dimension lookups for FK resolution."""
    logger.info("Loading dimension lookups...")

    lookups = {}

    # Party lookup
    party_query = """
        SELECT party_id, dim_party_key
        FROM ta_dw.dim_party
        WHERE is_current = 1
    """
    party_result = ch_conn.query(party_query)
    lookups['party'] = {row[0]: row[1] for row in party_result}

    # Region lookup
    region_query = """
        SELECT region_id, dim_region_key
        FROM ta_dw.dim_region
        WHERE is_current = 1
    """
    region_result = ch_conn.query(region_query)
    lookups['region'] = {row[0]: row[1] for row in region_result}

    logger.info(f"Loaded {len(lookups['party'])} party lookups")
    logger.info(f"Loaded {len(lookups['region'])} region lookups")

    return lookups


def convert_date_to_key(date) -> int:
    """
    Convert date to date key (YYYYMMDD format).

    Args:
        date: Date object or string

    Returns:
        int: Date key (e.g., 20250106)
    """
    if date is None:
        return 0

    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d').date()

    return int(date.strftime('%Y%m%d'))


def transform(source_data: List[Dict], ch_conn) -> List[Tuple]:
    """Transform license data for L3."""
    logger.info("Transforming license data...")

    # Load dimension lookups
    dim_lookups = load_dimension_lookups(ch_conn)

    transformed = []

    for row in source_data:
        # Lookup dimension keys
        party_key = dim_lookups['party'].get(row['party_id'], -1)
        region_key = dim_lookups['region'].get(row['region_id'], -1)
        date_key = convert_date_to_key(row['issue_date'])

        # Calculate indicators
        is_revoked = 1 if row['license_status'] == 'REVOKED' else 0

        transformed_row = (
            party_key,
            region_key,
            date_key,
            row['license_id'],
            row['license_number'],
            row['issue_date'],
            row['expiry_date'],
            row['renewal_date'],
            row['license_fee'],
            row['penalty_amount'] if row['penalty_amount'] else 0,
            row['renewal_fee'] if row['renewal_fee'] else 0,
            row['is_expired'],
            row['is_renewed'],
            is_revoked,
            row['license_type'],
            row['license_status'],
            row['business_activity']
        )

        transformed.append(transformed_row)

    logger.info(f"Transformed {len(transformed)} license record(s)")

    return transformed


def load(ch_conn, data: List[Tuple]):
    """Load license data to ClickHouse L3."""
    logger.info("Loading license data to ClickHouse L3...")

    table_name = 'fact_license'
    database = 'ta_dw'

    # Truncate for full reload
    logger.info(f"Truncating {database}.{table_name}...")
    ch_conn.truncate_table(table_name, database)

    # Define column order
    column_names = [
        'dim_party_key',
        'dim_region_key',
        'dim_date_key',
        'license_id',
        'license_number',
        'issue_date',
        'expiry_date',
        'renewal_date',
        'license_fee',
        'penalty_amount',
        'renewal_fee',
        'is_expired',
        'is_renewed',
        'is_revoked',
        'license_type',
        'license_status',
        'business_activity'
    ]

    # Bulk insert
    logger.info(f"Inserting {len(data)} rows...")
    ch_conn.insert(f'{database}.{table_name}', data, column_names=column_names)

    logger.info(f"Loaded {len(data)} license record(s) to {database}.{table_name}")


def verify(ch_conn):
    """Verify loaded license data."""
    logger.info("Verifying loaded data...")

    # Row count
    count = ch_conn.get_table_row_count('fact_license', 'ta_dw')
    logger.info(f"Total rows: {count}")

    # Summary statistics
    query = """
        SELECT
            license_type,
            COUNT(*) as count,
            SUM(license_fee) as total_fees,
            SUM(is_expired) as expired_count,
            SUM(is_renewed) as renewed_count
        FROM ta_dw.fact_license
        GROUP BY license_type
        ORDER BY count DESC
    """

    result = ch_conn.query(query)

    logger.info("=" * 80)
    logger.info("License Summary by Type:")
    logger.info("-" * 80)
    logger.info(f"{'Type':<20} {'Count':>10} {'Fees':>15} {'Expired':>10} {'Renewed':>10}")
    logger.info("-" * 80)

    for row in result:
        logger.info(f"{row[0]:<20} {row[1]:>10,} ${row[2]:>14,.2f} {row[3]:>10,} {row[4]:>10,}")

    logger.info("=" * 80)


def main():
    """Main ETL orchestration."""
    logger.info("=" * 80)
    logger.info("Starting License Fact ETL (L2 → L3)")
    logger.info("=" * 80)

    try:
        with get_db_connection() as mysql_conn, \
             get_clickhouse_connection() as ch_conn:

            # Extract
            source_data = extract(mysql_conn)

            if not source_data:
                logger.warning("No data extracted from L2. Exiting.")
                return 0

            # Transform
            transformed_data = transform(source_data, ch_conn)

            # Load
            load(ch_conn, transformed_data)

            # Verify
            verify(ch_conn)

            logger.info("")
            logger.info("=" * 80)
            logger.info("✅ ETL Completed Successfully")
            logger.info(f"Loaded {len(transformed_data)} license records")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ ETL Failed")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        logger.exception(e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
```

### Step 3: Update Pipeline and Validation

Add to `run_full_etl.sh` and `mapping_config.py` similar to dimension example above.

---

## Modifying Existing ETL Scripts

### Scenario: Add a New Field

You need to add `tax_category` field to `fact_filing`.

### Step 1: Update L3 Schema

```sql
-- Add column to ClickHouse
ALTER TABLE ta_dw.fact_filing
    ADD COLUMN tax_category String DEFAULT '';
```

### Step 2: Update Extract Query

```python
# In etl/l2_to_l3_fact_filing.py

def extract(mysql_conn) -> List[Dict]:
    query = """
        SELECT
            fr.filing_return_id,
            fr.party_id,
            fr.tax_type_code,
            tt.tax_category,  # ← ADD THIS LINE
            ...
        FROM filing_assessment.filing_return fr
        INNER JOIN tax_framework.tax_type tt
            ON fr.tax_type_code = tt.tax_type_code
        ...
    """
```

### Step 3: Update Transform Logic

```python
# In transform() function

def transform(source_data: List[Dict], mysql_conn, ch_conn) -> List[Tuple]:
    for row in source_data:
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
            is_nil_return,
            row['tax_category']  # ← ADD THIS LINE
        )
```

### Step 4: Update Load Column List

```python
# In load() function

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
    'is_nil_return',
    'tax_category'  # ← ADD THIS LINE
]
```

### Step 5: Test Changes

```bash
# Truncate and reload fact table
python etl/l2_to_l3_fact_filing.py

# Verify new column populated
python -c "
from utils.clickhouse_utils import get_clickhouse_connection
with get_clickhouse_connection() as ch:
    result = ch.query('SELECT tax_category, COUNT(*) FROM ta_dw.fact_filing GROUP BY tax_category')
    for row in result:
        print(f'{row[0]}: {row[1]} rows')
"
```

---

## Adding Custom Transformations

### Example 1: Calculate Risk Score

```python
def calculate_risk_score(row: Dict) -> int:
    """
    Calculate taxpayer risk score based on multiple factors.

    Args:
        row: Source data row with taxpayer metrics

    Returns:
        int: Risk score (0-100)
    """
    score = 0

    # Factor 1: Late filings (0-30 points)
    late_filing_rate = row['late_filings'] / row['total_filings'] if row['total_filings'] > 0 else 0
    score += int(late_filing_rate * 30)

    # Factor 2: Payment delays (0-30 points)
    late_payment_rate = row['late_payments'] / row['total_payments'] if row['total_payments'] > 0 else 0
    score += int(late_payment_rate * 30)

    # Factor 3: Audit findings (0-20 points)
    if row['audit_assessments'] > 0:
        score += 20

    # Factor 4: Arrears amount (0-20 points)
    if row['arrears_amount'] > 100000:
        score += 20
    elif row['arrears_amount'] > 50000:
        score += 10

    return min(score, 100)  # Cap at 100
```

### Example 2: Categorize Amount Ranges

```python
def categorize_amount(amount: Decimal) -> str:
    """Categorize transaction amounts into ranges."""
    if amount < 1000:
        return 'SMALL'
    elif amount < 10000:
        return 'MEDIUM'
    elif amount < 100000:
        return 'LARGE'
    else:
        return 'EXTRA_LARGE'
```

### Example 3: Handle Complex Business Rules

```python
def determine_compliance_status(row: Dict) -> str:
    """
    Determine taxpayer compliance status based on multiple criteria.

    Business Rules:
    - COMPLIANT: All filings on time, no arrears
    - AT_RISK: Some late filings, minor arrears
    - NON_COMPLIANT: Chronic delays, significant arrears
    """

    # Check filing compliance
    filing_compliance = row['on_time_filings'] / row['total_filings'] if row['total_filings'] > 0 else 1.0

    # Check payment compliance
    has_arrears = row['arrears_amount'] > 0

    # Apply business logic
    if filing_compliance >= 0.95 and not has_arrears:
        return 'COMPLIANT'
    elif filing_compliance >= 0.80 and row['arrears_amount'] < 10000:
        return 'AT_RISK'
    else:
        return 'NON_COMPLIANT'
```

---

## Adding New Validation Rules

### Scenario: Add Business Rule Validation

You want to ensure that all payments are not greater than their related assessments.

### Step 1: Create Validator Method

Edit `etl/validators/business_validator.py`:

```python
class BusinessValidator(BaseValidator):
    """Business rule validations."""

    def validate_impl(self):
        # ... existing validations ...

        # New validation: Payments should not exceed assessments
        self._validate_payment_amounts()

    def _validate_payment_amounts(self):
        """Validate that payments don't exceed related assessments."""
        logger.info("Validating payment amounts vs assessments...")

        query = """
            SELECT
                p.payment_id,
                p.payment_amount,
                a.assessed_amount
            FROM ta_dw.fact_payment p
            INNER JOIN ta_dw.fact_assessment a
                ON p.dim_party_key = a.dim_party_key
                AND p.dim_tax_type_key = a.dim_tax_type_key
                AND p.dim_tax_period_key = a.dim_tax_period_key
            WHERE p.payment_amount > a.assessed_amount
        """

        result = self.ch_conn.query(query)

        if len(result) == 0:
            self.add_pass(
                "Payment amounts validation",
                details={'message': 'No payments exceed assessments'}
            )
        else:
            self.add_fail(
                "Payment amounts validation",
                details={
                    'message': f'{len(result)} payments exceed assessments',
                    'sample_payment_ids': [row[0] for row in result[:5]]
                }
            )
```

### Step 2: Test Validation

```bash
# Run validation
python etl/validate_etl.py

# Should see your new check in the output
```

---

## Testing Your Changes

### Unit Testing Example

Create `tests/test_filing_etl.py`:

```python
import unittest
from unittest.mock import Mock, MagicMock
from etl.l2_to_l3_fact_filing import extract, transform, convert_date_to_key

class TestFilingETL(unittest.TestCase):
    """Unit tests for filing ETL."""

    def test_convert_date_to_key(self):
        """Test date to key conversion."""
        from datetime import date

        # Test with date object
        result = convert_date_to_key(date(2025, 1, 6))
        self.assertEqual(result, 20250106)

        # Test with string
        result = convert_date_to_key('2025-01-06')
        self.assertEqual(result, 20250106)

        # Test with None
        result = convert_date_to_key(None)
        self.assertEqual(result, 0)

    def test_transform_handles_null_amounts(self):
        """Test that NULL amounts are handled correctly."""
        source_data = [{
            'filing_return_id': 1,
            'party_id': 21,
            'tax_type_code': 'VAT',
            'declared_amount': None,  # NULL amount
            'filing_date': '2025-01-06',
            'due_date': '2025-01-15',
            'days_late': 0
        }]

        # Mock connections
        mock_mysql = Mock()
        mock_ch = MagicMock()
        mock_ch.query.return_value = [(21, 101)]  # party lookup

        result = transform(source_data, mock_mysql, mock_ch)

        # Check NULL was converted to 0
        self.assertEqual(result[0][8], 0)  # declared_amount position

    def test_extract_returns_list(self):
        """Test extract returns list of dictionaries."""
        mock_conn = Mock()
        mock_conn.execute_query.return_value = [
            {'filing_return_id': 1, 'party_id': 21}
        ]

        result = extract(mock_conn)

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], dict)


if __name__ == '__main__':
    unittest.main()
```

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_filing_etl.py

# Run with coverage
python -m pytest --cov=etl tests/
```

---

## Debugging ETL Issues

### Enable Debug Logging

```bash
# Run with DEBUG level
LOG_LEVEL=DEBUG python etl/l2_to_l3_fact_filing.py
```

### Debug Extract Phase

```python
# Add debug statements in extract()
def extract(mysql_conn) -> List[Dict]:
    query = """..."""

    logger.debug(f"Executing query: {query}")

    result = mysql_conn.execute_query(query)

    logger.debug(f"Query returned {len(result)} rows")
    if result:
        logger.debug(f"First row: {result[0]}")

    return result
```

### Debug Transform Phase

```python
# Add debug statements in transform()
def transform(source_data, mysql_conn, ch_conn):
    logger.debug(f"Transforming {len(source_data)} records")

    for idx, row in enumerate(source_data):
        if idx < 5:  # Debug first 5 rows
            logger.debug(f"Row {idx}: {row}")

        # ... transformation logic ...

        if idx < 5:
            logger.debug(f"Transformed {idx}: {transformed_row}")
```

### Check Dimension Lookups

```python
# Add dimension lookup debugging
def load_dimension_lookups(ch_conn):
    lookups = {}

    # Party lookup
    party_result = ch_conn.query("SELECT party_id, dim_party_key FROM ta_dw.dim_party WHERE is_current = 1")
    lookups['party'] = {row[0]: row[1] for row in party_result}

    logger.debug(f"Party lookup: {lookups['party']}")

    # Check for missing lookups during transform
    for row in source_data:
        if row['party_id'] not in lookups['party']:
            logger.warning(f"Missing party lookup for party_id={row['party_id']}")
```

### Query ClickHouse Directly

```bash
# Check loaded data
python -c "
from utils.clickhouse_utils import get_clickhouse_connection
with get_clickhouse_connection() as ch:
    result = ch.query('SELECT COUNT(*), SUM(declared_amount) FROM ta_dw.fact_filing')
    print(f'Count: {result[0][0]}, Total: {result[0][1]}')

    # Check for -1 keys (lookup failures)
    result = ch.query('SELECT COUNT(*) FROM ta_dw.fact_filing WHERE dim_party_key = -1')
    print(f'Lookup failures: {result[0][0]}')
"
```

---

## Performance Tuning

### Optimize Extract Queries

```python
# ❌ Bad: Multiple round trips
def extract(mysql_conn):
    # Fetch main records
    filings = mysql_conn.execute_query("SELECT * FROM filing_return")

    for filing in filings:
        # Fetch related data (N+1 problem!)
        tax_type = mysql_conn.execute_query(
            f"SELECT * FROM tax_type WHERE code = '{filing['tax_type_code']}'"
        )

# ✅ Good: Single query with JOINs
def extract(mysql_conn):
    query = """
        SELECT
            fr.*,
            tt.tax_type_name,
            p.party_name
        FROM filing_return fr
        INNER JOIN tax_type tt ON fr.tax_type_code = tt.tax_type_code
        INNER JOIN party p ON fr.party_id = p.party_id
    """
    return mysql_conn.execute_query(query)
```

### Optimize Dimension Lookups

```python
# ✅ Good: Load once, use many times
def transform(source_data, mysql_conn, ch_conn):
    # Load all lookups once (in-memory cache)
    dim_lookups = load_dimension_lookups(ch_conn)

    for row in source_data:
        # Fast dictionary lookup (O(1))
        party_key = dim_lookups['party'].get(row['party_id'], -1)
```

### Batch Size Tuning

```python
# For large datasets, increase batch size
BATCH_SIZE = 5000  # Larger batches = fewer network round trips

def load_in_batches(ch_conn, data, batch_size=BATCH_SIZE):
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        ch_conn.insert(table, batch, columns)
```

### Index Recommendations

```sql
-- MySQL L2: Add indexes on frequently joined columns
CREATE INDEX idx_filing_party ON filing_return(party_id);
CREATE INDEX idx_filing_date ON filing_return(filing_date);
CREATE INDEX idx_filing_type ON filing_return(tax_type_code);

-- ClickHouse L3: Optimize ORDER BY
-- Already optimized in table creation
-- ORDER BY (dim_date_key, dim_party_key, dim_tax_type_key)
```

---

## Best Practices

### 1. Follow Naming Conventions

```python
# ✅ Good: Clear, descriptive names
def extract_filing_data(mysql_conn) -> List[Dict]:
    pass

def transform_to_fact_filing(source_data) -> List[Tuple]:
    pass

# ❌ Bad: Vague names
def get_data(conn):
    pass

def process(data):
    pass
```

### 2. Add Comprehensive Logging

```python
# ✅ Good: Informative logging at key points
logger.info("Starting ETL")
logger.info(f"Extracted {len(data)} records")
logger.info(f"Transformation complete: {len(transformed)} records")
logger.error(f"ETL failed: {e}")

# ❌ Bad: No logging or excessive logging
# No logs at all
# or
logger.debug(f"Processing row {i}")  # Every row logged
```

### 3. Handle NULL Values Explicitly

```python
# ✅ Good: Explicit NULL handling
declared_amount = row['declared_amount'] if row['declared_amount'] is not None else 0
penalty = row.get('penalty_amount', 0) or 0

# ❌ Bad: Assuming no NULLs
declared_amount = row['declared_amount']  # May cause TypeError
```

### 4. Validate Prerequisites

```python
# ✅ Good: Check dimensions loaded before loading facts
def main():
    with get_clickhouse_connection() as ch:
        # Check dim_party exists and has data
        party_count = ch.get_table_row_count('dim_party', 'ta_dw')
        if party_count == 0:
            raise ValueError("dim_party must be loaded before fact tables")

        # Proceed with ETL
        ...
```

### 5. Use Type Hints

```python
# ✅ Good: Type hints for clarity
def extract(mysql_conn) -> List[Dict]:
    pass

def transform(source_data: List[Dict], ch_conn) -> List[Tuple]:
    pass

def load(ch_conn, data: List[Tuple]) -> None:
    pass
```

---

## Common Pitfalls

### 1. Forgetting to Update Column List

```python
# ❌ Pitfall: Added column to transform but not to column_names
transformed_row = (key1, key2, value1, value2, new_value)  # 5 elements

column_names = ['key1', 'key2', 'value1', 'value2']  # Only 4 names!
# Result: Insert fails with "column count mismatch"

# ✅ Solution: Always update both
transformed_row = (key1, key2, value1, value2, new_value)
column_names = ['key1', 'key2', 'value1', 'value2', 'new_value']
```

### 2. Not Handling Lookup Failures

```python
# ❌ Pitfall: Assuming lookup always succeeds
party_key = dim_lookups['party'][row['party_id']]  # KeyError if not found!

# ✅ Solution: Use .get() with default
party_key = dim_lookups['party'].get(row['party_id'], -1)
```

### 3. Wrong Date Format

```python
# ❌ Pitfall: String dates in wrong format
date_key = row['filing_date'].replace('-', '')  # '2025-01-06' → '20250106'
# But what if date is already datetime object?

# ✅ Solution: Handle both formats
def convert_date_to_key(date) -> int:
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d').date()
    return int(date.strftime('%Y%m%d'))
```

### 4. Not Testing with Edge Cases

```python
# ✅ Test with:
# - NULL values
# - Zero amounts
# - Missing dimension records
# - Empty result sets
# - Very large numbers
# - Special characters in strings
```

### 5. Hardcoding Values

```python
# ❌ Bad: Hardcoded database name
ch_conn.truncate_table('fact_filing', 'ta_dw')

# ✅ Good: Use configuration
from config.clickhouse_config import CLICKHOUSE_CONFIG
database = CLICKHOUSE_CONFIG['database']
ch_conn.truncate_table('fact_filing', database)
```

---

## Getting Help

### Resources

1. **Documentation**:
   - [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture
   - [DESIGN.md](DESIGN.md) - Design decisions
   - [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues

2. **Code Examples**:
   - Review existing ETL scripts in `etl/` directory
   - Similar patterns across all fact table ETLs

3. **Testing**:
   - Run validation: `python etl/validate_etl.py`
   - Check data: Query ClickHouse directly

### Development Workflow

```
1. Understand requirement
2. Review existing similar ETL
3. Create/modify schema (if needed)
4. Implement ETL script
5. Test with small dataset
6. Add validation rules
7. Run full validation
8. Update pipeline script
9. Document changes
10. Commit to version control
```

---

## Conclusion

This guide provides practical examples for common development tasks. Remember:

✅ **Follow existing patterns** for consistency
✅ **Test thoroughly** before deploying
✅ **Add comprehensive logging** for debugging
✅ **Handle edge cases** (NULLs, missing lookups, etc.)
✅ **Document your changes** for future developers

Happy coding! 🚀

---

**Document Version**: 1.0.0
**Last Updated**: 2025-11-06
**Maintained By**: Development Team
