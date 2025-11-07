#!/usr/bin/env python3
"""
L2 → L3 ETL: Tax Type Dimension

Extracts tax types from MySQL tax_framework.tax_type and loads into
ClickHouse dim_tax_type dimension table.

Phase B: Simple load, no SCD Type 2 tracking yet.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any
import clickhouse_connect

from utils.db_utils import get_db_connection
from config.clickhouse_config import CLICKHOUSE_CONFIG


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def extract_tax_types_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract tax type data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of tax type dictionaries
    """
    logger.info("Extracting tax types from L2 MySQL...")

    query = """
        SELECT
            tax_type_id,
            tax_type_code,
            tax_type_name,
            tax_category,
            is_withholding_tax,
            default_filing_frequency,
            applies_to_individuals,
            applies_to_enterprises,
            penalty_rate_default,
            interest_rate_default,
            valid_from,
            valid_to,
            is_current,
            is_active,
            created_by,
            created_date,
            modified_by,
            modified_date
        FROM tax_framework.tax_type
        WHERE is_active = TRUE
        ORDER BY sort_order
    """

    rows = mysql_conn.fetch_all(query)

    tax_types = []
    for row in rows:
        tax_type = {
            'tax_type_id': row[0],
            'tax_type_code': row[1],
            'tax_type_name': row[2],
            'tax_category': row[3],
            'is_withholding_tax': row[4],
            'filing_frequency': row[5],
            'applies_individuals': row[6],
            'applies_enterprises': row[7],
            'penalty_rate': row[8],
            'interest_rate': row[9],
            'valid_from': row[10],
            'valid_to': row[11],
            'is_current': row[12],
            'is_active': row[13],
            'created_by': row[14],
            'created_date': row[15],
            'modified_by': row[16],
            'modified_date': row[17]
        }
        tax_types.append(tax_type)

    logger.info(f"  Extracted {len(tax_types)} tax type(s) from L2")
    return tax_types


def transform_tax_types(tax_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform tax type data for L3 dimension.

    Args:
        tax_types: Raw tax type data from L2

    Returns:
        Transformed tax type data for L3
    """
    logger.info("Transforming tax types for L3...")

    transformed = []
    for idx, tax_type in enumerate(tax_types, start=1):
        # Determine if direct or indirect tax
        is_direct = 1 if tax_type['tax_category'] == 'INCOME' else 0

        # Determine rate type (simplified for Phase B)
        rate_type = 'PROGRESSIVE' if tax_type['tax_type_code'] in ['PIT', 'CIT'] else 'FLAT'

        # Priority level (income taxes = high, others = medium)
        priority = 'HIGH' if tax_type['tax_category'] == 'INCOME' else 'MEDIUM'

        # Revenue category (simplified)
        revenue_category = f"{tax_type['tax_category']}_TAX"

        transformed_tax_type = {
            'tax_type_key': idx,  # Surrogate key = sequence
            'tax_type_code': tax_type['tax_type_code'],
            'tax_type_name': tax_type['tax_type_name'],
            'tax_category': tax_type['tax_category'],
            'is_direct_tax': is_direct,
            'filing_frequency': tax_type['filing_frequency'] or 'ANNUAL',
            'standard_rate': None,  # Phase B: No rates yet
            'rate_type': rate_type,
            'revenue_category': revenue_category,
            'priority_level': priority,
            'valid_from': datetime.combine(tax_type['valid_from'], datetime.min.time()),
            'valid_to': None,  # All current for Phase B
            'is_current': 1,
            'created_by': 'ETL_SYSTEM',
            'created_date': datetime.now(),
            'updated_by': None,
            'updated_date': None,
            'etl_batch_id': 1  # Phase B: Simple batch ID
        }
        transformed.append(transformed_tax_type)

    logger.info(f"  Transformed {len(transformed)} tax type(s)")
    return transformed


def load_to_clickhouse(tax_types: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load tax types into ClickHouse dim_tax_type.

    Args:
        tax_types: Transformed tax type data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading tax types to ClickHouse dim_tax_type...")

    if not tax_types:
        logger.warning("  No tax types to load")
        return 0

    # Truncate existing data (Phase B: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.dim_tax_type")
    logger.info("  Truncated dim_tax_type")

    # Prepare data for bulk insert
    data = [
        [
            tt['tax_type_key'],
            tt['tax_type_code'],
            tt['tax_type_name'],
            tt['tax_category'],
            tt['is_direct_tax'],
            tt['filing_frequency'],
            tt['standard_rate'],
            tt['rate_type'],
            tt['revenue_category'],
            tt['priority_level'],
            tt['valid_from'],
            tt['valid_to'],
            tt['is_current'],
            tt['created_by'],
            tt['created_date'],
            tt['updated_by'],
            tt['updated_date'],
            tt['etl_batch_id']
        ]
        for tt in tax_types
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.dim_tax_type',
        data,
        column_names=[
            'tax_type_key', 'tax_type_code', 'tax_type_name', 'tax_category',
            'is_direct_tax', 'filing_frequency', 'standard_rate', 'rate_type',
            'revenue_category', 'priority_level', 'valid_from', 'valid_to',
            'is_current', 'created_by', 'created_date', 'updated_by',
            'updated_date', 'etl_batch_id'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to dim_tax_type")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in dim_tax_type.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying dim_tax_type data...")

    result = clickhouse_client.query("""
        SELECT
            tax_type_key,
            tax_type_code,
            tax_type_name,
            tax_category,
            is_direct_tax,
            filing_frequency,
            priority_level
        FROM ta_dw.dim_tax_type
        ORDER BY tax_type_key
    """)

    print("\n" + "=" * 80)
    print("dim_tax_type: Loaded Tax Types")
    print("=" * 80)
    print(f"{'Key':<5} {'Code':<6} {'Name':<30} {'Category':<15} {'Direct':<7} {'Filing':<10} {'Priority':<10}")
    print("-" * 80)

    for row in result.result_rows:
        tax_type_key, code, name, category, is_direct, filing, priority = row
        print(f"{tax_type_key:<5} {code:<6} {name:<30} {category:<15} "
              f"{'Yes' if is_direct else 'No':<7} {filing:<10} {priority:<10}")

    print("=" * 80)
    print(f"Total: {len(result.result_rows)} tax type(s)")
    print("=" * 80 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 60)
    logger.info("Starting L2 → L3 Tax Type ETL (Phase B)")
    logger.info("=" * 60)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # ETL Process
            tax_types = extract_tax_types_from_l2(mysql_conn)
            transformed = transform_tax_types(tax_types)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 60)
            logger.info("Tax Type ETL Completed Successfully")
            logger.info(f"  Extracted: {len(tax_types)} tax types")
            logger.info(f"  Loaded: {loaded_count} tax types")
            logger.info("=" * 60)

            return 0

    except Exception as e:
        logger.error(f"Tax Type ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
