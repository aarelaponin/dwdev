#!/usr/bin/env python3
"""
L2 → L3 ETL: Tax Period Dimension

Extracts tax periods from MySQL tax_framework.tax_period and loads into
ClickHouse dim_tax_period dimension table.

Phase C: Filing & Assessment - loads 90 tax periods (2023-2025) for VAT, WHT, ESL, CIT, PIT.
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


def extract_tax_periods_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract tax period data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of tax period dictionaries
    """
    logger.info("Extracting tax periods from L2 MySQL...")

    query = """
        SELECT
            tp.tax_period_id,
            tp.period_code,
            tp.tax_type_id,
            tt.tax_type_code,
            tp.period_year,
            tp.period_month,
            tp.period_number,
            tp.period_start_date,
            tp.period_end_date,
            tp.filing_due_date,
            tp.payment_due_date,
            tp.period_status,
            tp.created_by,
            tp.created_date
        FROM tax_framework.tax_period tp
        INNER JOIN tax_framework.tax_type tt ON tp.tax_type_id = tt.tax_type_id
        ORDER BY tp.period_start_date, tt.tax_type_code
    """

    rows = mysql_conn.fetch_all(query)

    periods = []
    for row in rows:
        period = {
            'tax_period_id': row[0],
            'period_code': row[1],
            'tax_type_id': row[2],
            'tax_type_code': row[3],
            'period_year': row[4],
            'period_month': row[5],
            'period_number': row[6],
            'period_start_date': row[7],
            'period_end_date': row[8],
            'filing_due_date': row[9],
            'payment_due_date': row[10],
            'period_status': row[11],
            'created_by': row[12],
            'created_date': row[13]
        }
        periods.append(period)

    logger.info(f"  Extracted {len(periods)} tax period(s) from L2")
    return periods


def transform_tax_periods(periods: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform tax period data for L3 dimension.

    Args:
        periods: Raw tax period data from L2

    Returns:
        Transformed tax period data for L3
    """
    logger.info("Transforming tax periods for L3...")

    transformed = []
    for idx, period in enumerate(periods, start=1):
        # Determine if period is closed (status = 'CLOSED' or 'LOCKED')
        is_closed = 1 if period['period_status'] in ('CLOSED', 'LOCKED') else 0

        # Determine if this is the current period (status = 'OPEN')
        is_current = 1 if period['period_status'] == 'OPEN' else 0

        # Derive period_quarter from period_month if available
        # For quarterly periods, period_number is 1-4
        # For monthly periods, derive quarter from month
        period_quarter = None
        if period['period_month'] is not None:
            # Monthly period - calculate quarter
            period_quarter = ((period['period_month'] - 1) // 3) + 1
        elif period['period_number'] in (1, 2, 3, 4):
            # Likely quarterly period - use period_number as quarter
            period_quarter = period['period_number']

        transformed_period = {
            'period_key': idx,  # Surrogate key = sequence
            'tax_period_id': period['tax_period_id'],
            'period_code': period['period_code'],
            'tax_type_code': period['tax_type_code'],
            'period_year': period['period_year'],
            'period_quarter': period_quarter,
            'period_month': period['period_month'],
            'period_start_date': period['period_start_date'],
            'period_end_date': period['period_end_date'],
            'filing_due_date': period['filing_due_date'],
            'payment_due_date': period['payment_due_date'],
            'is_current_period': is_current,
            'is_closed': is_closed,
            'created_by': 'ETL_SYSTEM',
            'created_date': datetime.now(),
            'etl_batch_id': 1  # Phase C: Simple batch ID
        }
        transformed.append(transformed_period)

    logger.info(f"  Transformed {len(transformed)} tax period(s)")
    return transformed


def load_to_clickhouse(periods: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load tax periods into ClickHouse dim_tax_period.

    Args:
        periods: Transformed tax period data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading tax periods to ClickHouse dim_tax_period...")

    if not periods:
        logger.warning("  No tax periods to load")
        return 0

    # Truncate existing data (Phase C: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.dim_tax_period")
    logger.info("  Truncated dim_tax_period")

    # Prepare data for bulk insert
    data = [
        [
            p['period_key'],
            p['tax_period_id'],
            p['period_code'],
            p['tax_type_code'],
            p['period_year'],
            p['period_quarter'],
            p['period_month'],
            p['period_start_date'],
            p['period_end_date'],
            p['filing_due_date'],
            p['payment_due_date'],
            p['is_current_period'],
            p['is_closed'],
            p['created_by'],
            p['created_date'],
            p['etl_batch_id']
        ]
        for p in periods
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.dim_tax_period',
        data,
        column_names=[
            'period_key', 'tax_period_id', 'period_code', 'tax_type_code',
            'period_year', 'period_quarter', 'period_month', 'period_start_date',
            'period_end_date', 'filing_due_date', 'payment_due_date',
            'is_current_period', 'is_closed', 'created_by', 'created_date',
            'etl_batch_id'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to dim_tax_period")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in dim_tax_period.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying dim_tax_period data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            tax_type_code,
            COUNT(*) as period_count,
            MIN(period_start_date) as earliest_period,
            MAX(period_end_date) as latest_period,
            SUM(is_closed) as closed_periods,
            SUM(is_current_period) as current_periods
        FROM ta_dw.dim_tax_period
        GROUP BY tax_type_code
        ORDER BY tax_type_code
    """)

    print("\n" + "=" * 100)
    print("dim_tax_period: Summary by Tax Type")
    print("=" * 100)
    print(f"{'Tax Type':<10} {'Count':<8} {'Earliest':<12} {'Latest':<12} {'Closed':<8} {'Current':<8}")
    print("-" * 100)

    total_periods = 0
    for row in summary.result_rows:
        tax_type, count, earliest, latest, closed, current = row
        total_periods += count
        print(f"{tax_type:<10} {count:<8} {str(earliest):<12} {str(latest):<12} {closed:<8} {current:<8}")

    print("-" * 100)
    print(f"{'TOTAL':<10} {total_periods:<8}")
    print("=" * 100)

    # Sample periods
    sample = clickhouse_client.query("""
        SELECT
            period_key,
            period_code,
            tax_type_code,
            period_start_date,
            period_end_date,
            filing_due_date,
            CASE WHEN is_closed = 1 THEN 'CLOSED' ELSE 'OPEN' END as status
        FROM ta_dw.dim_tax_period
        ORDER BY period_start_date DESC
        LIMIT 10
    """)

    print("\nSample Periods (Most Recent 10):")
    print("-" * 100)
    print(f"{'Key':<5} {'Code':<15} {'Type':<6} {'Start':<12} {'End':<12} {'Due':<12} {'Status':<8}")
    print("-" * 100)

    for row in sample.result_rows:
        period_key, code, tax_type, start, end, due, status = row
        print(f"{period_key:<5} {code:<15} {tax_type:<6} {str(start):<12} {str(end):<12} {str(due):<12} {status:<8}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Tax Period ETL (Phase C)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # ETL Process
            periods = extract_tax_periods_from_l2(mysql_conn)
            transformed = transform_tax_periods(periods)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Tax Period ETL Completed Successfully")
            logger.info(f"  Extracted: {len(periods)} periods")
            logger.info(f"  Loaded: {loaded_count} periods")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Tax Period ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
