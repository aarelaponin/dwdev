#!/usr/bin/env python3
"""
L2 → L3 ETL: Filing Fact Table

Extracts tax return filing data from MySQL filing_assessment.tax_return and loads into
ClickHouse fact_filing fact table.

Phase C: Filing & Assessment - loads 69 tax returns with dimension lookups.
"""

import logging
from datetime import datetime, date
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


def extract_filings_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract tax return filing data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of filing dictionaries
    """
    logger.info("Extracting tax return filings from L2 MySQL...")

    query = """
        SELECT
            tr.tax_return_id,
            tr.return_number,
            tr.tax_account_id,
            tr.tax_period_id,
            tr.filing_date,
            tr.filing_due_date,
            tr.is_filing_late,
            tr.days_late,
            tr.return_status_code,
            tr.is_amended,
            tr.filing_method_code,
            ta.party_id,
            tp.tax_type_id,
            tt.tax_type_code,
            tp.period_start_date,
            tp.period_end_date,
            COALESCE(SUM(trl.line_value_numeric), 0) as total_declared_amount,
            a.assessment_date
        FROM filing_assessment.tax_return tr
        INNER JOIN registration.tax_account ta ON tr.tax_account_id = ta.tax_account_id
        INNER JOIN tax_framework.tax_period tp ON tr.tax_period_id = tp.tax_period_id
        INNER JOIN tax_framework.tax_type tt ON tp.tax_type_id = tt.tax_type_id
        LEFT JOIN filing_assessment.tax_return_line trl ON tr.tax_return_id = trl.tax_return_id
        LEFT JOIN filing_assessment.assessment a ON tr.tax_return_id = a.tax_return_id
        WHERE tr.is_current_version = 1
        GROUP BY tr.tax_return_id, tr.return_number, tr.tax_account_id, tr.tax_period_id,
                 tr.filing_date, tr.filing_due_date, tr.is_filing_late, tr.days_late,
                 tr.return_status_code, tr.is_amended, tr.filing_method_code,
                 ta.party_id, tp.tax_type_id, tt.tax_type_code,
                 tp.period_start_date, tp.period_end_date, a.assessment_date
        ORDER BY tr.filing_date, tr.tax_return_id
    """

    rows = mysql_conn.fetch_all(query)

    filings = []
    for row in rows:
        filing = {
            'tax_return_id': row[0],
            'return_number': row[1],
            'tax_account_id': row[2],
            'tax_period_id': row[3],
            'filing_date': row[4],
            'filing_due_date': row[5],
            'is_filing_late': row[6],
            'days_late': row[7] if row[7] is not None else 0,
            'return_status': row[8],
            'is_amended': row[9],
            'filing_method': row[10],
            'party_id': row[11],
            'tax_type_id': row[12],
            'tax_type_code': row[13],
            'period_start_date': row[14],
            'period_end_date': row[15],
            'declared_amount': row[16],
            'assessment_date': row[17]
        }
        filings.append(filing)

    logger.info(f"  Extracted {len(filings)} filing(s) from L2")
    return filings


def lookup_dimension_keys(clickhouse_client) -> Dict[str, Dict]:
    """
    Build lookup dictionaries for dimension surrogate keys.

    Args:
        clickhouse_client: ClickHouse client connection

    Returns:
        Dictionary of lookup maps
    """
    logger.info("Building dimension key lookups...")

    lookups = {}

    # Lookup dim_party keys by party_id
    party_result = clickhouse_client.query("""
        SELECT party_id, party_key
        FROM ta_dw.dim_party
        WHERE is_current = 1
    """)
    lookups['party'] = {row[0]: row[1] for row in party_result.result_rows}
    logger.info(f"  Loaded {len(lookups['party'])} party key(s)")

    # Lookup dim_tax_type keys by tax_type_code
    tax_type_result = clickhouse_client.query("""
        SELECT tax_type_code, tax_type_key
        FROM ta_dw.dim_tax_type
    """)
    lookups['tax_type'] = {row[0]: row[1] for row in tax_type_result.result_rows}
    logger.info(f"  Loaded {len(lookups['tax_type'])} tax type key(s)")

    # Lookup dim_tax_period keys by tax_period_id
    tax_period_result = clickhouse_client.query("""
        SELECT tax_period_id, period_key
        FROM ta_dw.dim_tax_period
    """)
    lookups['tax_period'] = {row[0]: row[1] for row in tax_period_result.result_rows}
    logger.info(f"  Loaded {len(lookups['tax_period'])} tax period key(s)")

    # Lookup dim_time keys by date_key (YYYYMMDD format)
    time_result = clickhouse_client.query("""
        SELECT date_key, date_key
        FROM ta_dw.dim_time
    """)
    lookups['time'] = {row[0]: row[1] for row in time_result.result_rows}
    logger.info(f"  Loaded {len(lookups['time'])} time key(s)")

    return lookups


def transform_filings(filings: List[Dict[str, Any]], lookups: Dict) -> List[Dict[str, Any]]:
    """
    Transform filing data for fact table.
    Performs dimension key lookups and calculates measures.

    Args:
        filings: Raw filing data from L2
        lookups: Dimension key lookup dictionaries

    Returns:
        Transformed filing facts
    """
    logger.info("Transforming filings for fact table...")

    transformed = []
    skipped = 0

    for filing in filings:
        # Look up dimension keys
        party_key = lookups['party'].get(filing['party_id'])
        if not party_key:
            logger.warning(f"  Party ID {filing['party_id']} not found in dim_party, skipping")
            skipped += 1
            continue

        tax_type_key = lookups['tax_type'].get(filing['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Tax type {filing['tax_type_code']} not found in dim_tax_type, skipping")
            skipped += 1
            continue

        tax_period_key = lookups['tax_period'].get(filing['tax_period_id'])
        if not tax_period_key:
            logger.warning(f"  Tax period ID {filing['tax_period_id']} not found in dim_tax_period, skipping")
            skipped += 1
            continue

        # Convert filing_date to date_key format (YYYYMMDD)
        filing_date = filing['filing_date']
        if isinstance(filing_date, datetime):
            filing_date = filing_date.date()
        date_key = int(filing_date.strftime('%Y%m%d'))

        if date_key not in lookups['time']:
            logger.warning(f"  Date {filing_date} not found in dim_time, skipping")
            skipped += 1
            continue

        # Calculate processing days if assessment completed
        processing_days = None
        if filing['assessment_date']:
            assessment_date = filing['assessment_date']
            if isinstance(assessment_date, datetime):
                assessment_date = assessment_date.date()
            processing_days = (assessment_date - filing_date).days

        # Determine flags
        is_late = 1 if filing['is_filing_late'] else 0
        is_amended = 1 if filing['is_amended'] else 0
        is_electronic = 1 if filing['filing_method'] in ('E_PORTAL', 'WEB_SERVICE') else 0
        is_nil_return = 1 if filing['declared_amount'] == 0 else 0
        is_auto_assessed = 1 if filing['return_status'] == 'ACCEPTED' else 0

        # Phase C: Placeholder values for dimensions not yet fully implemented
        dim_tax_account_key = 0  # Placeholder
        dim_location_key = 0  # Placeholder
        dim_org_unit_key = 1  # Default office

        # Tax base amount (Phase C: assume 10x the tax amount for simplicity)
        tax_base_amount = filing['declared_amount'] * 10 if filing['declared_amount'] > 0 else 0

        transformed_filing = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_period_key': tax_period_key,
            'dim_tax_account_key': dim_tax_account_key,
            'dim_location_key': dim_location_key,
            'dim_org_unit_key': dim_org_unit_key,
            'dim_date_key': date_key,
            'return_id': filing['tax_return_id'],
            'return_number': filing['return_number'],
            'filing_count': 1,
            'days_late': filing['days_late'],
            'processing_days': processing_days,
            'declared_amount': filing['declared_amount'],
            'tax_base_amount': tax_base_amount,
            'is_amended': is_amended,
            'is_late': is_late,
            'is_nil_return': is_nil_return,
            'is_electronic': is_electronic,
            'is_auto_assessed': is_auto_assessed,
            'filing_date': filing_date,
            'due_date': filing['filing_due_date'],
            'period_start_date': filing['period_start_date'],
            'period_end_date': filing['period_end_date'],
            'assessment_date': filing['assessment_date'] if filing['assessment_date'] else None,
            'filing_status': filing['return_status'],
            'etl_batch_id': 1
        }
        transformed.append(transformed_filing)

    logger.info(f"  Transformed {len(transformed)} filing(s), skipped {skipped}")
    return transformed


def load_to_clickhouse(filings: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load filings into ClickHouse fact_filing.

    Args:
        filings: Transformed filing data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading filings to ClickHouse fact_filing...")

    if not filings:
        logger.warning("  No filings to load")
        return 0

    # Truncate existing data (Phase C: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_filing")
    logger.info("  Truncated fact_filing")

    # Prepare data for bulk insert
    data = [
        [
            f['dim_party_key'],
            f['dim_tax_type_key'],
            f['dim_tax_period_key'],
            f['dim_tax_account_key'],
            f['dim_location_key'],
            f['dim_org_unit_key'],
            f['dim_date_key'],
            f['return_id'],
            f['return_number'],
            f['filing_count'],
            f['days_late'],
            f['processing_days'],
            f['declared_amount'],
            f['tax_base_amount'],
            f['is_amended'],
            f['is_late'],
            f['is_nil_return'],
            f['is_electronic'],
            f['is_auto_assessed'],
            f['filing_date'],
            f['due_date'],
            f['period_start_date'],
            f['period_end_date'],
            f['assessment_date'],
            f['filing_status'],
            f['etl_batch_id']
        ]
        for f in filings
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_filing',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_period_key',
            'dim_tax_account_key', 'dim_location_key', 'dim_org_unit_key',
            'dim_date_key', 'return_id', 'return_number', 'filing_count',
            'days_late', 'processing_days', 'declared_amount', 'tax_base_amount',
            'is_amended', 'is_late', 'is_nil_return', 'is_electronic',
            'is_auto_assessed', 'filing_date', 'due_date', 'period_start_date',
            'period_end_date', 'assessment_date', 'filing_status', 'etl_batch_id'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_filing")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_filing.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_filing data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as total_filings,
            SUM(filing_count) as filing_count,
            SUM(declared_amount) as total_declared,
            SUM(is_late) as late_filings,
            SUM(is_amended) as amended_returns,
            SUM(is_nil_return) as nil_returns,
            SUM(is_electronic) as electronic_filings,
            AVG(days_late) as avg_days_late
        FROM ta_dw.fact_filing
    """)

    print("\n" + "=" * 100)
    print("fact_filing: Overall Summary")
    print("=" * 100)

    for row in summary.result_rows:
        total, filing_count, declared, late, amended, nil, electronic, avg_late = row
        print(f"Total Filings:        {total}")
        print(f"Filing Count:         {filing_count}")
        print(f"Total Declared:       ${declared:,.2f}")
        print(f"Late Filings:         {late} ({late/total*100:.1f}%)")
        print(f"Amended Returns:      {amended}")
        print(f"Nil Returns:          {nil} ({nil/total*100:.1f}%)")
        print(f"Electronic Filings:   {electronic} ({electronic/total*100:.1f}%)")
        print(f"Avg Days Late:        {avg_late:.1f}")

    print("=" * 100)

    # Summary by tax type
    by_tax_type = clickhouse_client.query("""
        SELECT
            tt.tax_type_code,
            COUNT(*) as filing_count,
            SUM(f.declared_amount) as total_declared,
            SUM(f.is_late) as late_count,
            AVG(f.days_late) as avg_days_late
        FROM ta_dw.fact_filing f
        JOIN ta_dw.dim_tax_type tt ON f.dim_tax_type_key = tt.tax_type_key
        GROUP BY tt.tax_type_code
        ORDER BY tt.tax_type_code
    """)

    print("\nSummary by Tax Type:")
    print("-" * 100)
    print(f"{'Tax Type':<10} {'Count':<8} {'Declared':<15} {'Late':<8} {'Avg Days Late':<15}")
    print("-" * 100)

    for row in by_tax_type.result_rows:
        tax_type, count, declared, late, avg_late = row
        print(f"{tax_type:<10} {count:<8} ${declared:>13,.2f} {late:<8} {avg_late:>13.1f}")

    print("=" * 100)

    # Sample recent filings
    sample = clickhouse_client.query("""
        SELECT
            f.return_number,
            tt.tax_type_code,
            f.filing_date,
            f.declared_amount,
            f.is_late,
            f.days_late,
            f.filing_status
        FROM ta_dw.fact_filing f
        JOIN ta_dw.dim_tax_type tt ON f.dim_tax_type_key = tt.tax_type_key
        ORDER BY f.filing_date DESC
        LIMIT 10
    """)

    print("\nSample Filings (Most Recent 10):")
    print("-" * 100)
    print(f"{'Return #':<20} {'Type':<6} {'Filed':<12} {'Amount':<15} {'Late?':<6} {'Days':<6} {'Status':<12}")
    print("-" * 100)

    for row in sample.result_rows:
        ret_num, tax_type, filed, amount, is_late, days_late, status = row
        late_flag = 'Yes' if is_late else 'No'
        print(f"{ret_num:<20} {tax_type:<6} {str(filed):<12} ${amount:>13,.2f} {late_flag:<6} {days_late:<6} {status:<12}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Filing Fact ETL (Phase C)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            filings = extract_filings_from_l2(mysql_conn)
            transformed = transform_filings(filings, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Filing Fact ETL Completed Successfully")
            logger.info(f"  Extracted: {len(filings)} filings")
            logger.info(f"  Transformed: {len(transformed)} filings")
            logger.info(f"  Loaded: {loaded_count} filings")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Filing Fact ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
