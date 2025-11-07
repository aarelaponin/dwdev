#!/usr/bin/env python3
"""
L2 → L3 ETL: Payment Fact

Extracts payments from MySQL payment_refund.payment and loads into
ClickHouse fact_payment fact table.

Phase D: Payment & Refund - loads ~80 payments with allocation details.
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


def extract_payments_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract payment data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of payment dictionaries
    """
    logger.info("Extracting payments from L2 MySQL...")

    query = """
        SELECT
            p.payment_id,
            p.payment_reference_number,
            p.tax_account_id,
            p.party_id,
            p.payment_type_code,
            p.payment_location_code,
            p.payment_date,
            p.received_date,
            p.payment_amount,
            p.bank_id,
            p.payment_status_code,
            p.is_allocated,
            p.allocated_date,
            p.is_suspended,
            p.is_reconciled,
            p.receipt_number,
            p.created_by,
            p.created_date,
            ta.tax_type_code,
            (SELECT COUNT(*)
             FROM payment_refund.payment_allocation pa
             WHERE pa.payment_id = p.payment_id) as allocation_count,
            (SELECT IFNULL(SUM(pa2.allocated_amount), 0)
             FROM payment_refund.payment_allocation pa2
             WHERE pa2.payment_id = p.payment_id) as allocated_amount
        FROM payment_refund.payment p
        INNER JOIN registration.tax_account ta ON p.tax_account_id = ta.tax_account_id
        ORDER BY p.payment_date
    """

    rows = mysql_conn.fetch_all(query)

    payments = []
    for row in rows:
        payment = {
            'payment_id': row[0],
            'payment_reference_number': row[1],
            'tax_account_id': row[2],
            'party_id': row[3],
            'payment_type_code': row[4],
            'payment_location_code': row[5],
            'payment_date': row[6],
            'received_date': row[7],
            'payment_amount': row[8],
            'bank_id': row[9],
            'payment_status_code': row[10],
            'is_allocated': row[11],
            'allocated_date': row[12],
            'is_suspended': row[13],
            'is_reconciled': row[14],
            'receipt_number': row[15],
            'created_by': row[16],
            'created_date': row[17],
            'tax_type_code': row[18],
            'allocation_count': row[19],
            'allocated_amount': row[20]
        }
        payments.append(payment)

    logger.info(f"  Extracted {len(payments)} payment(s) from L2")
    return payments


def transform_payments(payments: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform payment data for L3 fact table.

    Args:
        payments: Raw payment data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed payment data for L3
    """
    logger.info("Transforming payments for L3...")

    transformed = []
    for payment in payments:
        # Get dimension keys
        party_key = lookups['party'].get(payment['party_id'])
        if not party_key:
            logger.warning(f"  Skipping payment {payment['payment_id']}: party {payment['party_id']} not found in dim_party")
            continue

        # Get tax_type_key from tax_type_code
        tax_type_key = lookups['tax_type_code'].get(payment['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Skipping payment {payment['payment_id']}: tax_type_code {payment['tax_type_code']} not found")
            continue

        # Get date_key
        date_key = lookups['date'].get(payment['payment_date'])
        if not date_key:
            logger.warning(f"  Skipping payment {payment['payment_id']}: payment_date {payment['payment_date']} not found in dim_time")
            continue

        # Calculate unallocated amount
        unallocated_amount = payment['payment_amount'] - payment['allocated_amount']

        # Determine flags
        is_allocated = 1 if payment['is_allocated'] else 0
        is_partial_payment = 1 if payment['allocation_count'] > 1 else 0
        is_overpayment = 1 if unallocated_amount > 0 else 0
        is_electronic = 1 if payment['payment_type_code'] in ('E_PORTAL', 'DIRECT_DEBIT', 'BANK_TRANSFER') else 0
        is_verified = 1 if payment['is_reconciled'] else 0
        is_reversed = 0  # Phase D: no reversals

        transformed_payment = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_account_key': payment['tax_account_id'],  # Phase D: use natural key
            'dim_payment_method_key': 1,  # Phase D: default value
            'dim_location_key': 1,  # Phase D: default value
            'dim_org_unit_key': 1,  # Phase D: default value
            'dim_date_key': date_key,
            'payment_id': payment['payment_id'],
            'payment_reference': payment['payment_reference_number'],
            'transaction_reference': None,  # Not available in L2
            'payment_amount': payment['payment_amount'],
            'allocated_amount': payment['allocated_amount'],
            'unallocated_amount': unallocated_amount,
            'bank_charges': 0,  # Not available in L2
            'payment_count': 1,
            'allocation_count': payment['allocation_count'],
            'is_allocated': is_allocated,
            'is_partial_payment': is_partial_payment,
            'is_overpayment': is_overpayment,
            'is_electronic': is_electronic,
            'is_verified': is_verified,
            'is_reversed': is_reversed,
            'payment_date': payment['payment_date'],
            'received_date': payment['received_date'],
            'allocation_date': payment['allocated_date'] if payment['allocated_date'] else None,
            'verification_date': None,  # Not available in L2
            'payment_status': payment['payment_status_code'],
            'etl_batch_id': 1,  # Phase D: Simple batch ID
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_payment)

    logger.info(f"  Transformed {len(transformed)} payment(s)")
    return transformed


def lookup_dimension_keys(clickhouse_client) -> Dict[str, Dict]:
    """
    Build lookup dictionaries for dimension keys.

    Args:
        clickhouse_client: ClickHouse client connection

    Returns:
        Dictionary of lookup dictionaries
    """
    logger.info("Building dimension lookups...")

    lookups = {}

    # Party keys
    party_result = clickhouse_client.query("SELECT party_id, party_key FROM ta_dw.dim_party WHERE is_current = 1")
    lookups['party'] = {row[0]: row[1] for row in party_result.result_rows}
    logger.info(f"  Loaded {len(lookups['party'])} party lookups")

    # Tax type keys
    tax_type_result = clickhouse_client.query("SELECT tax_type_code, tax_type_key FROM ta_dw.dim_tax_type")
    lookups['tax_type_code'] = {row[0]: row[1] for row in tax_type_result.result_rows}
    logger.info(f"  Loaded {len(lookups['tax_type_code'])} tax type lookups")

    # Date keys
    date_result = clickhouse_client.query("SELECT full_date, date_key FROM ta_dw.dim_time")
    lookups['date'] = {row[0]: row[1] for row in date_result.result_rows}
    logger.info(f"  Loaded {len(lookups['date'])} date lookups")

    return lookups


def load_to_clickhouse(payments: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load payments into ClickHouse fact_payment.

    Args:
        payments: Transformed payment data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading payments to ClickHouse fact_payment...")

    if not payments:
        logger.warning("  No payments to load")
        return 0

    # Truncate existing data (Phase D: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_payment")
    logger.info("  Truncated fact_payment")

    # Prepare data for bulk insert
    data = [
        [
            p['dim_party_key'],
            p['dim_tax_type_key'],
            p['dim_tax_account_key'],
            p['dim_payment_method_key'],
            p['dim_location_key'],
            p['dim_org_unit_key'],
            p['dim_date_key'],
            p['payment_id'],
            p['payment_reference'],
            p['transaction_reference'],
            p['payment_amount'],
            p['allocated_amount'],
            p['unallocated_amount'],
            p['bank_charges'],
            p['payment_count'],
            p['allocation_count'],
            p['is_allocated'],
            p['is_partial_payment'],
            p['is_overpayment'],
            p['is_electronic'],
            p['is_verified'],
            p['is_reversed'],
            p['payment_date'],
            p['received_date'],
            p['allocation_date'],
            p['verification_date'],
            p['payment_status'],
            p['etl_batch_id'],
            p['etl_timestamp']
        ]
        for p in payments
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_payment',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_account_key',
            'dim_payment_method_key', 'dim_location_key', 'dim_org_unit_key',
            'dim_date_key', 'payment_id', 'payment_reference', 'transaction_reference',
            'payment_amount', 'allocated_amount', 'unallocated_amount', 'bank_charges',
            'payment_count', 'allocation_count', 'is_allocated', 'is_partial_payment',
            'is_overpayment', 'is_electronic', 'is_verified', 'is_reversed',
            'payment_date', 'received_date', 'allocation_date', 'verification_date',
            'payment_status', 'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_payment")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_payment.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_payment data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as payment_count,
            SUM(payment_amount) as total_amount,
            SUM(allocated_amount) as allocated_total,
            SUM(unallocated_amount) as unallocated_total,
            SUM(is_allocated) as fully_allocated_count,
            SUM(is_overpayment) as overpayment_count,
            MIN(payment_date) as earliest_payment,
            MAX(payment_date) as latest_payment
        FROM ta_dw.fact_payment
    """)

    row = summary.result_rows[0]
    print("\n" + "=" * 100)
    print("fact_payment: Overall Summary")
    print("=" * 100)
    print(f"Total Payments:          {row[0]}")
    print(f"Total Amount:            ${row[1]:,.2f}")
    print(f"Allocated Amount:        ${row[2]:,.2f}")
    print(f"Unallocated Amount:      ${row[3]:,.2f}")
    print(f"Fully Allocated:         {row[4]}")
    print(f"Overpayments:            {row[5]}")
    print(f"Date Range:              {row[6]} to {row[7]}")
    print("=" * 100)

    # By payment status
    by_status = clickhouse_client.query("""
        SELECT
            payment_status,
            COUNT(*) as payment_count,
            SUM(payment_amount) as total_amount
        FROM ta_dw.fact_payment
        GROUP BY payment_status
        ORDER BY payment_count DESC
    """)

    print("\nPayments by Status:")
    print("-" * 100)
    print(f"{'Status':<20} {'Count':<10} {'Total Amount':<20}")
    print("-" * 100)

    for row in by_status.result_rows:
        status, count, amount = row
        print(f"{status:<20} {count:<10} ${amount:>18,.2f}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Payment ETL (Phase D)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            payments = extract_payments_from_l2(mysql_conn)
            transformed = transform_payments(payments, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Payment ETL Completed Successfully")
            logger.info(f"  Extracted: {len(payments)} payments")
            logger.info(f"  Loaded: {loaded_count} payments")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Payment ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
