#!/usr/bin/env python3
"""
L2 → L3 ETL: Account Balance Fact

Extracts account balances from MySQL accounting.account_balance and loads into
ClickHouse fact_account_balance fact table.

Phase D: Accounting - loads ~220 account balances (per account/period/charge_type).
SIMPLIFIED: Uses period-end as snapshot date rather than monthly snapshots.
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


def extract_balances_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract account balance data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of account balance dictionaries
    """
    logger.info("Extracting account balances from L2 MySQL...")

    query = """
        SELECT
            ab.account_balance_id,
            ab.tax_account_id,
            ab.tax_period_id,
            ab.charge_type_code,
            ab.opening_balance,
            ab.debit_amount,
            ab.credit_amount,
            ab.closing_balance,
            ab.is_current,
            ab.last_updated_date,
            ta.party_id,
            ta.tax_type_code,
            ta.account_status_code,
            tp.period_end_date,
            YEAR(tp.period_end_date) as snapshot_year,
            MONTH(tp.period_end_date) as snapshot_month
        FROM accounting.account_balance ab
        INNER JOIN registration.tax_account ta ON ab.tax_account_id = ta.tax_account_id
        INNER JOIN tax_framework.tax_period tp ON ab.tax_period_id = tp.tax_period_id
        ORDER BY ab.tax_account_id, tp.period_end_date, ab.charge_type_code
    """

    rows = mysql_conn.fetch_all(query)

    balances = []
    for row in rows:
        balance = {
            'account_balance_id': row[0],
            'tax_account_id': row[1],
            'tax_period_id': row[2],
            'charge_type_code': row[3],
            'opening_balance': row[4],
            'debit_amount': row[5],
            'credit_amount': row[6],
            'closing_balance': row[7],
            'is_current': row[8],
            'last_updated_date': row[9],
            'party_id': row[10],
            'tax_type_code': row[11],
            'account_status_code': row[12],
            'snapshot_date': row[13],  # period_end_date
            'snapshot_year': row[14],
            'snapshot_month': row[15]
        }
        balances.append(balance)

    logger.info(f"  Extracted {len(balances)} account balance(s) from L2")
    return balances


def transform_balances(balances: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform account balance data for L3 fact table.

    Args:
        balances: Raw balance data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed balance data for L3
    """
    logger.info("Transforming account balances for L3...")

    transformed = []
    for balance in balances:
        # Get dimension keys
        party_key = lookups['party'].get(balance['party_id'])
        if not party_key:
            logger.warning(f"  Skipping balance {balance['account_balance_id']}: party {balance['party_id']} not found")
            continue

        tax_type_key = lookups['tax_type_code'].get(balance['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Skipping balance {balance['account_balance_id']}: tax_type_code {balance['tax_type_code']} not found")
            continue

        date_key = lookups['date'].get(balance['snapshot_date'])
        if not date_key:
            logger.warning(f"  Skipping balance {balance['account_balance_id']}: snapshot_date {balance['snapshot_date']} not found")
            continue

        # Calculate derived measures
        net_activity = balance['closing_balance'] - balance['opening_balance']

        # Determine flags
        has_arrears = 1 if balance['closing_balance'] > 0 else 0
        is_credit_balance = 1 if balance['closing_balance'] < 0 else 0
        is_zero_balance = 1 if balance['closing_balance'] == 0 else 0

        transformed_balance = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_account_key': balance['tax_account_id'],  # Phase D: use natural key
            'dim_location_key': 1,  # Phase D: default
            'dim_date_key': date_key,
            'snapshot_date': balance['snapshot_date'],
            'snapshot_year': balance['snapshot_year'],
            'snapshot_month': balance['snapshot_month'],
            'tax_account_id': balance['tax_account_id'],
            'opening_balance': balance['opening_balance'],
            'closing_balance': balance['closing_balance'],
            'assessed_amount': balance['debit_amount'],  # Debits = assessments
            'payment_received': balance['credit_amount'],  # Credits = payments
            'refund_paid': 0,  # Not tracked separately in Phase D
            'penalties_levied': 0,  # Would need breakdown by charge_type
            'interest_accrued': 0,  # Would need breakdown by charge_type
            'adjustments': 0,  # Not tracked in Phase D
            'net_activity': net_activity,
            'arrears_amount': balance['closing_balance'] if balance['closing_balance'] > 0 else 0,
            'current_amount': 0,  # Not tracked in Phase D
            'assessment_count': 0,  # Not available from account_balance
            'payment_count': 0,  # Not available from account_balance
            'has_arrears': has_arrears,
            'is_credit_balance': is_credit_balance,
            'is_zero_balance': is_zero_balance,
            'days_in_arrears': None,  # Not tracked in Phase D
            'account_status': balance['account_status_code'],
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_balance)

    logger.info(f"  Transformed {len(transformed)} account balance(s)")
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


def load_to_clickhouse(balances: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load account balances into ClickHouse fact_account_balance.

    Args:
        balances: Transformed balance data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading account balances to ClickHouse fact_account_balance...")

    if not balances:
        logger.warning("  No balances to load")
        return 0

    # Truncate existing data (Phase D: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_account_balance")
    logger.info("  Truncated fact_account_balance")

    # Prepare data for bulk insert
    data = [
        [
            b['dim_party_key'],
            b['dim_tax_type_key'],
            b['dim_tax_account_key'],
            b['dim_location_key'],
            b['dim_date_key'],
            b['snapshot_date'],
            b['snapshot_year'],
            b['snapshot_month'],
            b['tax_account_id'],
            b['opening_balance'],
            b['closing_balance'],
            b['assessed_amount'],
            b['payment_received'],
            b['refund_paid'],
            b['penalties_levied'],
            b['interest_accrued'],
            b['adjustments'],
            b['net_activity'],
            b['arrears_amount'],
            b['current_amount'],
            b['assessment_count'],
            b['payment_count'],
            b['has_arrears'],
            b['is_credit_balance'],
            b['is_zero_balance'],
            b['days_in_arrears'],
            b['account_status'],
            b['etl_batch_id'],
            b['etl_timestamp']
        ]
        for b in balances
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_account_balance',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_account_key',
            'dim_location_key', 'dim_date_key', 'snapshot_date',
            'snapshot_year', 'snapshot_month', 'tax_account_id',
            'opening_balance', 'closing_balance', 'assessed_amount',
            'payment_received', 'refund_paid', 'penalties_levied',
            'interest_accrued', 'adjustments', 'net_activity',
            'arrears_amount', 'current_amount', 'assessment_count',
            'payment_count', 'has_arrears', 'is_credit_balance',
            'is_zero_balance', 'days_in_arrears', 'account_status',
            'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_account_balance")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_account_balance.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_account_balance data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as balance_count,
            SUM(closing_balance) as total_balance,
            SUM(has_arrears) as accounts_in_arrears,
            SUM(is_credit_balance) as credit_balances,
            SUM(is_zero_balance) as zero_balances,
            MIN(snapshot_date) as earliest_snapshot,
            MAX(snapshot_date) as latest_snapshot
        FROM ta_dw.fact_account_balance
    """)

    row = summary.result_rows[0]
    print("\n" + "=" * 100)
    print("fact_account_balance: Overall Summary")
    print("=" * 100)
    print(f"Total Balance Records:   {row[0]}")
    print(f"Total Outstanding:       ${row[1]:,.2f}")
    print(f"Accounts in Arrears:     {row[2]}")
    print(f"Credit Balances:         {row[3]}")
    print(f"Zero Balances:           {row[4]}")
    print(f"Snapshot Range:          {row[5]} to {row[6]}")
    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Account Balance ETL (Phase D)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            balances = extract_balances_from_l2(mysql_conn)
            transformed = transform_balances(balances, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Account Balance ETL Completed Successfully")
            logger.info(f"  Extracted: {len(balances)} balances")
            logger.info(f"  Loaded: {loaded_count} balances")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Account Balance ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
