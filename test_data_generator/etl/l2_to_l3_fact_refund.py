#!/usr/bin/env python3
"""
L2 → L3 ETL: Refund Fact

Extracts refund records from MySQL payment_refund schema and loads into
ClickHouse fact_refund fact table.

Phase F: Refunds - loads ~9 refund records from overpayments.
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


def extract_refunds_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract refund data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of refund dictionaries
    """
    logger.info("Extracting refunds from L2 MySQL...")

    query = """
        SELECT
            r.refund_id,
            r.refund_number,
            r.tax_account_id,
            r.party_id,
            ta.tax_type_code,
            r.refund_type_code,
            r.refund_status_code,
            r.claim_date,
            r.gross_refund_amount,
            r.offset_amount,
            r.net_refund_amount,
            r.interest_amount,
            r.total_refund_amount,
            r.review_start_date,
            r.review_completed_date,
            r.approval_date,
            r.refund_date,
            CASE
                WHEN r.refund_status_code IN ('APPROVED', 'VOUCHER_PREPARED', 'DISBURSED') THEN 1
                ELSE 0
            END as is_approved,
            CASE
                WHEN r.refund_status_code = 'DISBURSED' THEN 1
                ELSE 0
            END as is_disbursed,
            CASE
                WHEN r.offset_amount > 0 THEN 1
                ELSE 0
            END as is_offset,
            CASE
                WHEN rv.refund_voucher_id IS NOT NULL THEN 1
                ELSE 0
            END as is_verified,
            CASE
                WHEN r.refund_bank_account IS NOT NULL THEN 1
                ELSE 0
            END as is_electronic,
            DATEDIFF(r.refund_date, r.claim_date) as processing_days,
            DATEDIFF(r.approval_date, r.claim_date) as approval_days
        FROM payment_refund.refund r
        INNER JOIN registration.tax_account ta ON r.tax_account_id = ta.tax_account_id
        LEFT JOIN payment_refund.refund_voucher rv ON r.refund_voucher_id = rv.refund_voucher_id
        ORDER BY r.claim_date
    """

    rows = mysql_conn.fetch_all(query)

    refunds = []
    for row in rows:
        refund = {
            'refund_id': row[0],
            'refund_number': row[1],
            'tax_account_id': row[2],
            'party_id': row[3],
            'tax_type_code': row[4],
            'refund_type_code': row[5],
            'refund_status_code': row[6],
            'claim_date': row[7],
            'gross_refund_amount': row[8],
            'offset_amount': row[9],
            'net_refund_amount': row[10],
            'interest_amount': row[11],
            'total_refund_amount': row[12],
            'review_start_date': row[13],
            'review_completed_date': row[14],
            'approval_date': row[15],
            'refund_date': row[16],
            'is_approved': row[17],
            'is_disbursed': row[18],
            'is_offset': row[19],
            'is_verified': row[20],
            'is_electronic': row[21],
            'processing_days': row[22],
            'approval_days': row[23]
        }
        refunds.append(refund)

    logger.info(f"  Extracted {len(refunds)} refund(s) from L2")
    return refunds


def transform_refunds(refunds: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform refund data for L3 fact table.

    Args:
        refunds: Raw refund data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed refund data for L3
    """
    logger.info("Transforming refunds for L3...")

    transformed = []
    for refund in refunds:
        # Get dimension keys
        party_key = lookups['party'].get(refund['party_id'])
        if not party_key:
            logger.warning(f"  Skipping refund {refund['refund_id']}: party {refund['party_id']} not found")
            continue

        tax_type_key = lookups['tax_type_code'].get(refund['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Skipping refund {refund['refund_id']}: tax_type_code {refund['tax_type_code']} not found")
            continue

        # Date key: use claim_date
        date_key = lookups['date'].get(refund['claim_date'])
        if not date_key:
            logger.warning(f"  Skipping refund {refund['refund_id']}: claim_date {refund['claim_date']} not found")
            continue

        transformed_refund = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_account_key': refund['tax_account_id'],  # Phase F: use natural key
            'dim_location_key': 1,  # Phase F: default
            'dim_org_unit_key': 1,  # Phase F: default
            'dim_officer_key': 1,  # Phase F: default
            'dim_date_key': date_key,
            'refund_id': refund['refund_id'],
            'refund_number': refund['refund_number'],
            'refund_amount': refund['gross_refund_amount'],
            'offset_amount': refund['offset_amount'],
            'net_refund_amount': refund['net_refund_amount'],
            'interest_paid': refund['interest_amount'],
            'refund_count': 1,
            'processing_days': refund['processing_days'] if refund['processing_days'] is not None else 0,
            'approval_days': refund['approval_days'] if refund['approval_days'] is not None else None,
            'is_verified': refund['is_verified'],
            'is_offset': refund['is_offset'],
            'is_electronic': refund['is_electronic'],
            'is_approved': refund['is_approved'],
            'is_disbursed': refund['is_disbursed'],
            'request_date': refund['claim_date'],
            'approval_date': refund['approval_date'],
            'disbursement_date': refund['refund_date'],
            'refund_status': refund['refund_status_code'],
            'refund_type': refund['refund_type_code'],
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_refund)

    logger.info(f"  Transformed {len(transformed)} refund(s)")
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


def load_to_clickhouse(refunds: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load refunds into ClickHouse fact_refund.

    Args:
        refunds: Transformed refund data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading refunds to ClickHouse fact_refund...")

    if not refunds:
        logger.warning("  No refunds to load")
        return 0

    # Truncate existing data (Phase F: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_refund")
    logger.info("  Truncated fact_refund")

    # Prepare data for bulk insert
    data = [
        [
            r['dim_party_key'],
            r['dim_tax_type_key'],
            r['dim_tax_account_key'],
            r['dim_location_key'],
            r['dim_org_unit_key'],
            r['dim_officer_key'],
            r['dim_date_key'],
            r['refund_id'],
            r['refund_number'],
            r['refund_amount'],
            r['offset_amount'],
            r['net_refund_amount'],
            r['interest_paid'],
            r['refund_count'],
            r['processing_days'],
            r['approval_days'],
            r['is_verified'],
            r['is_offset'],
            r['is_electronic'],
            r['is_approved'],
            r['is_disbursed'],
            r['request_date'],
            r['approval_date'],
            r['disbursement_date'],
            r['refund_status'],
            r['refund_type'],
            r['etl_batch_id'],
            r['etl_timestamp']
        ]
        for r in refunds
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_refund',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_account_key',
            'dim_location_key', 'dim_org_unit_key', 'dim_officer_key',
            'dim_date_key', 'refund_id', 'refund_number',
            'refund_amount', 'offset_amount', 'net_refund_amount',
            'interest_paid', 'refund_count', 'processing_days',
            'approval_days', 'is_verified', 'is_offset', 'is_electronic',
            'is_approved', 'is_disbursed', 'request_date',
            'approval_date', 'disbursement_date', 'refund_status',
            'refund_type', 'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_refund")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_refund.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_refund data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as refund_count,
            SUM(refund_amount) as total_refund_amount,
            SUM(offset_amount) as total_offset,
            SUM(net_refund_amount) as total_net_refund,
            SUM(interest_paid) as total_interest,
            AVG(processing_days) as avg_processing_days,
            AVG(approval_days) as avg_approval_days,
            SUM(is_approved) as approved_count,
            SUM(is_disbursed) as disbursed_count,
            MIN(request_date) as earliest_request,
            MAX(disbursement_date) as latest_disbursement
        FROM ta_dw.fact_refund
    """)

    row = summary.result_rows[0]
    avg_approval_days = row[6] if row[6] is not None else 0.0

    print("\n" + "=" * 100)
    print("fact_refund: Overall Summary")
    print("=" * 100)
    print(f"Total Refunds:           {row[0]}")
    print(f"Total Refund Amount:     ${row[1]:,.2f}")
    print(f"Total Offset:            ${row[2]:,.2f}")
    print(f"Total Net Refund:        ${row[3]:,.2f}")
    print(f"Total Interest Paid:     ${row[4]:,.2f}")
    print(f"Avg Processing Days:     {row[5]:.1f}")
    print(f"Avg Approval Days:       {avg_approval_days:.1f}")
    print(f"Approved:                {row[7]}")
    print(f"Disbursed:               {row[8]}")
    print(f"Request Date Range:      {row[9]} to {row[10] if row[10] else 'N/A'}")
    print("=" * 100)

    # By refund type
    by_type = clickhouse_client.query("""
        SELECT
            refund_type,
            COUNT(*) as refund_count,
            SUM(net_refund_amount) as total_amount
        FROM ta_dw.fact_refund
        GROUP BY refund_type
        ORDER BY refund_count DESC
    """)

    print("\nRefunds by Type:")
    print("-" * 100)
    print(f"{'Type':<30} {'Count':<10} {'Total Amount':<20}")
    print("-" * 100)

    for row in by_type.result_rows:
        refund_type, count, amount = row
        print(f"{refund_type:<30} {count:<10} ${amount:>18,.2f}")

    # By status
    by_status = clickhouse_client.query("""
        SELECT
            refund_status,
            COUNT(*) as refund_count,
            SUM(net_refund_amount) as total_amount
        FROM ta_dw.fact_refund
        GROUP BY refund_status
        ORDER BY refund_count DESC
    """)

    print("\nRefunds by Status:")
    print("-" * 100)
    print(f"{'Status':<30} {'Count':<10} {'Total Amount':<20}")
    print("-" * 100)

    for row in by_status.result_rows:
        status, count, amount = row
        print(f"{status:<30} {count:<10} ${amount:>18,.2f}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Refund ETL (Phase F)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            refunds = extract_refunds_from_l2(mysql_conn)
            transformed = transform_refunds(refunds, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Refund ETL Completed Successfully")
            logger.info(f"  Extracted: {len(refunds)} refunds")
            logger.info(f"  Loaded: {loaded_count} refunds")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Refund ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
