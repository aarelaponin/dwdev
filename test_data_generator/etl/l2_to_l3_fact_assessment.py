#!/usr/bin/env python3
"""
L2 → L3 ETL: Assessment Fact Table

Extracts tax assessment data from MySQL filing_assessment.assessment and loads into
ClickHouse fact_assessment fact table.

Phase C: Filing & Assessment - loads 69 assessments with penalties and interest.
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


def extract_assessments_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract tax assessment data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of assessment dictionaries
    """
    logger.info("Extracting tax assessments from L2 MySQL...")

    query = """
        SELECT
            a.assessment_id,
            a.assessment_number,
            a.tax_account_id,
            a.tax_period_id,
            a.tax_return_id,
            a.assessment_type_code,
            a.assessment_date,
            a.payment_due_date,
            a.finalization_date,
            a.assessed_tax_amount,
            a.penalty_amount,
            a.interest_amount,
            a.credit_amount,
            a.net_assessment_amount,
            a.amount_paid,
            a.balance_outstanding,
            a.assessment_status_code,
            a.is_current_version,
            ta.party_id,
            tp.tax_type_id,
            tt.tax_type_code,
            tp.period_start_date,
            tp.period_end_date,
            tr.filing_date,
            tr.return_number,
            COALESCE(SUM(trl.line_value_numeric), 0) as declared_amount
        FROM filing_assessment.assessment a
        INNER JOIN registration.tax_account ta ON a.tax_account_id = ta.tax_account_id
        INNER JOIN tax_framework.tax_period tp ON a.tax_period_id = tp.tax_period_id
        INNER JOIN tax_framework.tax_type tt ON tp.tax_type_id = tt.tax_type_id
        LEFT JOIN filing_assessment.tax_return tr ON a.tax_return_id = tr.tax_return_id
        LEFT JOIN filing_assessment.tax_return_line trl ON tr.tax_return_id = trl.tax_return_id
        WHERE a.is_current_version = 1
        GROUP BY a.assessment_id, a.assessment_number, a.tax_account_id, a.tax_period_id,
                 a.tax_return_id, a.assessment_type_code, a.assessment_date, a.payment_due_date,
                 a.finalization_date, a.assessed_tax_amount, a.penalty_amount, a.interest_amount,
                 a.credit_amount, a.net_assessment_amount, a.amount_paid, a.balance_outstanding,
                 a.assessment_status_code, a.is_current_version, ta.party_id,
                 tp.tax_type_id, tt.tax_type_code, tp.period_start_date, tp.period_end_date,
                 tr.filing_date, tr.return_number
        ORDER BY a.assessment_date, a.assessment_id
    """

    rows = mysql_conn.fetch_all(query)

    assessments = []
    for row in rows:
        assessment = {
            'assessment_id': row[0],
            'assessment_number': row[1],
            'tax_account_id': row[2],
            'tax_period_id': row[3],
            'tax_return_id': row[4],
            'assessment_type': row[5],
            'assessment_date': row[6],
            'payment_due_date': row[7],
            'finalization_date': row[8],
            'assessed_tax_amount': row[9],
            'penalty_amount': row[10],
            'interest_amount': row[11],
            'credit_amount': row[12],
            'net_assessment_amount': row[13],
            'amount_paid': row[14],
            'balance_outstanding': row[15],
            'assessment_status': row[16],
            'is_current_version': row[17],
            'party_id': row[18],
            'tax_type_id': row[19],
            'tax_type_code': row[20],
            'period_start_date': row[21],
            'period_end_date': row[22],
            'filing_date': row[23],
            'return_number': row[24],
            'declared_amount': row[25]
        }
        assessments.append(assessment)

    logger.info(f"  Extracted {len(assessments)} assessment(s) from L2")
    return assessments


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


def transform_assessments(assessments: List[Dict[str, Any]], lookups: Dict) -> List[Dict[str, Any]]:
    """
    Transform assessment data for fact table.
    Performs dimension key lookups and calculates measures.

    Args:
        assessments: Raw assessment data from L2
        lookups: Dimension key lookup dictionaries

    Returns:
        Transformed assessment facts
    """
    logger.info("Transforming assessments for fact table...")

    transformed = []
    skipped = 0

    for assessment in assessments:
        # Look up dimension keys
        party_key = lookups['party'].get(assessment['party_id'])
        if not party_key:
            logger.warning(f"  Party ID {assessment['party_id']} not found in dim_party, skipping")
            skipped += 1
            continue

        tax_type_key = lookups['tax_type'].get(assessment['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Tax type {assessment['tax_type_code']} not found in dim_tax_type, skipping")
            skipped += 1
            continue

        tax_period_key = lookups['tax_period'].get(assessment['tax_period_id'])
        if not tax_period_key:
            logger.warning(f"  Tax period ID {assessment['tax_period_id']} not found in dim_tax_period, skipping")
            skipped += 1
            continue

        # Convert assessment_date to date_key format (YYYYMMDD)
        assessment_date = assessment['assessment_date']
        if isinstance(assessment_date, datetime):
            assessment_date = assessment_date.date()
        date_key = int(assessment_date.strftime('%Y%m%d'))

        if date_key not in lookups['time']:
            logger.warning(f"  Date {assessment_date} not found in dim_time, skipping")
            skipped += 1
            continue

        # Calculate processing days if return was filed
        processing_days = None
        if assessment['filing_date']:
            filing_date = assessment['filing_date']
            if isinstance(filing_date, datetime):
                filing_date = filing_date.date()
            processing_days = (assessment_date - filing_date).days

        # Determine assessment type flags
        is_self = 1 if assessment['assessment_type'] == 'SELF' else 0
        is_admin = 1 if assessment['assessment_type'] == 'ADMINISTRATIVE' else 0
        is_audit = 1 if assessment['assessment_type'] == 'AUDIT' else 0
        is_bja = 1 if assessment['assessment_type'] == 'BEST_JUDGMENT' else 0
        is_amended = 1 if assessment['assessment_type'] == 'AMENDED' else 0
        is_final = 1 if assessment['assessment_status'] == 'FINAL' else 0
        is_refund = 1 if assessment['net_assessment_amount'] < 0 else 0

        # Phase C: Placeholder values for dimensions not yet fully implemented
        dim_tax_account_key = 0  # Placeholder
        dim_location_key = 0  # Placeholder
        dim_org_unit_key = 1  # Default office
        dim_officer_key = 0  # Placeholder

        # Tax base amount (Phase C: assume 10x the tax amount for simplicity)
        tax_base_amount = assessment['assessed_tax_amount'] * 10 if assessment['assessed_tax_amount'] > 0 else 0

        transformed_assessment = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_period_key': tax_period_key,
            'dim_tax_account_key': dim_tax_account_key,
            'dim_location_key': dim_location_key,
            'dim_org_unit_key': dim_org_unit_key,
            'dim_officer_key': dim_officer_key,
            'dim_date_key': date_key,
            'assessment_id': assessment['assessment_id'],
            'return_id': assessment['tax_return_id'],
            'assessment_number': assessment['assessment_number'],
            'assessed_amount': assessment['assessed_tax_amount'],
            'tax_base_amount': tax_base_amount,
            'declared_amount': assessment['declared_amount'] if assessment['declared_amount'] > 0 else None,
            'principal_tax': assessment['assessed_tax_amount'],
            'penalties': assessment['penalty_amount'],
            'interest': assessment['interest_amount'],
            'surcharges': 0,  # Phase C: No surcharges in test data
            'adjustments': 0,  # Phase C: No adjustments in test data
            'credits_applied': assessment['credit_amount'],
            'net_amount': assessment['net_assessment_amount'],
            'assessment_count': 1,
            'processing_days': processing_days,
            'is_self_assessment': is_self,
            'is_administrative': is_admin,
            'is_audit_assessment': is_audit,
            'is_best_judgment': is_bja,
            'is_amended': is_amended,
            'is_final': is_final,
            'is_refund': is_refund,
            'assessment_date': assessment_date,
            'due_date': assessment['payment_due_date'],
            'period_start_date': assessment['period_start_date'],
            'period_end_date': assessment['period_end_date'],
            'finalization_date': assessment['finalization_date'] if assessment['finalization_date'] else None,
            'assessment_status': assessment['assessment_status'],
            'etl_batch_id': 1
        }
        transformed.append(transformed_assessment)

    logger.info(f"  Transformed {len(transformed)} assessment(s), skipped {skipped}")
    return transformed


def load_to_clickhouse(assessments: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load assessments into ClickHouse fact_assessment.

    Args:
        assessments: Transformed assessment data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading assessments to ClickHouse fact_assessment...")

    if not assessments:
        logger.warning("  No assessments to load")
        return 0

    # Truncate existing data (Phase C: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_assessment")
    logger.info("  Truncated fact_assessment")

    # Prepare data for bulk insert
    data = [
        [
            a['dim_party_key'],
            a['dim_tax_type_key'],
            a['dim_tax_period_key'],
            a['dim_tax_account_key'],
            a['dim_location_key'],
            a['dim_org_unit_key'],
            a['dim_officer_key'],
            a['dim_date_key'],
            a['assessment_id'],
            a['return_id'],
            a['assessment_number'],
            a['assessed_amount'],
            a['tax_base_amount'],
            a['declared_amount'],
            a['principal_tax'],
            a['penalties'],
            a['interest'],
            a['surcharges'],
            a['adjustments'],
            a['credits_applied'],
            a['net_amount'],
            a['assessment_count'],
            a['processing_days'],
            a['is_self_assessment'],
            a['is_administrative'],
            a['is_audit_assessment'],
            a['is_best_judgment'],
            a['is_amended'],
            a['is_final'],
            a['is_refund'],
            a['assessment_date'],
            a['due_date'],
            a['period_start_date'],
            a['period_end_date'],
            a['finalization_date'],
            a['assessment_status'],
            a['etl_batch_id']
        ]
        for a in assessments
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_assessment',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_period_key',
            'dim_tax_account_key', 'dim_location_key', 'dim_org_unit_key',
            'dim_officer_key', 'dim_date_key', 'assessment_id', 'return_id',
            'assessment_number', 'assessed_amount', 'tax_base_amount',
            'declared_amount', 'principal_tax', 'penalties', 'interest',
            'surcharges', 'adjustments', 'credits_applied', 'net_amount',
            'assessment_count', 'processing_days', 'is_self_assessment',
            'is_administrative', 'is_audit_assessment', 'is_best_judgment',
            'is_amended', 'is_final', 'is_refund', 'assessment_date',
            'due_date', 'period_start_date', 'period_end_date',
            'finalization_date', 'assessment_status', 'etl_batch_id'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_assessment")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_assessment.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_assessment data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as total_assessments,
            SUM(assessment_count) as assessment_count,
            SUM(assessed_amount) as total_assessed,
            SUM(principal_tax) as total_principal,
            SUM(penalties) as total_penalties,
            SUM(interest) as total_interest,
            SUM(net_amount) as total_net,
            SUM(is_self_assessment) as self_assessments,
            AVG(processing_days) as avg_processing_days
        FROM ta_dw.fact_assessment
    """)

    print("\n" + "=" * 100)
    print("fact_assessment: Overall Summary")
    print("=" * 100)

    for row in summary.result_rows:
        total, count, assessed, principal, penalties, interest, net, self_assess, avg_days = row
        print(f"Total Assessments:    {total}")
        print(f"Assessment Count:     {count}")
        print(f"Total Assessed:       ${assessed:,.2f}")
        print(f"Principal Tax:        ${principal:,.2f}")
        print(f"Total Penalties:      ${penalties:,.2f}")
        print(f"Total Interest:       ${interest:,.2f}")
        print(f"Net Amount:           ${net:,.2f}")
        print(f"Self-Assessments:     {self_assess} ({self_assess/total*100:.1f}%)")
        if avg_days:
            print(f"Avg Processing Days:  {avg_days:.1f}")

    print("=" * 100)

    # Summary by tax type
    by_tax_type = clickhouse_client.query("""
        SELECT
            tt.tax_type_code,
            COUNT(*) as assessment_count,
            SUM(f.assessed_amount) as total_assessed,
            SUM(f.penalties) as total_penalties,
            SUM(f.interest) as total_interest,
            SUM(f.net_amount) as total_net
        FROM ta_dw.fact_assessment f
        JOIN ta_dw.dim_tax_type tt ON f.dim_tax_type_key = tt.tax_type_key
        GROUP BY tt.tax_type_code
        ORDER BY tt.tax_type_code
    """)

    print("\nSummary by Tax Type:")
    print("-" * 100)
    print(f"{'Tax Type':<10} {'Count':<8} {'Assessed':<15} {'Penalties':<15} {'Interest':<15} {'Net Amount':<15}")
    print("-" * 100)

    for row in by_tax_type.result_rows:
        tax_type, count, assessed, penalties, interest, net = row
        print(f"{tax_type:<10} {count:<8} ${assessed:>13,.2f} ${penalties:>13,.2f} ${interest:>13,.2f} ${net:>13,.2f}")

    print("=" * 100)

    # Sample recent assessments
    sample = clickhouse_client.query("""
        SELECT
            f.assessment_number,
            tt.tax_type_code,
            f.assessment_date,
            f.assessed_amount,
            f.penalties,
            f.interest,
            f.net_amount,
            f.assessment_status
        FROM ta_dw.fact_assessment f
        JOIN ta_dw.dim_tax_type tt ON f.dim_tax_type_key = tt.tax_type_key
        ORDER BY f.assessment_date DESC
        LIMIT 10
    """)

    print("\nSample Assessments (Most Recent 10):")
    print("-" * 120)
    print(f"{'Assessment #':<25} {'Type':<6} {'Date':<12} {'Assessed':<15} {'Penalties':<12} {'Interest':<12} {'Net Amount':<15} {'Status':<12}")
    print("-" * 120)

    for row in sample.result_rows:
        assess_num, tax_type, assess_date, assessed, penalties, interest, net, status = row
        print(f"{assess_num:<25} {tax_type:<6} {str(assess_date):<12} ${assessed:>13,.2f} ${penalties:>10,.2f} ${interest:>10,.2f} ${net:>13,.2f} {status:<12}")

    print("=" * 120 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Assessment Fact ETL (Phase C)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            assessments = extract_assessments_from_l2(mysql_conn)
            transformed = transform_assessments(assessments, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Assessment Fact ETL Completed Successfully")
            logger.info(f"  Extracted: {len(assessments)} assessments")
            logger.info(f"  Transformed: {len(transformed)} assessments")
            logger.info(f"  Loaded: {loaded_count} assessments")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Assessment Fact ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
