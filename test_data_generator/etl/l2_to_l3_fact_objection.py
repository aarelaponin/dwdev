#!/usr/bin/env python3
"""
L2 → L3 ETL: Objection Fact

Extracts objection records from MySQL compliance_control schema and loads into
ClickHouse fact_objection fact table.

Phase H: Objections & Appeals - loads objection cases from disputed assessments/audits.
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


def extract_objections_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract objection data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of objection dictionaries
    """
    logger.info("Extracting objections from L2 MySQL...")

    query = """
        SELECT
            oc.objection_case_id,
            oc.case_number,
            oc.party_id,
            oc.tax_type_code,
            oc.objection_type_code,
            oc.objection_subject_type,
            oc.assessment_id,
            oc.audit_case_id,
            oc.case_status_code,
            oc.priority_code,
            oc.assigned_officer_id,
            oc.filing_date,
            oc.assignment_date,
            oc.amount_disputed,
            oc.amount_at_stake_tax,
            oc.amount_at_stake_penalty,
            oc.amount_at_stake_interest,
            oc.decision_date,
            oc.decision_outcome_code,
            oc.amount_adjusted,
            oc.appeal_filed,
            oc.hearing_scheduled_date,
            oc.hearing_held_date,
            oc.closure_date,
            CASE
                WHEN oc.decision_outcome_code IS NOT NULL THEN 1
                ELSE 0
            END as is_resolved,
            CASE
                WHEN oc.decision_outcome_code IN ('FULLY_ACCEPTED', 'PARTIALLY_ACCEPTED') THEN 1
                ELSE 0
            END as is_upheld,
            CASE
                WHEN oc.decision_outcome_code = 'PARTIALLY_ACCEPTED' THEN 1
                ELSE 0
            END as is_reduced,
            CASE
                WHEN oc.decision_outcome_code = 'REJECTED' THEN 1
                ELSE 0
            END as is_cancelled,
            CASE
                WHEN oc.appeal_filed = TRUE THEN 1
                ELSE 0
            END as is_appealed_further,
            DATEDIFF(oc.decision_date, oc.filing_date) as resolution_days
        FROM compliance_control.objection_case oc
        ORDER BY oc.filing_date
    """

    rows = mysql_conn.fetch_all(query)

    objections = []
    for row in rows:
        objection = {
            'objection_case_id': row[0],
            'case_number': row[1],
            'party_id': row[2],
            'tax_type_code': row[3],
            'objection_type_code': row[4],
            'objection_subject_type': row[5],
            'assessment_id': row[6],
            'audit_case_id': row[7],
            'case_status_code': row[8],
            'priority_code': row[9],
            'assigned_officer_id': row[10],
            'filing_date': row[11],
            'assignment_date': row[12],
            'amount_disputed': row[13],
            'amount_at_stake_tax': row[14],
            'amount_at_stake_penalty': row[15],
            'amount_at_stake_interest': row[16],
            'decision_date': row[17],
            'decision_outcome_code': row[18],
            'amount_adjusted': row[19],
            'appeal_filed': row[20],
            'hearing_scheduled_date': row[21],
            'hearing_held_date': row[22],
            'closure_date': row[23],
            'is_resolved': row[24],
            'is_upheld': row[25],
            'is_reduced': row[26],
            'is_cancelled': row[27],
            'is_appealed_further': row[28],
            'resolution_days': row[29]
        }
        objections.append(objection)

    logger.info(f"  Extracted {len(objections)} objection(s) from L2")
    return objections


def transform_objections(objections: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform objection data for L3 fact table.

    Args:
        objections: Raw objection data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed objection data for L3
    """
    logger.info("Transforming objections for L3...")

    transformed = []
    for objection in objections:
        # Get dimension keys
        party_key = lookups['party'].get(objection['party_id'])
        if not party_key:
            logger.warning(f"  Skipping objection {objection['objection_case_id']}: party {objection['party_id']} not found")
            continue

        tax_type_key = lookups['tax_type_code'].get(objection['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Skipping objection {objection['objection_case_id']}: tax_type_code {objection['tax_type_code']} not found")
            continue

        # Date key: use filing_date
        date_key = lookups['date'].get(objection['filing_date'])
        if not date_key:
            logger.warning(f"  Skipping objection {objection['objection_case_id']}: filing_date {objection['filing_date']} not found")
            continue

        # Calculate resolved amount (amount_adjusted)
        resolved_amount = objection['amount_adjusted'] if objection['amount_adjusted'] else 0

        # Taxpayer favor amount is the amount adjusted in their favor
        taxpayer_favor_amount = resolved_amount

        transformed_objection = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_org_unit_key': 1,  # Phase H: default
            'dim_officer_key': objection['assigned_officer_id'] if objection['assigned_officer_id'] else 1,
            'dim_date_key': date_key,
            'objection_id': objection['objection_case_id'],
            'objection_number': objection['case_number'],
            'assessment_id': objection['assessment_id'],
            'disputed_amount': objection['amount_disputed'],
            'resolved_amount': resolved_amount,
            'taxpayer_favor_amount': taxpayer_favor_amount,
            'objection_count': 1,
            'resolution_days': objection['resolution_days'] if objection['resolution_days'] is not None else None,
            'is_resolved': objection['is_resolved'],
            'is_upheld': objection['is_upheld'],
            'is_reduced': objection['is_reduced'],
            'is_cancelled': objection['is_cancelled'],
            'is_appealed_further': objection['is_appealed_further'],
            'filing_date': objection['filing_date'],
            'hearing_date': objection['hearing_held_date'],  # Use actual held date
            'decision_date': objection['decision_date'],
            'closure_date': objection['closure_date'],
            'objection_status': objection['case_status_code'],
            'objection_level': 'INITIAL_OBJECTION',  # Phase H: first level before appeals
            'decision_outcome': objection['decision_outcome_code'],
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_objection)

    logger.info(f"  Transformed {len(transformed)} objection(s)")
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


def load_to_clickhouse(objections: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load objections into ClickHouse fact_objection.

    Args:
        objections: Transformed objection data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading objections to ClickHouse fact_objection...")

    if not objections:
        logger.warning("  No objections to load")
        return 0

    # Truncate existing data (Phase H: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_objection")
    logger.info("  Truncated fact_objection")

    # Prepare data for bulk insert
    data = [
        [
            o['dim_party_key'],
            o['dim_tax_type_key'],
            o['dim_org_unit_key'],
            o['dim_officer_key'],
            o['dim_date_key'],
            o['objection_id'],
            o['objection_number'],
            o['assessment_id'],
            o['disputed_amount'],
            o['resolved_amount'],
            o['taxpayer_favor_amount'],
            o['objection_count'],
            o['resolution_days'],
            o['is_resolved'],
            o['is_upheld'],
            o['is_reduced'],
            o['is_cancelled'],
            o['is_appealed_further'],
            o['filing_date'],
            o['hearing_date'],
            o['decision_date'],
            o['closure_date'],
            o['objection_status'],
            o['objection_level'],
            o['decision_outcome'],
            o['etl_batch_id'],
            o['etl_timestamp']
        ]
        for o in objections
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_objection',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_org_unit_key',
            'dim_officer_key', 'dim_date_key', 'objection_id',
            'objection_number', 'assessment_id', 'disputed_amount',
            'resolved_amount', 'taxpayer_favor_amount', 'objection_count',
            'resolution_days', 'is_resolved', 'is_upheld', 'is_reduced',
            'is_cancelled', 'is_appealed_further', 'filing_date',
            'hearing_date', 'decision_date', 'closure_date',
            'objection_status', 'objection_level', 'decision_outcome',
            'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_objection")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_objection.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_objection data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as objection_count,
            SUM(disputed_amount) as total_disputed,
            SUM(resolved_amount) as total_resolved,
            SUM(taxpayer_favor_amount) as total_favor,
            AVG(resolution_days) as avg_resolution_days,
            SUM(is_resolved) as resolved_count,
            SUM(is_upheld) as upheld_count,
            SUM(is_reduced) as reduced_count,
            SUM(is_cancelled) as cancelled_count,
            SUM(is_appealed_further) as appealed_count,
            MIN(filing_date) as earliest_filing,
            MAX(decision_date) as latest_decision
        FROM ta_dw.fact_objection
    """)

    row = summary.result_rows[0]
    avg_resolution_days = row[4] if row[4] is not None else 0.0

    print("\n" + "=" * 100)
    print("fact_objection: Overall Summary")
    print("=" * 100)
    print(f"Total Objections:        {row[0]}")
    print(f"Total Disputed:          ${row[1]:,.2f}")
    print(f"Total Resolved:          ${row[2]:,.2f}")
    print(f"Total Favor Amount:      ${row[3]:,.2f}")
    print(f"Avg Resolution Days:     {avg_resolution_days:.1f}")
    print(f"Resolved:                {row[5]}")
    print(f"Upheld:                  {row[6]}")
    print(f"Reduced:                 {row[7]}")
    print(f"Cancelled (Rejected):    {row[8]}")
    print(f"Appealed Further:        {row[9]}")
    print(f"Filing Date Range:       {row[10]} to {row[11] if row[11] else 'N/A'}")
    print("=" * 100)

    # By status
    by_status = clickhouse_client.query("""
        SELECT
            objection_status,
            COUNT(*) as objection_count,
            SUM(disputed_amount) as total_amount
        FROM ta_dw.fact_objection
        GROUP BY objection_status
        ORDER BY objection_count DESC
    """)

    print("\nObjections by Status:")
    print("-" * 100)
    print(f"{'Status':<30} {'Count':<10} {'Total Amount':<20}")
    print("-" * 100)

    for row in by_status.result_rows:
        status, count, amount = row
        print(f"{status:<30} {count:<10} ${amount:>18,.2f}")

    # By decision outcome
    by_outcome = clickhouse_client.query("""
        SELECT
            decision_outcome,
            COUNT(*) as objection_count,
            SUM(disputed_amount) as total_disputed,
            SUM(resolved_amount) as total_resolved
        FROM ta_dw.fact_objection
        WHERE decision_outcome IS NOT NULL
        GROUP BY decision_outcome
        ORDER BY objection_count DESC
    """)

    if by_outcome.result_rows:
        print("\nObjections by Decision Outcome:")
        print("-" * 100)
        print(f"{'Outcome':<30} {'Count':<10} {'Disputed':<20} {'Resolved':<20}")
        print("-" * 100)

        for row in by_outcome.result_rows:
            outcome, count, disputed, resolved = row
            print(f"{outcome:<30} {count:<10} ${disputed:>18,.2f} ${resolved:>18,.2f}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Objection ETL (Phase H)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            objections = extract_objections_from_l2(mysql_conn)
            transformed = transform_objections(objections, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Objection ETL Completed Successfully")
            logger.info(f"  Extracted: {len(objections)} objections")
            logger.info(f"  Loaded: {loaded_count} objections")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Objection ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
