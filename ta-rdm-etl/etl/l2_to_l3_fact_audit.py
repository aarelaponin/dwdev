#!/usr/bin/env python3
"""
L2 → L3 ETL: Audit Fact

Extracts audit cases from MySQL compliance_control schema and loads into
ClickHouse fact_audit accumulating snapshot fact table.

Phase G: Audits - loads ~5 audit cases with lifecycle tracking.
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


def extract_audits_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract audit case data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of audit dictionaries
    """
    logger.info("Extracting audits from L2 MySQL...")

    query = """
        SELECT
            ac.audit_case_id,
            ac.case_number,
            ac.audit_plan_id,
            ac.party_id,
            ac.tax_account_id,
            ta.tax_type_code,
            ac.audit_type_code,
            ac.audit_scope_code,
            ac.selection_method_code,
            ac.risk_score,
            ac.audit_period_from,
            ac.audit_period_to,
            ac.case_status_code,
            ac.priority_code,
            ac.assignment_date,
            ac.notification_date,
            ac.planned_start_date,
            ac.actual_start_date,
            ac.planned_completion_date,
            ac.actual_completion_date,
            ac.audit_location_code,
            ac.estimated_hours,
            ac.actual_hours,
            ac.adjustment_amount,
            ac.penalty_amount,
            ac.interest_amount,
            ac.total_amount_assessed,
            ac.taxpayer_agreed,
            ac.objection_filed,
            COUNT(af.audit_finding_id) as findings_count,
            DATEDIFF(ac.actual_completion_date, ac.actual_start_date) as duration_days,
            DATEDIFF(ac.planned_completion_date, ac.planned_start_date) as planned_duration_days,
            YEAR(ac.audit_period_to) - YEAR(ac.audit_period_from) + 1 as tax_periods_covered,
            CASE
                WHEN ac.selection_method_code = 'RISK_BASED' THEN 1
                ELSE 0
            END as is_risk_based,
            CASE
                WHEN ac.case_status_code IN ('FINALIZED', 'CLOSED') THEN 1
                ELSE 0
            END as is_completed,
            CASE
                WHEN ac.adjustment_amount > 0 THEN 1
                ELSE 0
            END as has_findings,
            CASE
                WHEN ac.objection_filed = TRUE THEN 1
                ELSE 0
            END as is_appealed
        FROM compliance_control.audit_case ac
        INNER JOIN registration.tax_account ta ON ac.tax_account_id = ta.tax_account_id
        LEFT JOIN compliance_control.audit_finding af ON ac.audit_case_id = af.audit_case_id
        GROUP BY ac.audit_case_id, ac.case_number, ac.audit_plan_id, ac.party_id,
                 ac.tax_account_id, ta.tax_type_code, ac.audit_type_code, ac.audit_scope_code,
                 ac.selection_method_code, ac.risk_score, ac.audit_period_from,
                 ac.audit_period_to, ac.case_status_code, ac.priority_code,
                 ac.assignment_date, ac.notification_date, ac.planned_start_date,
                 ac.actual_start_date, ac.planned_completion_date, ac.actual_completion_date,
                 ac.audit_location_code, ac.estimated_hours, ac.actual_hours,
                 ac.adjustment_amount, ac.penalty_amount, ac.interest_amount,
                 ac.total_amount_assessed, ac.taxpayer_agreed, ac.objection_filed
        ORDER BY ac.assignment_date
    """

    rows = mysql_conn.fetch_all(query)

    audits = []
    for row in rows:
        audit = {
            'audit_case_id': row[0],
            'case_number': row[1],
            'audit_plan_id': row[2],
            'party_id': row[3],
            'tax_account_id': row[4],
            'tax_type_code': row[5],
            'audit_type_code': row[6],
            'audit_scope_code': row[7],
            'selection_method_code': row[8],
            'risk_score': row[9],
            'audit_period_from': row[10],
            'audit_period_to': row[11],
            'case_status_code': row[12],
            'priority_code': row[13],
            'assignment_date': row[14],
            'notification_date': row[15],
            'planned_start_date': row[16],
            'actual_start_date': row[17],
            'planned_completion_date': row[18],
            'actual_completion_date': row[19],
            'audit_location_code': row[20],
            'estimated_hours': row[21],
            'actual_hours': row[22],
            'adjustment_amount': row[23],
            'penalty_amount': row[24],
            'interest_amount': row[25],
            'total_amount_assessed': row[26],
            'taxpayer_agreed': row[27],
            'objection_filed': row[28],
            'findings_count': row[29],
            'duration_days': row[30],
            'planned_duration_days': row[31],
            'tax_periods_covered': row[32],
            'is_risk_based': row[33],
            'is_completed': row[34],
            'has_findings': row[35],
            'is_appealed': row[36]
        }
        audits.append(audit)

    logger.info(f"  Extracted {len(audits)} audit(s) from L2")
    return audits


def transform_audits(audits: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform audit data for L3 accumulating snapshot fact table.

    Args:
        audits: Raw audit data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed audit data for L3
    """
    logger.info("Transforming audits for L3...")

    transformed = []
    for audit in audits:
        # Get dimension keys
        party_key = lookups['party'].get(audit['party_id'])
        if not party_key:
            logger.warning(f"  Skipping audit {audit['audit_case_id']}: party {audit['party_id']} not found")
            continue

        tax_type_key = lookups['tax_type_code'].get(audit['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Skipping audit {audit['audit_case_id']}: tax_type_code {audit['tax_type_code']} not found")
            continue

        # Date keys for accumulating snapshot
        # Primary date key: actual_start_date if available, else assignment_date
        primary_date = audit['actual_start_date'] if audit['actual_start_date'] else audit['assignment_date']
        dim_date_key = lookups['date'].get(primary_date)
        if not dim_date_key:
            logger.warning(f"  Skipping audit {audit['audit_case_id']}: primary_date {primary_date} not found")
            continue

        # Multiple date keys for lifecycle tracking
        dim_start_date_key = lookups['date'].get(audit['actual_start_date']) if audit['actual_start_date'] else None
        dim_planned_completion_date_key = lookups['date'].get(audit['planned_completion_date']) if audit['planned_completion_date'] else None
        dim_actual_completion_date_key = lookups['date'].get(audit['actual_completion_date']) if audit['actual_completion_date'] else None
        dim_report_issue_date_key = dim_actual_completion_date_key  # Phase G: use completion date as report issue date

        transformed_audit = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_audit_type_key': 1,  # Phase G: placeholder
            'dim_org_unit_key': 1,  # Phase G: default
            'dim_officer_key': 1,  # Phase G: default
            'dim_date_key': dim_date_key,
            'dim_start_date_key': dim_start_date_key if dim_start_date_key else dim_date_key,
            'dim_planned_completion_date_key': dim_planned_completion_date_key,
            'dim_actual_completion_date_key': dim_actual_completion_date_key,
            'dim_report_issue_date_key': dim_report_issue_date_key,
            'audit_case_id': audit['audit_case_id'],
            'audit_case_number': audit['case_number'],
            'audit_plan_id': audit['audit_plan_id'],
            'assessed_amount': audit['adjustment_amount'] if audit['adjustment_amount'] else 0,
            'penalties_assessed': audit['penalty_amount'] if audit['penalty_amount'] else 0,
            'interest_assessed': audit['interest_amount'] if audit['interest_amount'] else 0,
            'total_amount': audit['total_amount_assessed'] if audit['total_amount_assessed'] else 0,
            'collected_amount': 0,  # Phase G: not tracked
            'audit_count': 1,
            'findings_count': audit['findings_count'],
            'tax_periods_covered': audit['tax_periods_covered'],
            'duration_days': audit['duration_days'] if audit['duration_days'] else None,
            'planned_duration_days': audit['planned_duration_days'] if audit['planned_duration_days'] else None,
            'is_risk_based': audit['is_risk_based'],
            'is_completed': audit['is_completed'],
            'has_findings': audit['has_findings'],
            'is_appealed': audit['is_appealed'],
            'selection_date': audit['assignment_date'],
            'notification_date': audit['notification_date'],
            'field_start_date': audit['actual_start_date'],
            'field_end_date': audit['actual_completion_date'],
            'report_issue_date': audit['actual_completion_date'],
            'case_closure_date': audit['actual_completion_date'],
            'audit_status': audit['case_status_code'],
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_audit)

    logger.info(f"  Transformed {len(transformed)} audit(s)")
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


def load_to_clickhouse(audits: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load audits into ClickHouse fact_audit.

    Args:
        audits: Transformed audit data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading audits to ClickHouse fact_audit...")

    if not audits:
        logger.warning("  No audits to load")
        return 0

    # Truncate existing data (Phase G: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_audit")
    logger.info("  Truncated fact_audit")

    # Prepare data for bulk insert
    data = [
        [
            a['dim_party_key'],
            a['dim_tax_type_key'],
            a['dim_audit_type_key'],
            a['dim_org_unit_key'],
            a['dim_officer_key'],
            a['dim_date_key'],
            a['dim_start_date_key'],
            a['dim_planned_completion_date_key'],
            a['dim_actual_completion_date_key'],
            a['dim_report_issue_date_key'],
            a['audit_case_id'],
            a['audit_case_number'],
            a['audit_plan_id'],
            a['assessed_amount'],
            a['penalties_assessed'],
            a['interest_assessed'],
            a['total_amount'],
            a['collected_amount'],
            a['audit_count'],
            a['findings_count'],
            a['tax_periods_covered'],
            a['duration_days'],
            a['planned_duration_days'],
            a['is_risk_based'],
            a['is_completed'],
            a['has_findings'],
            a['is_appealed'],
            a['selection_date'],
            a['notification_date'],
            a['field_start_date'],
            a['field_end_date'],
            a['report_issue_date'],
            a['case_closure_date'],
            a['audit_status'],
            a['etl_batch_id'],
            a['etl_timestamp']
        ]
        for a in audits
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_audit',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_audit_type_key',
            'dim_org_unit_key', 'dim_officer_key', 'dim_date_key',
            'dim_start_date_key', 'dim_planned_completion_date_key',
            'dim_actual_completion_date_key', 'dim_report_issue_date_key',
            'audit_case_id', 'audit_case_number', 'audit_plan_id',
            'assessed_amount', 'penalties_assessed', 'interest_assessed',
            'total_amount', 'collected_amount', 'audit_count',
            'findings_count', 'tax_periods_covered', 'duration_days',
            'planned_duration_days', 'is_risk_based', 'is_completed',
            'has_findings', 'is_appealed', 'selection_date',
            'notification_date', 'field_start_date', 'field_end_date',
            'report_issue_date', 'case_closure_date', 'audit_status',
            'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_audit")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_audit.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_audit data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as audit_count,
            SUM(assessed_amount) as total_assessed,
            SUM(penalties_assessed) as total_penalties,
            SUM(interest_assessed) as total_interest,
            SUM(total_amount) as total_amount,
            SUM(findings_count) as total_findings,
            AVG(duration_days) as avg_duration,
            SUM(is_completed) as completed_count,
            SUM(is_appealed) as appealed_count,
            MIN(selection_date) as earliest_selection,
            MAX(field_end_date) as latest_completion
        FROM ta_dw.fact_audit
    """)

    row = summary.result_rows[0]
    avg_duration = row[6] if row[6] is not None else 0.0

    print("\n" + "=" * 100)
    print("fact_audit: Overall Summary")
    print("=" * 100)
    print(f"Total Audits:            {row[0]}")
    print(f"Total Assessed:          ${row[1]:,.2f}")
    print(f"Total Penalties:         ${row[2]:,.2f}")
    print(f"Total Interest:          ${row[3]:,.2f}")
    print(f"Total Amount:            ${row[4]:,.2f}")
    print(f"Total Findings:          {row[5]}")
    print(f"Avg Duration (days):     {avg_duration:.1f}")
    print(f"Completed Audits:        {row[7]}")
    print(f"Appealed Audits:         {row[8]}")
    print(f"Selection Date Range:    {row[9]} to {row[10] if row[10] else 'In Progress'}")
    print("=" * 100)

    # By audit status
    by_status = clickhouse_client.query("""
        SELECT
            audit_status,
            COUNT(*) as audit_count,
            SUM(total_amount) as total_amount
        FROM ta_dw.fact_audit
        GROUP BY audit_status
        ORDER BY audit_count DESC
    """)

    print("\nAudits by Status:")
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
    logger.info("Starting L2 → L3 Audit ETL (Phase G)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            audits = extract_audits_from_l2(mysql_conn)
            transformed = transform_audits(audits, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Audit ETL Completed Successfully")
            logger.info(f"  Extracted: {len(audits)} audits")
            logger.info(f"  Loaded: {loaded_count} audits")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Audit ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
