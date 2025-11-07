#!/usr/bin/env python3
"""
L2 → L3 ETL: Risk Assessment Fact

Extracts risk assessment records from MySQL compliance_control schema and loads into
ClickHouse fact_risk_assessment fact table.

Phase I: Risk Assessment - loads risk profiles with calculated risk scores and metrics.
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


def extract_risk_assessments_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract risk assessment data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of risk assessment dictionaries
    """
    logger.info("Extracting risk assessments from L2 MySQL...")

    query = """
        SELECT
            trp.taxpayer_risk_profile_id,
            trp.party_id,
            trp.overall_risk_score,
            trp.risk_rating_code,
            trp.filing_risk_score,
            trp.payment_risk_score,
            trp.accuracy_risk_score as reporting_risk_score,
            trp.industry_risk_score,
            trp.complexity_risk_score,
            trp.late_filing_count_12m,
            trp.non_filing_count_12m,
            trp.late_payment_count_12m,
            trp.current_arrears_amount,
            trp.objection_count_history,
            trp.third_party_discrepancy_count,
            trp.audit_adjustment_history,
            trp.profile_last_updated,
            trp.treatment_recommendation,
            -- Compute audit history risk score from adjustment amounts
            CASE
                WHEN trp.audit_adjustment_history > 50000 THEN 80.0
                WHEN trp.audit_adjustment_history > 10000 THEN 60.0
                WHEN trp.audit_adjustment_history > 0 THEN 40.0
                ELSE 15.0
            END as audit_history_risk_score,
            -- Compute third party risk score from discrepancy count
            CASE
                WHEN trp.third_party_discrepancy_count > 5 THEN 75.0
                WHEN trp.third_party_discrepancy_count > 2 THEN 50.0
                WHEN trp.third_party_discrepancy_count > 0 THEN 30.0
                ELSE 10.0
            END as third_party_risk_score,
            -- Count audit assessments
            (SELECT COUNT(*)
             FROM compliance_control.audit_case ac
             WHERE ac.party_id = trp.party_id
             AND ac.total_amount_assessed > 0) as audit_assessment_count,
            -- Check if audit candidate
            CASE
                WHEN trp.overall_risk_score > 60 OR trp.current_arrears_amount > 50000 THEN 1
                ELSE 0
            END as is_audit_candidate
        FROM compliance_control.taxpayer_risk_profile trp
        ORDER BY trp.overall_risk_score DESC
    """

    rows = mysql_conn.fetch_all(query)

    risk_assessments = []
    for row in rows:
        risk_assessment = {
            'risk_profile_id': row[0],
            'party_id': row[1],
            'overall_risk_score': row[2],
            'risk_rating_code': row[3],
            'filing_risk_score': row[4] if row[4] is not None else 20.0,
            'payment_risk_score': row[5] if row[5] is not None else 20.0,
            'reporting_risk_score': row[6] if row[6] is not None else 20.0,
            'industry_risk_score': row[7] if row[7] is not None else 20.0,
            'complexity_risk_score': row[8] if row[8] is not None else 20.0,
            'late_filing_count': row[9] if row[9] is not None else 0,
            'non_filing_count': row[10] if row[10] is not None else 0,
            'late_payment_count': row[11] if row[11] is not None else 0,
            'current_arrears_amount': row[12] if row[12] is not None else 0,
            'objection_count': row[13] if row[13] is not None else 0,
            'third_party_discrepancy_count': row[14] if row[14] is not None else 0,
            'audit_adjustment_history': row[15] if row[15] is not None else 0,
            'profile_last_updated': row[16],
            'treatment_recommendation': row[17],
            'audit_history_risk_score': row[18],
            'third_party_risk_score': row[19],
            'audit_assessment_count': row[20],
            'is_audit_candidate': row[21]
        }
        risk_assessments.append(risk_assessment)

    logger.info(f"  Extracted {len(risk_assessments)} risk assessment(s) from L2")
    return risk_assessments


def transform_risk_assessments(risk_assessments: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform risk assessment data for L3 fact table.

    Args:
        risk_assessments: Raw risk assessment data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed risk assessment data for L3
    """
    logger.info("Transforming risk assessments for L3...")

    transformed = []
    for risk in risk_assessments:
        # Get dimension keys
        party_key = lookups['party'].get(risk['party_id'])
        if not party_key:
            logger.warning(f"  Skipping risk assessment {risk['risk_profile_id']}: party {risk['party_id']} not found")
            continue

        # Date key: use profile_last_updated date
        assessment_date = risk['profile_last_updated'].date() if isinstance(risk['profile_last_updated'], datetime) else risk['profile_last_updated']
        date_key = lookups['date'].get(assessment_date)
        if not date_key:
            logger.warning(f"  Skipping risk assessment {risk['risk_profile_id']}: date {assessment_date} not found")
            continue

        # Compute assessment year and quarter
        assessment_year = assessment_date.year
        assessment_quarter = (assessment_date.month - 1) // 3 + 1

        # Risk flags
        is_high_risk = 1 if risk['overall_risk_score'] >= 60 else 0
        is_reduced = 1 if risk['risk_rating_code'] == 'PARTIALLY_ACCEPTED' else 0
        is_cancelled = 0  # Phase I: not applicable to risk assessment

        # Risk trend (Phase I: no previous data, set to STABLE)
        risk_trend = 'STABLE'
        previous_risk_score = None

        transformed_risk = {
            'dim_party_key': party_key,
            'dim_tax_type_key': None,  # Phase I: risk is party-level, not tax-type-specific
            'dim_location_key': 1,  # Phase I: default
            'dim_date_key': date_key,
            'assessment_date': assessment_date,
            'assessment_year': assessment_year,
            'assessment_quarter': assessment_quarter,
            'risk_profile_id': risk['risk_profile_id'],
            'party_id': risk['party_id'],
            'overall_risk_score': risk['overall_risk_score'],
            'filing_risk_score': risk['filing_risk_score'],
            'payment_risk_score': risk['payment_risk_score'],
            'reporting_risk_score': risk['reporting_risk_score'],
            'audit_history_risk_score': risk['audit_history_risk_score'],
            'third_party_risk_score': risk['third_party_risk_score'],
            'non_filing_count': risk['non_filing_count'],
            'late_filing_count': risk['late_filing_count'],
            'late_payment_count': risk['late_payment_count'],
            'audit_assessment_count': risk['audit_assessment_count'],
            'objection_count': risk['objection_count'],
            'is_high_risk': is_high_risk,
            'is_audit_candidate': risk['is_audit_candidate'],
            'risk_rating': risk['risk_rating_code'],
            'previous_risk_score': previous_risk_score,
            'risk_trend': risk_trend,
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_risk)

    logger.info(f"  Transformed {len(transformed)} risk assessment(s)")
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

    # Date keys
    date_result = clickhouse_client.query("SELECT full_date, date_key FROM ta_dw.dim_time")
    lookups['date'] = {row[0]: row[1] for row in date_result.result_rows}
    logger.info(f"  Loaded {len(lookups['date'])} date lookups")

    return lookups


def load_to_clickhouse(risk_assessments: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load risk assessments into ClickHouse fact_risk_assessment.

    Args:
        risk_assessments: Transformed risk assessment data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading risk assessments to ClickHouse fact_risk_assessment...")

    if not risk_assessments:
        logger.warning("  No risk assessments to load")
        return 0

    # Truncate existing data (Phase I: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_risk_assessment")
    logger.info("  Truncated fact_risk_assessment")

    # Prepare data for bulk insert
    data = [
        [
            r['dim_party_key'],
            r['dim_tax_type_key'],
            r['dim_location_key'],
            r['dim_date_key'],
            r['assessment_date'],
            r['assessment_year'],
            r['assessment_quarter'],
            r['risk_profile_id'],
            r['party_id'],
            r['overall_risk_score'],
            r['filing_risk_score'],
            r['payment_risk_score'],
            r['reporting_risk_score'],
            r['audit_history_risk_score'],
            r['third_party_risk_score'],
            r['non_filing_count'],
            r['late_filing_count'],
            r['late_payment_count'],
            r['audit_assessment_count'],
            r['objection_count'],
            r['is_high_risk'],
            r['is_audit_candidate'],
            r['risk_rating'],
            r['previous_risk_score'],
            r['risk_trend'],
            r['etl_batch_id'],
            r['etl_timestamp']
        ]
        for r in risk_assessments
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_risk_assessment',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_location_key',
            'dim_date_key', 'assessment_date', 'assessment_year',
            'assessment_quarter', 'risk_profile_id', 'party_id',
            'overall_risk_score', 'filing_risk_score', 'payment_risk_score',
            'reporting_risk_score', 'audit_history_risk_score',
            'third_party_risk_score', 'non_filing_count', 'late_filing_count',
            'late_payment_count', 'audit_assessment_count', 'objection_count',
            'is_high_risk', 'is_audit_candidate', 'risk_rating',
            'previous_risk_score', 'risk_trend', 'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_risk_assessment")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_risk_assessment.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_risk_assessment data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as assessment_count,
            AVG(overall_risk_score) as avg_risk_score,
            AVG(filing_risk_score) as avg_filing_risk,
            AVG(payment_risk_score) as avg_payment_risk,
            AVG(reporting_risk_score) as avg_reporting_risk,
            AVG(audit_history_risk_score) as avg_audit_history_risk,
            SUM(non_filing_count) as total_non_filing,
            SUM(late_filing_count) as total_late_filing,
            SUM(late_payment_count) as total_late_payment,
            SUM(audit_assessment_count) as total_audit_assessments,
            SUM(objection_count) as total_objections,
            SUM(is_high_risk) as high_risk_count,
            SUM(is_audit_candidate) as audit_candidate_count
        FROM ta_dw.fact_risk_assessment
    """)

    row = summary.result_rows[0]

    print("\n" + "=" * 100)
    print("fact_risk_assessment: Overall Summary")
    print("=" * 100)
    print(f"Total Assessments:       {row[0]}")
    print(f"Avg Risk Score:          {row[1]:.2f}")
    print(f"Avg Filing Risk:         {row[2]:.2f}")
    print(f"Avg Payment Risk:        {row[3]:.2f}")
    print(f"Avg Reporting Risk:      {row[4]:.2f}")
    print(f"Avg Audit History Risk:  {row[5]:.2f}")
    print(f"Total Non-Filings:       {row[6]}")
    print(f"Total Late Filings:      {row[7]}")
    print(f"Total Late Payments:     {row[8]}")
    print(f"Total Audit Assessments: {row[9]}")
    print(f"Total Objections:        {row[10]}")
    print(f"High Risk Count:         {row[11]}")
    print(f"Audit Candidates:        {row[12]}")
    print("=" * 100)

    # By risk rating
    by_rating = clickhouse_client.query("""
        SELECT
            risk_rating,
            COUNT(*) as count,
            AVG(overall_risk_score) as avg_score,
            SUM(is_audit_candidate) as audit_candidates
        FROM ta_dw.fact_risk_assessment
        GROUP BY risk_rating
        ORDER BY avg_score DESC
    """)

    print("\nRisk Assessments by Rating:")
    print("-" * 100)
    print(f"{'Rating':<20} {'Count':<10} {'Avg Score':<15} {'Audit Candidates':<20}")
    print("-" * 100)

    for row in by_rating.result_rows:
        rating, count, avg_score, candidates = row
        print(f"{rating:<20} {count:<10} {avg_score:<15.2f} {candidates:<20}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Risk Assessment ETL (Phase I)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            risk_assessments = extract_risk_assessments_from_l2(mysql_conn)
            transformed = transform_risk_assessments(risk_assessments, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Risk Assessment ETL Completed Successfully")
            logger.info(f"  Extracted: {len(risk_assessments)} risk assessments")
            logger.info(f"  Loaded: {loaded_count} risk assessments")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Risk Assessment ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
