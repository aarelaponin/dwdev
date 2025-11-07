#!/usr/bin/env python3
"""
L2 → L3 ETL: Taxpayer Activity Fact

Computes taxpayer activity snapshots by aggregating data from existing fact tables in L3.
This is a derived fact table that summarizes all taxpayer interactions and compliance metrics.

Phase J: Taxpayer Activity - creates monthly activity snapshots for each taxpayer.
"""

import logging
from datetime import datetime, date
from typing import List, Dict, Any
from decimal import Decimal
import clickhouse_connect

from config.clickhouse_config import CLICKHOUSE_CONFIG


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def get_snapshot_dates(clickhouse_client) -> List[Dict[str, Any]]:
    """
    Determine snapshot dates from available data.
    Use end of each month where we have data.

    Args:
        clickhouse_client: ClickHouse client connection

    Returns:
        List of snapshot date dictionaries
    """
    logger.info("Determining snapshot dates from available data...")

    # Get distinct year-months from fact tables
    query = """
        SELECT DISTINCT
            toLastDayOfMonth(full_date) as snapshot_date,
            year,
            month
        FROM ta_dw.dim_time
        WHERE full_date <= today()
        AND full_date >= '2024-01-01'
        ORDER BY snapshot_date DESC
        LIMIT 6
    """

    result = clickhouse_client.query(query)
    snapshots = []

    for row in result.result_rows:
        snapshots.append({
            'snapshot_date': row[0],
            'snapshot_year': row[1],
            'snapshot_month': row[2]
        })

    logger.info(f"  Found {len(snapshots)} snapshot date(s)")
    return snapshots


def compute_taxpayer_activity(clickhouse_client, snapshot_date: date) -> List[Dict[str, Any]]:
    """
    Compute taxpayer activity metrics for a given snapshot date.

    Args:
        clickhouse_client: ClickHouse client connection
        snapshot_date: Date of the snapshot

    Returns:
        List of taxpayer activity records
    """
    logger.info(f"Computing taxpayer activity for snapshot date {snapshot_date}...")

    # Get all parties
    parties_query = """
        SELECT DISTINCT
            party_key,
            party_id
        FROM ta_dw.dim_party
        WHERE is_current = 1
    """

    parties_result = clickhouse_client.query(parties_query)
    activities = []

    for party_row in parties_result.result_rows:
        party_key = party_row[0]
        party_id = party_row[1]

        # Filing metrics
        filing_query = f"""
            SELECT
                COUNT(*) as total_filings,
                SUM(if(is_late = 0, 1, 0)) as on_time_filings,
                SUM(is_late) as late_filings,
                0 as non_filings
            FROM ta_dw.fact_filing
            WHERE dim_party_key = {party_key}
            AND filing_date <= '{snapshot_date}'
        """
        filing_result = clickhouse_client.query(filing_query)
        filing_data = filing_result.result_rows[0] if filing_result.result_rows else (0, 0, 0, 0)

        # Payment metrics (Phase J: simplified - all payments counted as on-time)
        payment_query = f"""
            SELECT
                COUNT(*) as total_payments,
                SUM(payment_amount) as total_payment_amount,
                SUM(payment_amount) as on_time_payment_amount,
                0 as late_payment_amount
            FROM ta_dw.fact_payment
            WHERE dim_party_key = {party_key}
            AND payment_date <= '{snapshot_date}'
        """
        payment_result = clickhouse_client.query(payment_query)
        payment_data = payment_result.result_rows[0] if payment_result.result_rows else (0, 0, 0, 0)

        # Assessment metrics
        assessment_query = f"""
            SELECT
                COUNT(*) as total_assessments,
                SUM(assessed_amount) as total_assessed,
                SUM(if(is_self_assessment = 1, assessed_amount, 0)) as self_assessed,
                SUM(if(is_administrative = 1 OR is_audit_assessment = 1, assessed_amount, 0)) as admin_assessed
            FROM ta_dw.fact_assessment
            WHERE dim_party_key = {party_key}
            AND assessment_date <= '{snapshot_date}'
        """
        assessment_result = clickhouse_client.query(assessment_query)
        assessment_data = assessment_result.result_rows[0] if assessment_result.result_rows else (0, 0, 0, 0)

        # Balance metrics (latest snapshot on or before snapshot_date)
        balance_query = f"""
            SELECT
                closing_balance,
                arrears_amount,
                if(is_credit_balance = 1, abs(closing_balance), 0) as credit_balance
            FROM ta_dw.fact_account_balance
            WHERE dim_party_key = {party_key}
            AND snapshot_date <= '{snapshot_date}'
            ORDER BY snapshot_date DESC
            LIMIT 1
        """
        balance_result = clickhouse_client.query(balance_query)
        balance_data = balance_result.result_rows[0] if balance_result.result_rows else (0, 0, 0)

        # Audit metrics
        audit_query = f"""
            SELECT
                COUNT(*) as audit_count,
                SUM(if(audit_status NOT IN ('FINALIZED', 'CLOSED'), 1, 0)) as active_audits
            FROM ta_dw.fact_audit
            WHERE dim_party_key = {party_key}
            AND selection_date <= '{snapshot_date}'
        """
        audit_result = clickhouse_client.query(audit_query)
        audit_data = audit_result.result_rows[0] if audit_result.result_rows else (0, 0)

        # Collection metrics
        collection_query = f"""
            SELECT
                COUNT(*) as collection_count,
                SUM(if(case_status IN ('INITIATED', 'IN_PROGRESS', 'PENDING', 'ACTIVE'), 1, 0)) as active_collections
            FROM ta_dw.fact_collection
            WHERE dim_party_key = {party_key}
            AND action_date <= '{snapshot_date}'
        """
        collection_result = clickhouse_client.query(collection_query)
        collection_data = collection_result.result_rows[0] if collection_result.result_rows else (0, 0)

        # Objection metrics
        objection_query = f"""
            SELECT
                SUM(if(objection_status IN ('UNDER_REVIEW', 'HEARING_SCHEDULED'), 1, 0)) as active_objections
            FROM ta_dw.fact_objection
            WHERE dim_party_key = {party_key}
            AND filing_date <= '{snapshot_date}'
        """
        objection_result = clickhouse_client.query(objection_query)
        active_objections = objection_result.result_rows[0][0] if objection_result.result_rows else 0

        # Risk rating
        risk_query = f"""
            SELECT risk_rating
            FROM ta_dw.fact_risk_assessment
            WHERE dim_party_key = {party_key}
            ORDER BY assessment_date DESC
            LIMIT 1
        """
        risk_result = clickhouse_client.query(risk_query)
        current_risk_rating = risk_result.result_rows[0][0] if risk_result.result_rows else 'UNKNOWN'

        # Calculate compliance rates
        total_filings = filing_data[0] if filing_data[0] else 0
        on_time_filings = filing_data[1] if filing_data[1] else 0
        filing_compliance_rate = Decimal(on_time_filings * 100 / total_filings) if total_filings > 0 else Decimal('0.00')

        total_payment_amount = payment_data[1] if payment_data[1] else Decimal('0.00')
        on_time_payment_amount = payment_data[2] if payment_data[2] else Decimal('0.00')
        payment_compliance_rate = Decimal(on_time_payment_amount * 100 / total_payment_amount) if total_payment_amount > 0 else Decimal('0.00')

        # Overall compliance score (average of filing and payment compliance)
        overall_compliance_score = (filing_compliance_rate + payment_compliance_rate) / 2 if total_filings > 0 or total_payment_amount > 0 else Decimal('0.00')

        # Is compliant flag
        is_compliant = 1 if overall_compliance_score >= 80 else 0

        # Get date key
        date_key_query = f"""
            SELECT date_key
            FROM ta_dw.dim_time
            WHERE full_date = '{snapshot_date}'
        """
        date_key_result = clickhouse_client.query(date_key_query)
        date_key = date_key_result.result_rows[0][0] if date_key_result.result_rows else None

        if not date_key:
            logger.warning(f"  Skipping party {party_id}: snapshot_date {snapshot_date} not found in dim_time")
            continue

        activity = {
            'dim_party_key': party_key,
            'dim_location_key': 1,  # Phase J: default
            'dim_date_key': date_key,
            'snapshot_date': snapshot_date,
            'snapshot_year': snapshot_date.year,
            'snapshot_month': snapshot_date.month,
            'party_id': party_id,
            'total_filings': filing_data[0],
            'on_time_filings': filing_data[1] if filing_data[1] else 0,
            'late_filings': filing_data[2] if filing_data[2] else 0,
            'non_filings': filing_data[3],
            'total_payments': payment_data[0],
            'payment_amount': payment_data[1] if payment_data[1] else Decimal('0.00'),
            'on_time_payment_amount': payment_data[2] if payment_data[2] else Decimal('0.00'),
            'late_payment_amount': payment_data[3] if payment_data[3] else Decimal('0.00'),
            'total_assessments': assessment_data[0],
            'assessed_amount': assessment_data[1] if assessment_data[1] else Decimal('0.00'),
            'self_assessed_amount': assessment_data[2] if assessment_data[2] else Decimal('0.00'),
            'admin_assessed_amount': assessment_data[3] if assessment_data[3] else Decimal('0.00'),
            'outstanding_balance': balance_data[0] if balance_data[0] else Decimal('0.00'),
            'arrears_amount': balance_data[1] if balance_data[1] else Decimal('0.00'),
            'credit_balance': balance_data[2] if balance_data[2] else Decimal('0.00'),
            'filing_compliance_rate': filing_compliance_rate,
            'payment_compliance_rate': payment_compliance_rate,
            'overall_compliance_score': overall_compliance_score,
            'collection_actions_count': collection_data[0],
            'audit_cases_count': audit_data[0],
            'has_active_audit': 1 if audit_data[1] > 0 else 0,
            'has_active_collection': 1 if collection_data[1] > 0 else 0,
            'has_active_objection': 1 if active_objections > 0 else 0,
            'is_compliant': is_compliant,
            'current_risk_rating': current_risk_rating,
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }

        activities.append(activity)

    logger.info(f"  Computed activity for {len(activities)} taxpayer(s)")
    return activities


def load_to_clickhouse(activities: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load taxpayer activities into ClickHouse fact_taxpayer_activity.

    Args:
        activities: Computed taxpayer activity data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading taxpayer activities to ClickHouse fact_taxpayer_activity...")

    if not activities:
        logger.warning("  No activities to load")
        return 0

    # Truncate existing data (Phase J: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_taxpayer_activity")
    logger.info("  Truncated fact_taxpayer_activity")

    # Prepare data for bulk insert
    data = [
        [
            a['dim_party_key'],
            a['dim_location_key'],
            a['dim_date_key'],
            a['snapshot_date'],
            a['snapshot_year'],
            a['snapshot_month'],
            a['party_id'],
            a['total_filings'],
            a['on_time_filings'],
            a['late_filings'],
            a['non_filings'],
            a['total_payments'],
            a['payment_amount'],
            a['on_time_payment_amount'],
            a['late_payment_amount'],
            a['total_assessments'],
            a['assessed_amount'],
            a['self_assessed_amount'],
            a['admin_assessed_amount'],
            a['outstanding_balance'],
            a['arrears_amount'],
            a['credit_balance'],
            a['filing_compliance_rate'],
            a['payment_compliance_rate'],
            a['overall_compliance_score'],
            a['collection_actions_count'],
            a['audit_cases_count'],
            a['has_active_audit'],
            a['has_active_collection'],
            a['has_active_objection'],
            a['is_compliant'],
            a['current_risk_rating'],
            a['etl_batch_id'],
            a['etl_timestamp']
        ]
        for a in activities
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_taxpayer_activity',
        data,
        column_names=[
            'dim_party_key', 'dim_location_key', 'dim_date_key',
            'snapshot_date', 'snapshot_year', 'snapshot_month',
            'party_id', 'total_filings', 'on_time_filings',
            'late_filings', 'non_filings', 'total_payments',
            'payment_amount', 'on_time_payment_amount', 'late_payment_amount',
            'total_assessments', 'assessed_amount', 'self_assessed_amount',
            'admin_assessed_amount', 'outstanding_balance', 'arrears_amount',
            'credit_balance', 'filing_compliance_rate', 'payment_compliance_rate',
            'overall_compliance_score', 'collection_actions_count',
            'audit_cases_count', 'has_active_audit', 'has_active_collection',
            'has_active_objection', 'is_compliant', 'current_risk_rating',
            'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_taxpayer_activity")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_taxpayer_activity.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_taxpayer_activity data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as snapshot_count,
            COUNT(DISTINCT party_id) as unique_taxpayers,
            SUM(total_filings) as total_filings,
            SUM(total_payments) as total_payments,
            SUM(payment_amount) as total_payment_amount,
            SUM(total_assessments) as total_assessments,
            SUM(assessed_amount) as total_assessed_amount,
            AVG(filing_compliance_rate) as avg_filing_compliance,
            AVG(payment_compliance_rate) as avg_payment_compliance,
            AVG(overall_compliance_score) as avg_compliance_score,
            SUM(is_compliant) as compliant_count,
            SUM(has_active_audit) as active_audits,
            SUM(has_active_collection) as active_collections,
            SUM(has_active_objection) as active_objections
        FROM ta_dw.fact_taxpayer_activity
    """)

    row = summary.result_rows[0]

    print("\n" + "=" * 100)
    print("fact_taxpayer_activity: Overall Summary")
    print("=" * 100)
    print(f"Total Snapshots:         {row[0]}")
    print(f"Unique Taxpayers:        {row[1]}")
    print(f"Total Filings:           {row[2]}")
    print(f"Total Payments:          {row[3]}")
    print(f"Total Payment Amount:    ${row[4]:,.2f}")
    print(f"Total Assessments:       {row[5]}")
    print(f"Total Assessed Amount:   ${row[6]:,.2f}")
    print(f"Avg Filing Compliance:   {row[7]:.2f}%")
    print(f"Avg Payment Compliance:  {row[8]:.2f}%")
    print(f"Avg Compliance Score:    {row[9]:.2f}")
    print(f"Compliant Taxpayers:     {row[10]}")
    print(f"Active Audits:           {row[11]}")
    print(f"Active Collections:      {row[12]}")
    print(f"Active Objections:       {row[13]}")
    print("=" * 100)

    # By snapshot month
    by_month = clickhouse_client.query("""
        SELECT
            snapshot_year,
            snapshot_month,
            COUNT(*) as taxpayer_count,
            AVG(overall_compliance_score) as avg_compliance
        FROM ta_dw.fact_taxpayer_activity
        GROUP BY snapshot_year, snapshot_month
        ORDER BY snapshot_year DESC, snapshot_month DESC
    """)

    print("\nActivity by Month:")
    print("-" * 100)
    print(f"{'Year':<10} {'Month':<10} {'Taxpayers':<15} {'Avg Compliance':<20}")
    print("-" * 100)

    for row in by_month.result_rows:
        year, month, count, compliance = row
        print(f"{year:<10} {month:<10} {count:<15} {compliance:<20.2f}%")

    # By risk rating
    by_risk = clickhouse_client.query("""
        SELECT
            current_risk_rating,
            COUNT(*) as count,
            AVG(overall_compliance_score) as avg_compliance,
            SUM(arrears_amount) as total_arrears
        FROM ta_dw.fact_taxpayer_activity
        GROUP BY current_risk_rating
        ORDER BY avg_compliance ASC
    """)

    print("\nActivity by Risk Rating:")
    print("-" * 100)
    print(f"{'Risk Rating':<20} {'Count':<10} {'Avg Compliance':<20} {'Total Arrears':<20}")
    print("-" * 100)

    for row in by_risk.result_rows:
        rating, count, compliance, arrears = row
        print(f"{rating:<20} {count:<10} {compliance:<20.2f}% ${arrears:>18,.2f}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L3 → L3 Taxpayer Activity ETL (Phase J)")
    logger.info("=" * 80)

    try:
        # Connect to ClickHouse L3
        clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

        # Get snapshot dates
        snapshot_dates = get_snapshot_dates(clickhouse_client)

        if not snapshot_dates:
            logger.warning("No snapshot dates found")
            return 1

        # Compute activity for each snapshot date
        all_activities = []
        for snapshot in snapshot_dates:
            activities = compute_taxpayer_activity(clickhouse_client, snapshot['snapshot_date'])
            all_activities.extend(activities)

        # Load to ClickHouse
        loaded_count = load_to_clickhouse(all_activities, clickhouse_client)

        # Verify
        verify_load(clickhouse_client)

        logger.info("=" * 80)
        logger.info("Taxpayer Activity ETL Completed Successfully")
        logger.info(f"  Computed: {len(all_activities)} activity snapshots")
        logger.info(f"  Loaded: {loaded_count} snapshots")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"Taxpayer Activity ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
