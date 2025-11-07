#!/usr/bin/env python3
"""
L2 → L3 ETL: Collection Fact

Extracts collection cases and enforcement actions from MySQL compliance_control schema
and loads into ClickHouse fact_collection fact table.

Phase E: Collections & Enforcement - loads ~35 enforcement actions.
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


def extract_collections_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract collection case and enforcement action data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of collection/action dictionaries
    """
    logger.info("Extracting collection data from L2 MySQL...")

    query = """
        SELECT
            ea.enforcement_action_id,
            ea.collection_case_id,
            cc.case_number,
            cc.party_id,
            cc.tax_account_id,
            ta.tax_type_code,
            cc.case_type_code,
            cc.case_status_code,
            cc.priority_code,
            cc.original_debt_amount,
            cc.current_debt_amount,
            cc.amount_collected as case_amount_collected,
            cc.debt_age_days,
            cc.enforcement_level_code,
            cc.case_opened_date,
            ea.action_type_code,
            ea.action_date,
            ea.action_status_code,
            ea.target_amount,
            ea.amount_collected as action_amount_collected,
            ea.action_outcome_code,
            DATEDIFF(ea.action_date, cc.case_opened_date) as days_since_case_opened,
            (SELECT MIN(payment_due_date)
             FROM filing_assessment.assessment
             WHERE tax_account_id = cc.tax_account_id
             AND is_current_version = 1) as debt_due_date
        FROM compliance_control.enforcement_action ea
        INNER JOIN compliance_control.collection_case cc ON ea.collection_case_id = cc.collection_case_id
        INNER JOIN registration.tax_account ta ON cc.tax_account_id = ta.tax_account_id
        ORDER BY ea.action_date
    """

    rows = mysql_conn.fetch_all(query)

    collections = []
    for row in rows:
        collection = {
            'enforcement_action_id': row[0],
            'collection_case_id': row[1],
            'case_number': row[2],
            'party_id': row[3],
            'tax_account_id': row[4],
            'tax_type_code': row[5],
            'case_type_code': row[6],
            'case_status_code': row[7],
            'priority_code': row[8],
            'original_debt_amount': row[9],
            'current_debt_amount': row[10],
            'case_amount_collected': row[11],
            'debt_age_days': row[12],
            'enforcement_level_code': row[13],
            'case_opened_date': row[14],
            'action_type_code': row[15],
            'action_date': row[16],
            'action_status_code': row[17],
            'target_amount': row[18],
            'action_amount_collected': row[19],
            'action_outcome_code': row[20],
            'days_since_case_opened': row[21],
            'debt_due_date': row[22]
        }
        collections.append(collection)

    logger.info(f"  Extracted {len(collections)} collection action(s) from L2")
    return collections


def transform_collections(collections: List[Dict[str, Any]], lookups: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Transform collection data for L3 fact table.

    Args:
        collections: Raw collection data from L2
        lookups: Dimension lookup dictionaries

    Returns:
        Transformed collection data for L3
    """
    logger.info("Transforming collections for L3...")

    transformed = []
    for coll in collections:
        # Get dimension keys
        party_key = lookups['party'].get(coll['party_id'])
        if not party_key:
            logger.warning(f"  Skipping action {coll['enforcement_action_id']}: party {coll['party_id']} not found")
            continue

        tax_type_key = lookups['tax_type_code'].get(coll['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Skipping action {coll['enforcement_action_id']}: tax_type_code {coll['tax_type_code']} not found")
            continue

        date_key = lookups['date'].get(coll['action_date'])
        if not date_key:
            logger.warning(f"  Skipping action {coll['enforcement_action_id']}: action_date {coll['action_date']} not found")
            continue

        # Calculate effectiveness ratio
        if coll['current_debt_amount'] > 0:
            effectiveness_ratio = float(coll['action_amount_collected']) / float(coll['current_debt_amount'])
        else:
            effectiveness_ratio = 0.0

        # Determine flags
        is_successful = 1 if coll['action_amount_collected'] > 0 else 0
        is_escalated = 1 if coll['enforcement_level_code'] in ('GARNISHMENT', 'LEGAL_ACTION') else 0

        transformed_collection = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_account_key': coll['tax_account_id'],  # Phase E: use natural key
            'dim_org_unit_key': 1,  # Phase E: default
            'dim_officer_key': 1,  # Phase E: default
            'dim_date_key': date_key,
            'collection_case_id': coll['collection_case_id'],
            'enforcement_action_id': coll['enforcement_action_id'],
            'case_number': coll['case_number'],
            'debt_amount': coll['current_debt_amount'],
            'collected_amount': coll['action_amount_collected'],
            'action_cost': 0,  # Not tracked in Phase E
            'effectiveness_ratio': round(effectiveness_ratio, 4),
            'action_count': 1,
            'is_successful': is_successful,
            'is_escalated': is_escalated,
            'action_date': coll['action_date'],
            'case_open_date': coll['case_opened_date'],
            'debt_due_date': coll['debt_due_date'],
            'action_type': coll['action_type_code'],
            'enforcement_level': coll['enforcement_level_code'],
            'case_status': coll['case_status_code'],
            'debt_age_days': coll['debt_age_days'],
            'etl_batch_id': 1,
            'etl_timestamp': datetime.now()
        }
        transformed.append(transformed_collection)

    logger.info(f"  Transformed {len(transformed)} collection action(s)")
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


def load_to_clickhouse(collections: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load collections into ClickHouse fact_collection.

    Args:
        collections: Transformed collection data
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading collections to ClickHouse fact_collection...")

    if not collections:
        logger.warning("  No collections to load")
        return 0

    # Truncate existing data (Phase E: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_collection")
    logger.info("  Truncated fact_collection")

    # Prepare data for bulk insert
    data = [
        [
            c['dim_party_key'],
            c['dim_tax_type_key'],
            c['dim_tax_account_key'],
            c['dim_org_unit_key'],
            c['dim_officer_key'],
            c['dim_date_key'],
            c['collection_case_id'],
            c['enforcement_action_id'],
            c['case_number'],
            c['debt_amount'],
            c['collected_amount'],
            c['action_cost'],
            c['effectiveness_ratio'],
            c['action_count'],
            c['is_successful'],
            c['is_escalated'],
            c['action_date'],
            c['case_open_date'],
            c['debt_due_date'],
            c['action_type'],
            c['enforcement_level'],
            c['case_status'],
            c['debt_age_days'],
            c['etl_batch_id'],
            c['etl_timestamp']
        ]
        for c in collections
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_collection',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_account_key',
            'dim_org_unit_key', 'dim_officer_key', 'dim_date_key',
            'collection_case_id', 'enforcement_action_id', 'case_number',
            'debt_amount', 'collected_amount', 'action_cost',
            'effectiveness_ratio', 'action_count', 'is_successful',
            'is_escalated', 'action_date', 'case_open_date', 'debt_due_date',
            'action_type', 'enforcement_level', 'case_status',
            'debt_age_days', 'etl_batch_id', 'etl_timestamp'
        ]
    )

    logger.info(f"  Loaded {len(data)} row(s) to fact_collection")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in fact_collection.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_collection data...")

    # Overall summary
    summary = clickhouse_client.query("""
        SELECT
            COUNT(*) as action_count,
            SUM(debt_amount) as total_debt,
            SUM(collected_amount) as total_collected,
            AVG(effectiveness_ratio) as avg_effectiveness,
            SUM(is_successful) as successful_actions,
            SUM(is_escalated) as escalated_actions,
            MIN(action_date) as earliest_action,
            MAX(action_date) as latest_action,
            AVG(debt_age_days) as avg_debt_age
        FROM ta_dw.fact_collection
    """)

    row = summary.result_rows[0]
    print("\n" + "=" * 100)
    print("fact_collection: Overall Summary")
    print("=" * 100)
    print(f"Total Actions:           {row[0]}")
    print(f"Total Debt:              ${row[1]:,.2f}")
    print(f"Total Collected:         ${row[2]:,.2f}")
    print(f"Avg Effectiveness:       {row[3]:.2%}")
    print(f"Successful Actions:      {row[4]}")
    print(f"Escalated Actions:       {row[5]}")
    print(f"Action Date Range:       {row[6]} to {row[7]}")
    print(f"Avg Debt Age (days):     {row[8]:.0f}")
    print("=" * 100)

    # By enforcement level
    by_level = clickhouse_client.query("""
        SELECT
            enforcement_level,
            COUNT(*) as action_count,
            SUM(collected_amount) as total_collected,
            AVG(effectiveness_ratio) as avg_effectiveness
        FROM ta_dw.fact_collection
        GROUP BY enforcement_level
        ORDER BY action_count DESC
    """)

    print("\nActions by Enforcement Level:")
    print("-" * 100)
    print(f"{'Level':<20} {'Count':<10} {'Collected':<20} {'Effectiveness':<15}")
    print("-" * 100)

    for row in by_level.result_rows:
        level, count, collected, effectiveness = row
        print(f"{level:<20} {count:<10} ${collected:>18,.2f} {effectiveness:>13.2%}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 80)
    logger.info("Starting L2 → L3 Collection ETL (Phase E)")
    logger.info("=" * 80)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # Build dimension lookups
            lookups = lookup_dimension_keys(clickhouse_client)

            # ETL Process
            collections = extract_collections_from_l2(mysql_conn)
            transformed = transform_collections(collections, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify
            verify_load(clickhouse_client)

            logger.info("=" * 80)
            logger.info("Collection ETL Completed Successfully")
            logger.info(f"  Extracted: {len(collections)} actions")
            logger.info(f"  Loaded: {loaded_count} actions")
            logger.info("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"Collection ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
