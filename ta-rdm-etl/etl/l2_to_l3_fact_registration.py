#!/usr/bin/env python3
"""
L2 → L3 ETL: Registration Fact Table

Extracts registration data from MySQL registration.tax_account and loads into
ClickHouse fact_registration fact table.

This ETL demonstrates star schema joins to dimension tables.

Phase B: Initial fact table load with dimension lookups.
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


def extract_registrations_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract registration data from L2 MySQL.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List of registration dictionaries
    """
    logger.info("Extracting registrations from L2 MySQL...")

    query = """
        SELECT
            ta.tax_account_id,
            ta.party_id,
            ta.tax_type_code,
            ta.tax_account_number,
            ta.account_status_code,
            ta.registration_date,
            ta.registration_method_code,
            ta.registration_reason,
            ta.effective_from_date,
            ta.effective_to_date,
            ta.segmentation_code,
            ta.assigned_office_code,
            ta.filing_frequency_code,
            ta.is_group_filing,
            p.party_id,
            p.tax_identification_number,
            p.party_type_code,
            p.party_name
        FROM registration.tax_account ta
        JOIN party.party p ON ta.party_id = p.party_id
        WHERE ta.account_status_code = 'ACTIVE'
        ORDER BY ta.tax_account_id
    """

    rows = mysql_conn.fetch_all(query)

    registrations = []
    for row in rows:
        registration = {
            'tax_account_id': row[0],
            'party_id': row[1],
            'tax_type_code': row[2],
            'tax_account_number': row[3],
            'account_status': row[4],
            'registration_date': row[5],
            'registration_method': row[6],
            'registration_reason': row[7],
            'effective_from': row[8],
            'effective_to': row[9],
            'segmentation': row[10],
            'office_code': row[11],
            'filing_frequency': row[12],
            'is_group': row[13],
            'tin': row[15],
            'party_type': row[16],
            'party_name': row[17]
        }
        registrations.append(registration)

    logger.info(f"  Extracted {len(registrations)} registration(s) from L2")
    return registrations


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

    # Lookup dim_time keys by date_key (YYYYMMDD format)
    # We'll format dates to date_key format during transform
    time_result = clickhouse_client.query("""
        SELECT date_key, date_key
        FROM ta_dw.dim_time
    """)
    lookups['time'] = {row[0]: row[1] for row in time_result.result_rows}
    logger.info(f"  Loaded {len(lookups['time'])} time key(s)")

    return lookups


def transform_registrations(registrations: List[Dict[str, Any]], lookups: Dict) -> List[Dict[str, Any]]:
    """
    Transform registration data for fact table.
    Performs dimension key lookups.

    Args:
        registrations: Raw registration data from L2
        lookups: Dimension key lookup dictionaries

    Returns:
        Transformed registration facts
    """
    logger.info("Transforming registrations for fact table...")

    transformed = []
    for reg in registrations:
        # Look up dimension keys
        party_key = lookups['party'].get(reg['party_id'])
        if not party_key:
            logger.warning(f"  Party ID {reg['party_id']} not found in dim_party, skipping")
            continue

        tax_type_key = lookups['tax_type'].get(reg['tax_type_code'])
        if not tax_type_key:
            logger.warning(f"  Tax type {reg['tax_type_code']} not found in dim_tax_type, skipping")
            continue

        # Convert registration_date to date_key format (YYYYMMDD)
        date_key = int(reg['registration_date'].strftime('%Y%m%d'))
        if date_key not in lookups['time']:
            logger.warning(f"  Date {reg['registration_date']} not found in dim_time, skipping")
            continue

        # Calculate processing days (Phase B: assume 7 days for all)
        processing_days = 7

        # Determine if voluntary (Phase B: all are mandatory/enforced)
        is_voluntary = 0

        # All registrations are approved (status = ACTIVE)
        is_approved = 1

        # Phase B: Default values for dimensions not yet implemented
        dim_tax_account_key = 0  # Placeholder
        dim_location_key = 0  # Placeholder
        dim_org_unit_key = 1  # Default office
        dim_officer_key = 0  # Placeholder

        transformed_reg = {
            'dim_party_key': party_key,
            'dim_tax_type_key': tax_type_key,
            'dim_tax_account_key': dim_tax_account_key,
            'dim_location_key': dim_location_key,
            'dim_org_unit_key': dim_org_unit_key,
            'dim_officer_key': dim_officer_key,
            'dim_date_key': date_key,
            'tax_account_id': reg['tax_account_id'],
            'registration_number': reg['tax_account_number'],
            'application_reference': None,  # Phase B: Not available
            'registration_count': 1,
            'processing_days': processing_days,
            'is_voluntary': is_voluntary,
            'is_deregistration': 0,  # All are registrations
            'is_approved': is_approved,
            'application_date': reg['registration_date'],
            'approval_date': reg['registration_date'],
            'effective_date': reg['effective_from'],
            'etl_batch_id': 1
        }
        transformed.append(transformed_reg)

    logger.info(f"  Transformed {len(transformed)} registration fact(s)")
    return transformed


def load_to_clickhouse(facts: List[Dict[str, Any]], clickhouse_client) -> int:
    """
    Load registration facts into ClickHouse.

    Args:
        facts: Transformed fact records
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading facts to ClickHouse fact_registration...")

    if not facts:
        logger.warning("  No facts to load")
        return 0

    # Truncate existing data (Phase B: full reload)
    clickhouse_client.command("TRUNCATE TABLE ta_dw.fact_registration")
    logger.info("  Truncated fact_registration")

    # Prepare data for bulk insert
    data = [
        [
            f['dim_party_key'],
            f['dim_tax_type_key'],
            f['dim_tax_account_key'],
            f['dim_location_key'],
            f['dim_org_unit_key'],
            f['dim_officer_key'],
            f['dim_date_key'],
            f['tax_account_id'],
            f['registration_number'],
            f['application_reference'],
            f['registration_count'],
            f['processing_days'],
            f['is_voluntary'],
            f['is_deregistration'],
            f['is_approved'],
            f['application_date'],
            f['approval_date'],
            f['effective_date'],
            f['etl_batch_id']
        ]
        for f in facts
    ]

    # Insert data
    clickhouse_client.insert(
        'ta_dw.fact_registration',
        data,
        column_names=[
            'dim_party_key', 'dim_tax_type_key', 'dim_tax_account_key',
            'dim_location_key', 'dim_org_unit_key', 'dim_officer_key',
            'dim_date_key', 'tax_account_id', 'registration_number',
            'application_reference', 'registration_count', 'processing_days',
            'is_voluntary', 'is_deregistration', 'is_approved',
            'application_date', 'approval_date', 'effective_date', 'etl_batch_id'
        ]
    )

    logger.info(f"  Loaded {len(data)} fact row(s)")
    return len(data)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded facts with star schema queries.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying fact_registration data...")

    # Basic counts
    result = clickhouse_client.query("""
        SELECT COUNT(*) as registration_count
        FROM ta_dw.fact_registration
    """)
    total = result.result_rows[0][0]

    print("\n" + "=" * 100)
    print("fact_registration: Verification Report")
    print("=" * 100)
    print(f"Total Registrations: {total}")
    print("=" * 100)

    # Registrations by party and tax type (STAR SCHEMA JOIN!)
    result = clickhouse_client.query("""
        SELECT
            p.party_name,
            p.party_type,
            tt.tax_type_code,
            tt.tax_type_name,
            t.full_date as registration_date,
            f.registration_number
        FROM ta_dw.fact_registration f
        JOIN ta_dw.dim_party p ON f.dim_party_key = p.party_key
        JOIN ta_dw.dim_tax_type tt ON f.dim_tax_type_key = tt.tax_type_key
        JOIN ta_dw.dim_time t ON f.dim_date_key = t.date_key
        WHERE p.is_current = 1
        ORDER BY p.party_name, tt.tax_type_code
    """)

    print("\nRegistrations by Party and Tax Type (Star Schema Query):")
    print("-" * 100)
    print(f"{'Party Name':<35} {'Type':<12} {'Tax':<6} {'Tax Name':<25} {'Reg Date':<12} {'Account':<18}")
    print("-" * 100)

    for row in result.result_rows:
        party_name, party_type, tax_code, tax_name, reg_date, acc_num = row
        print(f"{party_name[:34]:<35} {party_type:<12} {tax_code:<6} {tax_name[:24]:<25} "
              f"{str(reg_date):<12} {acc_num:<18}")

    print("=" * 100 + "\n")


def main():
    """Main ETL execution."""
    logger.info("=" * 60)
    logger.info("Starting L2 → L3 Registration Fact ETL (Phase B)")
    logger.info("=" * 60)

    try:
        # Connect to MySQL L2
        with get_db_connection() as mysql_conn:
            # Connect to ClickHouse L3
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # ETL Process
            registrations = extract_registrations_from_l2(mysql_conn)
            lookups = lookup_dimension_keys(clickhouse_client)
            transformed = transform_registrations(registrations, lookups)
            loaded_count = load_to_clickhouse(transformed, clickhouse_client)

            # Verify with star schema query
            verify_load(clickhouse_client)

            logger.info("=" * 60)
            logger.info("Registration Fact ETL Completed Successfully")
            logger.info(f"  Extracted: {len(registrations)} registrations")
            logger.info(f"  Loaded: {loaded_count} facts")
            logger.info("=" * 60)

            return 0

    except Exception as e:
        logger.error(f"Registration fact ETL failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
