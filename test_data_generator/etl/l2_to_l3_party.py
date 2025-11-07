#!/usr/bin/env python3
"""
L2 → L3 Party Dimension ETL

Extracts party data from MySQL Layer 2 (TA-RDM) and loads into
ClickHouse Layer 3 (ta_dw) dim_party table with SCD Type 2 support.

Phase A: Initial load of foundation data (5 parties)

Usage:
    python etl/l2_to_l3_party.py
    python etl/l2_to_l3_party.py --batch-id 1
    python etl/l2_to_l3_party.py --truncate  # Clear target before load
"""

import argparse
import sys
import os
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import get_db_connection
from utils.clickhouse_utils import get_clickhouse_connection
from config.database_config import get_connection_string as get_mysql_conn_str
from config.clickhouse_config import get_connection_string as get_ch_conn_str


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


# Default values for fields not yet in L2 schema
# Phase B: We now have location data, so address can be more meaningful
DEFAULT_VALUES = {
    'address_line2': None,
    'postal_code': None,
    'email': None,
    'phone': None,
    'mobile': None,
    'employee_count': None,
    'annual_turnover': None
}


def extract_party_data_from_l2(mysql_conn) -> List[Dict[str, Any]]:
    """
    Extract party data from MySQL Layer 2.

    Args:
        mysql_conn: MySQL database connection

    Returns:
        List[Dict]: Party data records
    """
    logger.info("Extracting party data from L2 MySQL...")

    # SQL query based on Phase B enhanced L2 schema
    # Now party.party has TIN, industry_code, legal_form_code, and location codes!
    query = """
        SELECT
            p.party_id,
            -- Phase B: TIN now in party.party for all parties
            p.tax_identification_number AS tin,
            UPPER(p.party_type_code) AS party_type,
            p.party_name,
            p.party_short_name,
            COALESCE(p.party_status_code, 'ACTIVE') AS party_status,

            -- Phase B: Industry from party.party
            p.industry_code,
            ind.industry_name,

            -- Risk assessment
            COALESCE(r.risk_rating_code, 'UNKNOWN') AS risk_rating,
            COALESCE(r.overall_risk_score, 0.0) AS risk_score,

            -- Individual demographics (NULL for enterprises)
            i.birth_date,
            UPPER(i.gender_code) AS gender,
            i.marital_status_code AS marital_status,

            -- Phase B: Enterprise attributes from party.party
            p.legal_form_code AS legal_form,
            p.registration_date,

            -- Phase B: Location from party.party
            p.country_code,
            p.district_code,
            p.locality_code,

            -- Audit timestamps
            p.created_date,
            p.modified_date

        FROM party.party p

        -- Left join individual subtype
        LEFT JOIN party.individual i ON p.party_id = i.party_id

        -- Left join risk profile
        LEFT JOIN compliance_control.taxpayer_risk_profile r ON p.party_id = r.party_id

        -- Phase B: Join reference tables for names
        LEFT JOIN reference.ref_industry ind ON p.industry_code = ind.industry_code

        ORDER BY p.party_id
    """

    # Execute query and fetch results
    cursor = mysql_conn.get_cursor()
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        logger.info(f"Extracted {len(rows)} party records from L2")

        # Convert to list of dicts
        parties = []
        for row in rows:
            party = dict(zip(columns, row))
            parties.append(party)

        return parties

    except Exception as e:
        logger.error(f"Failed to extract party data: {e}")
        raise
    finally:
        cursor.close()


def lookup_reference_values(mysql_conn, parties: List[Dict[str, Any]]):
    """
    Look up reference data values (country, district, locality names).

    Args:
        mysql_conn: MySQL database connection
        parties: List of party dictionaries (modified in-place)
    """
    logger.info("Looking up reference data values...")

    # Build lookup caches
    countries = {}
    districts = {}
    localities = {}

    try:
        # Countries (country_code in party.party is alpha-3)
        country_rows = mysql_conn.fetch_all(
            "SELECT iso_alpha_3, iso_alpha_2 FROM reference.ref_country"
        )
        for alpha3, alpha2 in country_rows:
            countries[alpha3] = alpha2

        # Districts
        district_rows = mysql_conn.fetch_all(
            "SELECT district_code, district_name FROM reference.ref_district"
        )
        for code, name in district_rows:
            districts[code] = name

        # Localities
        locality_rows = mysql_conn.fetch_all(
            "SELECT locality_code, locality_name FROM reference.ref_locality"
        )
        for code, name in locality_rows:
            localities[code] = name

        # Apply lookups to parties
        for party in parties:
            party['country'] = countries.get(party.get('country_code'), 'LK')
            party['district'] = districts.get(party.get('district_code'), 'Unknown')
            party['city'] = localities.get(party.get('locality_code'), 'Unknown')

    except Exception as e:
        logger.warning(f"Reference lookup failed (using defaults): {e}")
        # Use defaults if lookup fails
        for party in parties:
            party['country'] = 'LK'
            party['district'] = 'Unknown'
            party['city'] = 'Unknown'


def transform_party_data(parties: List[Dict[str, Any]], batch_id: int,
                        next_party_key: int) -> List[List[Any]]:
    """
    Transform L2 party data to L3 dimensional format.

    Args:
        parties: List of party dictionaries from L2
        batch_id: ETL batch identifier
        next_party_key: Starting surrogate key

    Returns:
        List[List]: Transformed rows ready for ClickHouse insert
    """
    logger.info(f"Transforming {len(parties)} party records for L3...")

    transformed_rows = []
    current_time = datetime.now()

    for idx, party in enumerate(parties):
        # Generate surrogate key
        party_key = next_party_key + idx

        # Derive party segment (Phase A: simplified)
        party_type = party['party_type']
        if party_type == 'INDIVIDUAL':
            party_segment = 'INDIVIDUAL'
        elif party_type == 'ENTERPRISE':
            party_segment = 'SMALL'  # Default until we have turnover data
        else:
            party_segment = 'UNKNOWN'

        # Round risk score to 2 decimal places
        risk_score = round(float(party.get('risk_score', 0.0)), 2)

        # Phase B: TIN comes from party.party (no more temp TINs!)
        tin = party.get('tin', '')

        # Phase B: Handle NULL birth_date for enterprises
        # ClickHouse Date type is not nullable, use sentinel date for enterprises
        # Also handle dates before 1970 (ClickHouse Date min value is 1970-01-01)
        raw_birth_date = party.get('birth_date')
        if raw_birth_date is None:
            birth_date = date(1970, 1, 1)  # Sentinel for enterprises
        elif raw_birth_date < date(1970, 1, 1):
            birth_date = date(1970, 1, 1)  # ClickHouse Date type minimum
            logger.warning(f"Birth date {raw_birth_date} is before 1970, using 1970-01-01 for party {party['party_id']}")
        else:
            birth_date = raw_birth_date

        # Phase B: Derive address_line1 from location (placeholder until address table exists)
        city = party.get('city', 'Unknown')
        district = party.get('district', 'Unknown')
        address_line1 = f"{city}, {district}" if city != 'Unknown' or district != 'Unknown' else 'Address Not Available'

        logger.debug(f"Party {party['party_id']}: type={party_type}, tin={tin}, birth_date={birth_date}")

        # Build row tuple matching dim_party column order
        # Note: LowCardinality(String) columns can't be None - use empty string
        row = [
            party_key,                                          # party_key
            party['party_id'],                                  # party_id
            tin,                                                # tin
            party['party_type'],                                # party_type
            party['party_name'],                                # party_name
            party['party_status'],                              # party_status
            party_segment,                                      # party_segment
            party.get('industry_code') or '',                   # industry_code (empty string if NULL)
            party.get('industry_name') or '',                   # industry_name (empty string if NULL)
            party.get('risk_rating') or 'UNKNOWN',              # risk_rating
            risk_score,                                         # risk_score
            birth_date,                                         # birth_date (sentinel 1900-01-01 for enterprises)
            party.get('gender') or '',                          # gender (empty string if NULL)
            party.get('legal_form') or '',                      # legal_form (empty string if NULL)
            party.get('registration_date'),                     # registration_date
            DEFAULT_VALUES['employee_count'],                   # employee_count
            DEFAULT_VALUES['annual_turnover'],                  # annual_turnover
            address_line1,                                      # address_line1 (derived from location)
            DEFAULT_VALUES['address_line2'],                    # address_line2
            party.get('city', 'Unknown'),                       # city
            party.get('district', 'Unknown'),                   # district
            DEFAULT_VALUES['postal_code'],                      # postal_code
            party.get('country', 'LK'),                         # country
            DEFAULT_VALUES['email'],                            # email
            DEFAULT_VALUES['phone'],                            # phone
            DEFAULT_VALUES['mobile'],                           # mobile
            current_time,                                       # valid_from
            None,                                               # valid_to (NULL = current)
            1,                                                  # is_current
            'ETL_SYSTEM',                                       # created_by
            current_time,                                       # created_date
            None,                                               # updated_by
            None,                                               # updated_date
            batch_id                                            # etl_batch_id
        ]

        transformed_rows.append(row)

    logger.info(f"Transformed {len(transformed_rows)} rows for L3")
    return transformed_rows


def load_to_clickhouse(ch_conn, rows: List[List[Any]]) -> int:
    """
    Load transformed data into ClickHouse dim_party table.

    Args:
        ch_conn: ClickHouse connection
        rows: Transformed rows

    Returns:
        int: Number of rows loaded
    """
    logger.info(f"Loading {len(rows)} rows to ClickHouse dim_party...")

    # Column names (must match dim_party schema order)
    columns = [
        'party_key', 'party_id', 'tin', 'party_type', 'party_name',
        'party_status', 'party_segment', 'industry_code', 'industry_name',
        'risk_rating', 'risk_score', 'birth_date', 'gender', 'legal_form',
        'registration_date', 'employee_count', 'annual_turnover',
        'address_line1', 'address_line2', 'city', 'district', 'postal_code',
        'country', 'email', 'phone', 'mobile', 'valid_from', 'valid_to',
        'is_current', 'created_by', 'created_date', 'updated_by',
        'updated_date', 'etl_batch_id'
    ]

    try:
        ch_conn.insert('dim_party', rows, column_names=columns)
        logger.info(f"Successfully loaded {len(rows)} rows to dim_party")
        return len(rows)

    except Exception as e:
        logger.error(f"Failed to load data to ClickHouse: {e}")
        raise


def validate_load(ch_conn, batch_id: int) -> Dict[str, Any]:
    """
    Validate the loaded data in ClickHouse.

    Args:
        ch_conn: ClickHouse connection
        batch_id: ETL batch identifier

    Returns:
        Dict: Validation statistics
    """
    logger.info("Validating loaded data...")

    stats = {}

    # Total row count
    total_rows = ch_conn.get_table_row_count('dim_party')
    stats['total_rows'] = total_rows

    # Rows for this batch
    batch_query = f"SELECT count() FROM dim_party WHERE etl_batch_id = {batch_id}"
    result = ch_conn.query(batch_query)
    stats['batch_rows'] = result[0][0] if result else 0

    # Count by party type
    type_query = "SELECT party_type, count() FROM dim_party GROUP BY party_type ORDER BY party_type"
    result = ch_conn.query(type_query)
    stats['by_type'] = {row[0]: row[1] for row in result}

    # Count by risk rating
    risk_query = "SELECT risk_rating, count() FROM dim_party GROUP BY risk_rating ORDER BY risk_rating"
    result = ch_conn.query(risk_query)
    stats['by_risk'] = {row[0]: row[1] for row in result}

    # Current vs historical
    current_query = "SELECT is_current, count() FROM dim_party GROUP BY is_current"
    result = ch_conn.query(current_query)
    stats['by_currency'] = {('Current' if row[0] == 1 else 'Historical'): row[1] for row in result}

    logger.info(f"Validation complete: {stats}")
    return stats


def print_summary(stats: Dict[str, Any], elapsed_time: float):
    """Print ETL summary report."""
    print()
    print("=" * 80)
    print("ETL SUMMARY REPORT")
    print("=" * 80)
    print(f"Total rows in dim_party: {stats['total_rows']:,}")
    print(f"Rows loaded in this batch: {stats['batch_rows']:,}")
    print()
    print("Party Type Distribution:")
    for ptype, count in stats['by_type'].items():
        print(f"  {ptype:<20} {count:>5,} rows")
    print()
    print("Risk Rating Distribution:")
    for rating, count in stats['by_risk'].items():
        print(f"  {rating:<20} {count:>5,} rows")
    print()
    print("Record Currency:")
    for currency, count in stats['by_currency'].items():
        print(f"  {currency:<20} {count:>5,} rows")
    print()
    print(f"Elapsed Time: {elapsed_time:.2f} seconds")
    print("=" * 80)
    print()


def main():
    """Main ETL entry point."""
    parser = argparse.ArgumentParser(
        description='L2 → L3 Party Dimension ETL'
    )

    parser.add_argument(
        '--batch-id',
        type=int,
        help='ETL batch identifier (auto-generated if not specified)'
    )

    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Truncate dim_party table before loading'
    )

    args = parser.parse_args()

    # Print banner
    print()
    print("=" * 80)
    print("L2 → L3 PARTY DIMENSION ETL")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source (L2): {get_mysql_conn_str()}")
    print(f"Target (L3): {get_ch_conn_str()}")
    print("=" * 80)
    print()

    start_time = datetime.now()

    try:
        # Connect to both databases
        with get_db_connection() as mysql_conn, get_clickhouse_connection() as ch_conn:

            # Truncate if requested
            if args.truncate:
                logger.info("Truncating dim_party table...")
                ch_conn.truncate_table('dim_party')

            # Generate batch ID
            if args.batch_id:
                batch_id = args.batch_id
            else:
                max_batch = ch_conn.get_max_value('dim_party', 'etl_batch_id')
                batch_id = (max_batch or 0) + 1

            logger.info(f"ETL Batch ID: {batch_id}")

            # Get next party_key
            max_party_key = ch_conn.get_max_value('dim_party', 'party_key')
            next_party_key = (max_party_key or 0) + 1
            logger.info(f"Next party_key: {next_party_key}")

            # EXTRACT: Get party data from L2
            parties = extract_party_data_from_l2(mysql_conn)

            if not parties:
                logger.warning("No party data found in L2")
                print("⚠️  No data to load")
                return 0

            # Look up reference data
            lookup_reference_values(mysql_conn, parties)

            # TRANSFORM: Convert to L3 format
            transformed_rows = transform_party_data(parties, batch_id, next_party_key)

            # LOAD: Insert into ClickHouse
            loaded_count = load_to_clickhouse(ch_conn, transformed_rows)

            # VALIDATE: Check loaded data
            stats = validate_load(ch_conn, batch_id)

            # Print summary
            elapsed = (datetime.now() - start_time).total_seconds()
            print_summary(stats, elapsed)

            print(f"✅ ETL completed successfully!")
            print(f"   Loaded {loaded_count:,} party records to dim_party")
            print()

            return 0

    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        print()
        print("=" * 80)
        print("❌ ETL FAILED")
        print(f"Error: {e}")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
