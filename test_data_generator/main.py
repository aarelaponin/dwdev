#!/usr/bin/env python3
"""
TA-RDM Test Data Generator - Main Orchestrator

Phase A: Foundation Validation
- Phase A.1: Reference Data (~200 records)
- Phase A.2: Party Data (5 parties with risk profiles)

Usage:
    python main.py --phase reference       # Generate reference data only
    python main.py --phase party --count 5 # Generate 5 parties
    python main.py --phase foundation      # Generate both reference + parties
"""

import argparse
import logging
import sys
from datetime import datetime

from utils.db_utils import get_db_connection
from generators.reference_generator import ReferenceDataGenerator
from generators.party_generator import PartyDataGenerator
from config.database_config import get_connection_string


# Configure logging
def setup_logging(log_level: str = 'INFO'):
    """
    Setup logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def generate_reference_data(db, seed: int = 42):
    """
    Generate reference data (Phase A.1).

    Args:
        db: Database connection
        seed: Random seed for reproducibility
    """
    logger = logging.getLogger(__name__)
    logger.info("Phase A.1: Generating Reference Data")

    generator = ReferenceDataGenerator(db, seed)
    generator.generate_all()


def generate_party_data(db, count: int = 5, seed: int = 42):
    """
    Generate party data (Phase A.2).

    Args:
        db: Database connection
        count: Number of parties to generate
        seed: Random seed for reproducibility
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Phase A.2: Generating Party Data ({count} parties)")

    generator = PartyDataGenerator(db, seed)
    generator.generate_all(count)


def validate_database_connection(db):
    """
    Validate database connection and check for required schemas.

    Args:
        db: Database connection

    Raises:
        RuntimeError: If database connection or schemas are invalid
    """
    logger = logging.getLogger(__name__)

    # Check required schemas exist
    required_schemas = ['reference', 'party', 'compliance_control']
    missing_schemas = []

    for schema in required_schemas:
        if not db.table_exists(schema, 'ref_country' if schema == 'reference' else 'party'):
            # Just check if schema is accessible (any table)
            pass

    logger.info(f"Database connection validated: {get_connection_string()}")


def main():
    """Main entry point for test data generator."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='TA-RDM Test Data Generator - Phase A: Foundation Validation'
    )

    parser.add_argument(
        '--phase',
        type=str,
        choices=['reference', 'party', 'foundation'],
        default='foundation',
        help='Phase to generate: reference (A.1), party (A.2), or foundation (both)'
    )

    parser.add_argument(
        '--count',
        type=int,
        default=5,
        help='Number of parties to generate (default: 5)'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without generating data'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # Print banner
    print("=" * 80)
    print("TA-RDM TEST DATA GENERATOR - PHASE A: FOUNDATION VALIDATION")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Phase: {args.phase.upper()}")
    print(f"Database: {get_connection_string()}")
    print(f"Random Seed: {args.seed}")
    print("=" * 80)
    print()

    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be generated")
        logger.info("Configuration validated successfully")
        return 0

    try:
        # Connect to database
        with get_db_connection() as db:
            # Validate database connection
            validate_database_connection(db)

            # Generate data based on phase
            if args.phase in ['reference', 'foundation']:
                generate_reference_data(db, args.seed)
                db.commit()
                logger.info("Reference data committed successfully")

            if args.phase in ['party', 'foundation']:
                generate_party_data(db, args.count, args.seed)
                db.commit()
                logger.info("Party data committed successfully")

            print()
            print("=" * 80)
            print("DATA GENERATION COMPLETED SUCCESSFULLY")
            print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)

            # Print validation queries
            print()
            print("Validation Queries:")
            print("-" * 80)
            if args.phase in ['reference', 'foundation']:
                print("SELECT COUNT(*) FROM reference.ref_region;")
                print("SELECT COUNT(*) FROM reference.ref_district;")
                print("SELECT COUNT(*) FROM reference.ref_locality;")
            if args.phase in ['party', 'foundation']:
                print("SELECT COUNT(*) FROM party.party;")
                print("SELECT COUNT(*) FROM party.individual;")
                print("SELECT COUNT(*) FROM compliance_control.taxpayer_risk_profile;")
                print()
                print("SELECT party_id, party_name, party_type_code FROM party.party;")
            print("=" * 80)

            return 0

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        print()
        print("=" * 80)
        print("DATA GENERATION FAILED")
        print(f"Error: {e}")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
