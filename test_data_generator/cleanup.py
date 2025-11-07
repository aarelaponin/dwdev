#!/usr/bin/env python3
"""
Cleanup utility for TA-RDM Test Data Generator.

Deletes all generated test data from the database to allow re-running
the generator from a clean state.

CAUTION: This will delete ALL data from test tables!
Only run on test/development databases!

Usage:
    python cleanup.py                    # Interactive mode (asks for confirmation)
    python cleanup.py --force            # Force cleanup without confirmation
    python cleanup.py --verify-only      # Only show what would be deleted
"""

import argparse
import sys
import logging
from datetime import datetime

from utils.db_utils import get_db_connection
from config.database_config import get_connection_string


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


# Tables to clean in order (reverse dependency order)
CLEANUP_ORDER = [
    # Compliance control (depends on party)
    ('compliance_control', 'taxpayer_risk_profile'),

    # Party subtypes (depend on party.party)
    ('party', 'individual'),

    # Main party table
    ('party', 'party'),

    # Reference tables (reverse dependency order)
    ('reference', 'ref_payment_method'),
    ('reference', 'ref_legal_form'),
    ('reference', 'ref_industry'),
    ('reference', 'ref_marital_status'),
    ('reference', 'ref_gender'),
    ('reference', 'ref_currency'),
    ('reference', 'ref_locality'),
    ('reference', 'ref_district'),
    ('reference', 'ref_region'),
    ('reference', 'ref_country'),
]


def get_table_counts(db):
    """
    Get current record counts for all tables.

    Args:
        db: Database connection

    Returns:
        dict: Mapping of (schema, table) to count
    """
    counts = {}

    for schema, table in CLEANUP_ORDER:
        try:
            count = db.get_table_row_count(schema, table)
            counts[(schema, table)] = count
        except Exception as e:
            logger.warning(f"Could not get count for {schema}.{table}: {e}")
            counts[(schema, table)] = '?'

    return counts


def print_table_counts(counts, title="Table Record Counts"):
    """
    Print table counts in a formatted table.

    Args:
        counts: Dict of (schema, table) to count
        title: Title for the table
    """
    print()
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)
    print(f"{'Schema':<25} {'Table':<35} {'Records':>10}")
    print("-" * 80)

    total = 0
    for (schema, table), count in counts.items():
        if isinstance(count, int):
            total += count
            count_str = f"{count:,}"
        else:
            count_str = str(count)

        print(f"{schema:<25} {table:<35} {count_str:>10}")

    print("-" * 80)
    if isinstance(total, int):
        print(f"{'TOTAL':<60} {total:>10,}")
    print("=" * 80)
    print()


def cleanup_database(db, force=False):
    """
    Delete all test data from the database.

    Args:
        db: Database connection
        force: If True, skip confirmation prompt

    Returns:
        bool: True if cleanup was successful
    """
    # Get current counts
    logger.info("Checking current database state...")
    before_counts = get_table_counts(db)

    # Print what will be deleted
    print_table_counts(before_counts, "Current Database State (Before Cleanup)")

    # Calculate total records
    total_records = sum(c for c in before_counts.values() if isinstance(c, int))

    if total_records == 0:
        logger.info("Database is already clean (no records found)")
        return True

    # Confirmation prompt
    if not force:
        print()
        print("⚠️  WARNING: This will DELETE all test data from the tables above!")
        print(f"   Total records to delete: {total_records:,}")
        print()
        response = input("Are you sure you want to continue? (yes/no): ").strip().lower()

        if response not in ['yes', 'y']:
            logger.info("Cleanup cancelled by user")
            return False

    # Perform cleanup
    logger.info("Starting database cleanup...")

    try:
        # Disable foreign key checks
        logger.debug("Disabling foreign key checks...")
        db.execute_query("SET FOREIGN_KEY_CHECKS = 0")

        # Delete from each table
        deleted_count = 0
        for schema, table in CLEANUP_ORDER:
            count = before_counts.get((schema, table), 0)
            if isinstance(count, int) and count > 0:
                logger.info(f"Deleting {count:,} records from {schema}.{table}...")
                db.execute_query(f"DELETE FROM `{schema}`.`{table}`")
                deleted_count += count

        # Re-enable foreign key checks
        logger.debug("Re-enabling foreign key checks...")
        db.execute_query("SET FOREIGN_KEY_CHECKS = 1")

        # Commit changes
        db.commit()
        logger.info(f"Successfully deleted {deleted_count:,} records")

        # Verify cleanup
        logger.info("Verifying cleanup...")
        after_counts = get_table_counts(db)
        print_table_counts(after_counts, "Database State After Cleanup")

        # Check if all tables are empty
        remaining = sum(c for c in after_counts.values() if isinstance(c, int))
        if remaining > 0:
            logger.warning(f"Warning: {remaining} records still remain in database")
            return False

        logger.info("✅ Database cleanup completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        db.rollback()
        logger.error("Cleanup failed - changes rolled back")
        return False


def verify_only(db):
    """
    Show what would be deleted without actually deleting.

    Args:
        db: Database connection
    """
    logger.info("Verification mode - checking database state...")
    counts = get_table_counts(db)
    print_table_counts(counts, "Current Database State (Verify Only)")

    total_records = sum(c for c in counts.values() if isinstance(c, int))

    if total_records == 0:
        logger.info("✅ Database is clean (no test data found)")
    else:
        logger.info(f"ℹ️  Found {total_records:,} test records that would be deleted")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Cleanup utility for TA-RDM Test Data Generator'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force cleanup without confirmation prompt'
    )

    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only show what would be deleted (no actual deletion)'
    )

    args = parser.parse_args()

    # Print banner
    print()
    print("=" * 80)
    print("TA-RDM TEST DATA CLEANUP UTILITY")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {get_connection_string()}")
    print("=" * 80)

    try:
        with get_db_connection() as db:
            if args.verify_only:
                verify_only(db)
                return 0
            else:
                success = cleanup_database(db, force=args.force)
                return 0 if success else 1

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print()
        print("=" * 80)
        print("CLEANUP FAILED")
        print(f"Error: {e}")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
