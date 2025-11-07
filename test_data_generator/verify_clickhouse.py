#!/usr/bin/env python3
"""
ClickHouse Connection Verification Script

Tests connection to ClickHouse Layer 3 data warehouse and displays
database structure and table information.

Usage:
    python verify_clickhouse.py
    python verify_clickhouse.py --show-schema dim_party
"""

import argparse
import sys
import logging
from datetime import datetime
from typing import List, Dict

from utils.clickhouse_utils import get_clickhouse_connection, ClickHouseConnection
from config.clickhouse_config import get_connection_string, CLICKHOUSE_CONFIG


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def print_banner():
    """Print script banner."""
    print()
    print("=" * 80)
    print("CLICKHOUSE LAYER 3 CONNECTION VERIFICATION")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {get_connection_string()}")
    print("=" * 80)
    print()


def test_connection(ch: ClickHouseConnection) -> bool:
    """
    Test basic connectivity to ClickHouse.

    Args:
        ch: ClickHouse connection

    Returns:
        bool: True if connection is successful
    """
    try:
        logger.info("Testing ClickHouse connection...")

        # Get server version
        result = ch.query("SELECT version()")
        version = result[0][0] if result else "Unknown"

        # Get database info
        db_query = "SELECT currentDatabase()"
        result = ch.query(db_query)
        current_db = result[0][0] if result else "Unknown"

        print("✅ Connection Test: SUCCESS")
        print(f"   Server Version: {version}")
        print(f"   Current Database: {current_db}")
        print()

        return True

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        print("❌ Connection Test: FAILED")
        print(f"   Error: {e}")
        print()
        return False


def list_all_tables(ch: ClickHouseConnection) -> List[str]:
    """
    List all tables in the database.

    Args:
        ch: ClickHouse connection

    Returns:
        List[str]: List of table names
    """
    try:
        logger.info("Listing all tables in database...")

        tables = ch.list_tables()

        print("=" * 80)
        print(f"TABLES IN DATABASE '{CLICKHOUSE_CONFIG['database']}'")
        print("=" * 80)

        if not tables:
            print("No tables found in database")
        else:
            print(f"Total: {len(tables)} tables\n")

            # Group tables by type (dimension vs fact)
            dimensions = [t for t in tables if t.startswith('dim_')]
            facts = [t for t in tables if t.startswith('fact_')]
            others = [t for t in tables if not t.startswith('dim_') and not t.startswith('fact_')]

            if dimensions:
                print(f"📊 Dimension Tables ({len(dimensions)}):")
                for i, table in enumerate(dimensions, 1):
                    row_count = ch.get_table_row_count(table)
                    print(f"   {i:2}. {table:<35} ({row_count:,} rows)")
                print()

            if facts:
                print(f"📈 Fact Tables ({len(facts)}):")
                for i, table in enumerate(facts, 1):
                    row_count = ch.get_table_row_count(table)
                    print(f"   {i:2}. {table:<35} ({row_count:,} rows)")
                print()

            if others:
                print(f"📋 Other Tables ({len(others)}):")
                for i, table in enumerate(others, 1):
                    row_count = ch.get_table_row_count(table)
                    print(f"   {i:2}. {table:<35} ({row_count:,} rows)")
                print()

        print("=" * 80)
        print()

        return tables

    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        print(f"❌ Error listing tables: {e}")
        print()
        return []


def show_table_schema(ch: ClickHouseConnection, table_name: str):
    """
    Display the schema of a specific table.

    Args:
        ch: ClickHouse connection
        table_name: Name of table to inspect
    """
    try:
        logger.info(f"Fetching schema for table '{table_name}'...")

        # Check if table exists
        if not ch.table_exists(table_name):
            print(f"❌ Table '{table_name}' does not exist in database '{CLICKHOUSE_CONFIG['database']}'")
            return

        # Get table schema
        columns = ch.get_table_schema(table_name)

        # Get row count
        row_count = ch.get_table_row_count(table_name)

        print("=" * 80)
        print(f"SCHEMA FOR TABLE '{table_name}'")
        print("=" * 80)
        print(f"Row Count: {row_count:,}")
        print()
        print(f"{'Column Name':<30} {'Data Type':<25} {'Comment':<25}")
        print("-" * 80)

        for col in columns:
            name = col['name']
            dtype = col['type']
            comment = col['comment'][:22] + '...' if len(col['comment']) > 25 else col['comment']
            print(f"{name:<30} {dtype:<25} {comment:<25}")

        print("=" * 80)
        print()

    except Exception as e:
        logger.error(f"Failed to show schema for table '{table_name}': {e}")
        print(f"❌ Error: {e}")
        print()


def check_key_tables(ch: ClickHouseConnection):
    """
    Check for key tables that we'll need for ETL.

    Args:
        ch: ClickHouse connection
    """
    key_tables = ['dim_party', 'dim_tax_type', 'dim_date', 'dim_geography']

    print("=" * 80)
    print("KEY TABLES CHECK (for L2→L3 ETL)")
    print("=" * 80)

    for table in key_tables:
        exists = ch.table_exists(table)
        if exists:
            row_count = ch.get_table_row_count(table)
            print(f"   ✅ {table:<30} (exists, {row_count:,} rows)")
        else:
            print(f"   ❌ {table:<30} (missing)")

    print("=" * 80)
    print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Verify ClickHouse connection and inspect database structure'
    )

    parser.add_argument(
        '--show-schema',
        metavar='TABLE_NAME',
        help='Show detailed schema for a specific table'
    )

    args = parser.parse_args()

    print_banner()

    try:
        with get_clickhouse_connection() as ch:
            # Test connection
            if not test_connection(ch):
                return 1

            # If specific table requested, show its schema
            if args.show_schema:
                show_table_schema(ch, args.show_schema)
                return 0

            # Otherwise, list all tables and check key tables
            list_all_tables(ch)
            check_key_tables(ch)

            print("✅ Verification complete!")
            print()
            return 0

    except Exception as e:
        logger.error(f"Verification failed: {e}", exc_info=True)
        print()
        print("=" * 80)
        print("VERIFICATION FAILED")
        print(f"Error: {e}")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
