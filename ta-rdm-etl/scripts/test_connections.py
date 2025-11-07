#!/usr/bin/env python3
"""
TA-RDM Connection Testing Utility

Tests database connectivity for MySQL L2 and ClickHouse L3.
Run this script after installation to verify database configurations.

Usage:
    python scripts/test_connections.py
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from utils.db_utils import get_db_connection
from utils.clickhouse_utils import get_clickhouse_connection
from config.database_config import get_connection_string
from config.clickhouse_config import CLICKHOUSE_CONFIG, get_connection_string as get_ch_conn_str

# Colors for output
try:
    from colorama import init, Fore, Style
    init()
    GREEN = Fore.GREEN
    RED = Fore.RED
    YELLOW = Fore.YELLOW
    BLUE = Fore.BLUE
    RESET = Style.RESET_ALL
except ImportError:
    GREEN = RED = YELLOW = BLUE = RESET = ''


def print_header():
    """Print test header."""
    print("=" * 80)
    print("TA-RDM DATABASE CONNECTION TESTING UTILITY")
    print("=" * 80)
    print()


def test_mysql_connection():
    """
    Test MySQL L2 connection.

    Returns:
        bool: True if connection successful, False otherwise
    """
    print(f"{BLUE}Testing MySQL L2 Connection...{RESET}")
    print("-" * 80)

    try:
        # Get connection string (without password)
        conn_str = get_connection_string()
        print(f"Connection: {conn_str}")
        print()

        # Try to connect
        with get_db_connection() as db:
            print(f"{GREEN}✓ MySQL connection established{RESET}")

            # Test query
            result = db.execute_query("SELECT VERSION() as version")
            if result:
                version = result[0]['version']
                print(f"  MySQL Version: {version}")

            # Check for required schemas
            schemas_to_check = [
                'reference',
                'party',
                'registration',
                'tax_framework',
                'filing_assessment',
                'payment_refund',
                'accounting',
                'compliance_control'
            ]

            print(f"\n  Checking required schemas:")
            missing_schemas = []

            for schema in schemas_to_check:
                try:
                    tables = db.execute_query(f"SHOW TABLES FROM {schema}")
                    table_count = len(tables) if tables else 0
                    if table_count > 0:
                        print(f"  {GREEN}✓{RESET} {schema} ({table_count} tables)")
                    else:
                        print(f"  {YELLOW}⚠{RESET} {schema} (0 tables - may be empty)")
                        missing_schemas.append(schema)
                except Exception as e:
                    print(f"  {RED}✗{RESET} {schema} (not accessible: {e})")
                    missing_schemas.append(schema)

            if missing_schemas:
                print(f"\n  {YELLOW}Warning: Some schemas are missing or inaccessible{RESET}")
                print(f"  Missing: {', '.join(missing_schemas)}")
                print(f"  Ensure MySQL L2 schema is created before running ETL")

            print(f"\n{GREEN}✓ MySQL L2 connection successful{RESET}")
            print()
            return True

    except Exception as e:
        print(f"{RED}✗ MySQL connection failed{RESET}")
        print(f"Error: {e}")
        print()
        print("Troubleshooting:")
        print("1. Verify MySQL is running")
        print("2. Check DB_HOST, DB_PORT, DB_USER, DB_PASSWORD in .env")
        print("3. Verify network connectivity to MySQL server")
        print("4. Check MySQL user has sufficient privileges")
        print()
        return False


def test_clickhouse_connection():
    """
    Test ClickHouse L3 connection.

    Returns:
        bool: True if connection successful, False otherwise
    """
    print(f"{BLUE}Testing ClickHouse L3 Connection...{RESET}")
    print("-" * 80)

    try:
        # Get connection config
        config = CLICKHOUSE_CONFIG
        print(f"Connection: {get_ch_conn_str()}")
        print()

        # Try to connect
        with get_clickhouse_connection() as ch:
            print(f"{GREEN}✓ ClickHouse connection established{RESET}")

            # Test query
            result = ch.query("SELECT version() as version")
            if result:
                print(f"  ClickHouse Version: {result[0][0]}")

            # Check if database exists
            db_name = config['database']
            databases_result = ch.query(f"SHOW DATABASES")
            databases = [row[0] for row in databases_result]

            if db_name in databases:
                print(f"  {GREEN}✓{RESET} Database '{db_name}' exists")

                # Check for dimension and fact tables
                tables_result = ch.query(f"SHOW TABLES FROM {db_name}")
                tables = [row[0] for row in tables_result]

                dim_tables = [t for t in tables if t.startswith('dim_')]
                fact_tables = [t for t in tables if t.startswith('fact_')]

                print(f"\n  Table counts:")
                print(f"  - Dimension tables: {len(dim_tables)}")
                print(f"  - Fact tables: {len(fact_tables)}")

                if len(dim_tables) == 0 and len(fact_tables) == 0:
                    print(f"\n  {YELLOW}⚠ Warning: No tables found in database{RESET}")
                    print(f"  Run ETL scripts to populate data warehouse")
                else:
                    print(f"\n  Sample tables:")
                    for table in (dim_tables + fact_tables)[:5]:
                        row_count = ch.get_table_row_count(table, db_name)
                        print(f"  - {table}: {row_count:,} rows")
            else:
                print(f"  {RED}✗{RESET} Database '{db_name}' does not exist")
                print(f"  Available databases: {', '.join(databases)}")
                print(f"\n  Create database: CREATE DATABASE {db_name}")

        print(f"\n{GREEN}✓ ClickHouse L3 connection successful{RESET}")
        print()
        return True

    except Exception as e:
        print(f"{RED}✗ ClickHouse connection failed{RESET}")
        print(f"Error: {e}")
        print()
        print("Troubleshooting:")
        print("1. Verify ClickHouse is running")
        print("2. Check CH_HOST, CH_PORT, CH_DATABASE in .env")
        print("3. Test with: curl http://$CH_HOST:$CH_PORT/ping")
        print("4. Verify HTTP interface is enabled in ClickHouse config")
        print()
        return False


def main():
    """Main entry point."""
    print_header()

    # Test MySQL
    mysql_ok = test_mysql_connection()

    # Test ClickHouse
    clickhouse_ok = test_clickhouse_connection()

    # Summary
    print("=" * 80)
    print("CONNECTION TEST SUMMARY")
    print("=" * 80)

    if mysql_ok:
        print(f"{GREEN}✓ MySQL L2 connection: OK{RESET}")
    else:
        print(f"{RED}✗ MySQL L2 connection: FAILED{RESET}")

    if clickhouse_ok:
        print(f"{GREEN}✓ ClickHouse L3 connection: OK{RESET}")
    else:
        print(f"{RED}✗ ClickHouse L3 connection: FAILED{RESET}")

    print("=" * 80)
    print()

    if mysql_ok and clickhouse_ok:
        print(f"{GREEN}✓ All database connections OK{RESET}")
        print()
        print("Next steps:")
        print("  1. Run ETL pipeline: ./scripts/run_full_etl.sh")
        print("  2. Or run individual ETL: python etl/l2_to_l3_party.py")
        print("  3. Validate data: python etl/validate_etl.py")
        print()
        return 0
    else:
        print(f"{RED}✗ Some database connections failed{RESET}")
        print()
        print("Please fix connection issues before running ETL")
        print("See INSTALLATION.md and TROUBLESHOOTING.md for help")
        print()
        return 1


if __name__ == '__main__':
    sys.exit(main())
