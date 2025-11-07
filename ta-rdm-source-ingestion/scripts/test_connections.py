#!/usr/bin/env python3
"""
Test Database Connections

Tests connectivity to all configured databases:
- L2 MySQL Canonical Database
- Configuration MySQL Database
- RAMIS SQL Server (source)
- Staging MySQL Database

Usage:
    python scripts/test_connections.py
    python scripts/test_connections.py --db l2        # Test specific database
    python scripts/test_connections.py --verbose      # Show detailed info
"""

import sys
import os
import argparse
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_utils import setup_logging
from utils.db_utils import DatabaseConnection, DatabaseType
from config.database_config import (
    L2_DB_CONFIG, CONFIG_DB_CONFIG, RAMIS_DB_CONFIG, STAGING_DB_CONFIG,
    get_connection_string
)

# Setup logging
logger = setup_logging(name='test_connections', log_level='INFO')


def test_mysql_connection(name: str, config: Dict[str, Any], verbose: bool = False) -> bool:
    """
    Test MySQL database connection.

    Args:
        name: Database name (for logging)
        config: Database configuration
        verbose: Show detailed information

    Returns:
        bool: True if connection successful
    """
    logger.info(f"\nTesting {name}...")
    logger.info(f"  Host: {config['host']}:{config['port']}")
    logger.info(f"  User: {config['user']}")

    if config.get('database'):
        logger.info(f"  Database: {config['database']}")

    try:
        db = DatabaseConnection(DatabaseType.MYSQL, config)
        db.connect()

        # Get MySQL version
        version_row = db.fetch_one("SELECT VERSION()")
        version = version_row[0] if version_row else "Unknown"

        # Get current database
        db_row = db.fetch_one("SELECT DATABASE()")
        current_db = db_row[0] if db_row else "None"

        logger.info(f"  ✓ Connection successful!")
        logger.info(f"  MySQL Version: {version}")
        logger.info(f"  Current Database: {current_db}")

        if verbose:
            # Get additional info
            user_row = db.fetch_one("SELECT CURRENT_USER()")
            current_user = user_row[0] if user_row else "Unknown"

            charset_row = db.fetch_one(
                "SELECT @@character_set_database, @@collation_database"
            )
            charset = charset_row[0] if charset_row else "Unknown"
            collation = charset_row[1] if charset_row else "Unknown"

            logger.info(f"  Current User: {current_user}")
            logger.info(f"  Character Set: {charset}")
            logger.info(f"  Collation: {collation}")

            # Test permissions
            try:
                db.execute_query("CREATE TABLE IF NOT EXISTS _test_connection (id INT)")
                db.execute_query("DROP TABLE IF EXISTS _test_connection")
                db.commit()
                logger.info(f"  Permissions: CREATE, DROP - OK")
            except Exception as e:
                logger.warning(f"  Permissions: Limited - {e}")

        db.disconnect()
        return True

    except Exception as e:
        logger.error(f"  ✗ Connection failed: {e}")
        return False


def test_sqlserver_connection(name: str, config: Dict[str, Any], verbose: bool = False) -> bool:
    """
    Test SQL Server database connection.

    Args:
        name: Database name (for logging)
        config: Database configuration
        verbose: Show detailed information

    Returns:
        bool: True if connection successful
    """
    logger.info(f"\nTesting {name}...")
    logger.info(f"  Host: {config['host']}:{config['port']}")
    logger.info(f"  User: {config['user']}")
    logger.info(f"  Database: {config.get('database', 'master')}")

    try:
        db = DatabaseConnection(DatabaseType.SQL_SERVER, config)
        db.connect()

        # Get SQL Server version
        version_row = db.fetch_one("SELECT @@VERSION")
        version = version_row[0] if version_row else "Unknown"
        # Shorten version string
        version_short = version.split('\n')[0][:80] if version else "Unknown"

        logger.info(f"  ✓ Connection successful!")
        logger.info(f"  SQL Server: {version_short}")

        if verbose:
            # Get current database
            db_row = db.fetch_one("SELECT DB_NAME()")
            current_db = db_row[0] if db_row else "Unknown"

            # Get server name
            server_row = db.fetch_one("SELECT @@SERVERNAME")
            server_name = server_row[0] if server_row else "Unknown"

            logger.info(f"  Server Name: {server_name}")
            logger.info(f"  Current Database: {current_db}")

            # Test SELECT permission
            try:
                db.fetch_one("SELECT GETDATE()")
                logger.info(f"  Permissions: SELECT - OK")
            except Exception as e:
                logger.warning(f"  Permissions: Limited - {e}")

        db.disconnect()
        return True

    except ImportError as e:
        logger.error(f"  ✗ pymssql not installed: {e}")
        logger.error(f"  Install with: pip install pymssql")
        return False

    except Exception as e:
        logger.error(f"  ✗ Connection failed: {e}")
        return False


def test_all_connections(verbose: bool = False):
    """
    Test all database connections.

    Args:
        verbose: Show detailed information
    """
    logger.info("=" * 80)
    logger.info("TA-RDM Source Ingestion - Database Connection Tests")
    logger.info("=" * 80)

    results = {}

    # Test L2 MySQL
    results['L2 Canonical (MySQL)'] = test_mysql_connection(
        'L2 Canonical Database (MySQL)',
        L2_DB_CONFIG,
        verbose
    )

    # Test Config MySQL
    results['Config (MySQL)'] = test_mysql_connection(
        'Configuration Database (MySQL)',
        CONFIG_DB_CONFIG,
        verbose
    )

    # Test Staging MySQL
    results['Staging (MySQL)'] = test_mysql_connection(
        'Staging Database (MySQL)',
        STAGING_DB_CONFIG,
        verbose
    )

    # Test RAMIS SQL Server
    results['RAMIS (SQL Server)'] = test_sqlserver_connection(
        'RAMIS Source Database (SQL Server)',
        RAMIS_DB_CONFIG,
        verbose
    )

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("Connection Test Summary")
    logger.info("=" * 80)

    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)

    for db_name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"  {db_name:<30} {status}")

    logger.info("=" * 80)
    logger.info(f"Results: {success_count}/{total_count} connections successful")
    logger.info("=" * 80)

    if success_count == total_count:
        logger.info("\n✓ All database connections are working!")
        logger.info("\nNext steps:")
        logger.info("  1. Initialize configuration database:")
        logger.info("     python scripts/setup_config_db.py")
        logger.info("  2. Import mapping configurations:")
        logger.info("     python scripts/import_mappings.py --config metadata/mappings/ramis_example.yaml")
    else:
        logger.warning("\n⚠ Some database connections failed")
        logger.warning("Please check your .env configuration and database credentials")

    return success_count == total_count


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Test TA-RDM Source Ingestion database connections'
    )
    parser.add_argument(
        '--db',
        type=str,
        choices=['l2', 'config', 'ramis', 'staging', 'all'],
        default='all',
        help='Database to test (default: all)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed connection information'
    )

    args = parser.parse_args()

    try:
        if args.db == 'all':
            success = test_all_connections(args.verbose)
        elif args.db == 'l2':
            success = test_mysql_connection('L2 Canonical (MySQL)', L2_DB_CONFIG, args.verbose)
        elif args.db == 'config':
            success = test_mysql_connection('Configuration (MySQL)', CONFIG_DB_CONFIG, args.verbose)
        elif args.db == 'staging':
            success = test_mysql_connection('Staging (MySQL)', STAGING_DB_CONFIG, args.verbose)
        elif args.db == 'ramis':
            success = test_sqlserver_connection('RAMIS (SQL Server)', RAMIS_DB_CONFIG, args.verbose)
        else:
            logger.error(f"Unknown database: {args.db}")
            sys.exit(1)

        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
