#!/usr/bin/env python3
"""
Setup Configuration Database

Initializes the TA-RDM Source Ingestion configuration database
by creating the schema and all 9 metadata tables.

Usage:
    python scripts/setup_config_db.py
    python scripts/setup_config_db.py --drop-existing  # Drop and recreate
    python scripts/setup_config_db.py --schema-file custom_schema.sql
"""

import sys
import os
import argparse
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_utils import setup_logging
from utils.db_utils import DatabaseConnection, DatabaseType
from config.database_config import CONFIG_DB_CONFIG

# Setup logging
logger = setup_logging(name='setup_config_db', log_level='INFO')


def read_sql_file(file_path: str) -> str:
    """
    Read SQL file content.

    Args:
        file_path: Path to SQL file

    Returns:
        str: SQL file content
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"SQL file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading SQL file: {e}")
        raise


def split_sql_statements(sql_content: str) -> list:
    """
    Split SQL content into individual statements.

    Args:
        sql_content: SQL file content

    Returns:
        List of SQL statements
    """
    # Remove comments
    lines = []
    for line in sql_content.split('\n'):
        # Remove single-line comments
        if '--' in line:
            line = line[:line.index('--')]
        lines.append(line)

    sql_content = '\n'.join(lines)

    # Split by semicolon (simple approach)
    statements = []
    for stmt in sql_content.split(';'):
        stmt = stmt.strip()
        if stmt and not stmt.startswith('--'):
            statements.append(stmt)

    return statements


def execute_sql_file(db: DatabaseConnection, sql_file: str, drop_existing: bool = False):
    """
    Execute SQL file to create database schema.

    Args:
        db: Database connection
        sql_file: Path to SQL file
        drop_existing: Drop existing schema before creation
    """
    logger.info(f"Reading SQL file: {sql_file}")
    sql_content = read_sql_file(sql_file)

    # Drop existing schema if requested
    if drop_existing:
        logger.warning("Dropping existing 'config' schema...")
        try:
            db.execute_query("DROP SCHEMA IF EXISTS config")
            db.commit()
            logger.info("Existing schema dropped")
        except Exception as e:
            logger.warning(f"Could not drop schema: {e}")

    # Split into statements
    statements = split_sql_statements(sql_content)
    logger.info(f"Found {len(statements)} SQL statements")

    # Execute each statement
    success_count = 0
    error_count = 0

    for i, stmt in enumerate(statements, 1):
        try:
            # Skip empty statements
            if not stmt.strip():
                continue

            # Log statement summary (first 100 chars)
            stmt_summary = stmt[:100].replace('\n', ' ')
            logger.debug(f"Executing statement {i}: {stmt_summary}...")

            db.execute_query(stmt)
            db.commit()
            success_count += 1

        except Exception as e:
            error_count += 1
            logger.error(f"Error executing statement {i}: {e}")
            logger.debug(f"Failed statement: {stmt}")

            # Continue or abort?
            if "CREATE TABLE" in stmt.upper() and "already exists" in str(e).lower():
                logger.warning("Table already exists, continuing...")
                continue
            else:
                raise

    logger.info(f"Execution complete: {success_count} successful, {error_count} errors")


def verify_schema(db: DatabaseConnection) -> bool:
    """
    Verify that all required tables exist.

    Args:
        db: Database connection

    Returns:
        bool: True if all tables exist
    """
    required_tables = [
        'source_systems',
        'table_mappings',
        'column_mappings',
        'lookup_mappings',
        'data_quality_rules',
        'etl_execution_log',
        'data_quality_log',
        'staging_tables',
        'table_dependencies'
    ]

    logger.info("Verifying database schema...")

    missing_tables = []
    for table in required_tables:
        if db.table_exists('config', table):
            logger.info(f"✓ Table config.{table} exists")
        else:
            logger.error(f"✗ Table config.{table} NOT FOUND")
            missing_tables.append(table)

    if missing_tables:
        logger.error(f"Missing tables: {', '.join(missing_tables)}")
        return False

    logger.info("✓ All required tables exist")
    return True


def display_schema_info(db: DatabaseConnection):
    """
    Display information about the created schema.

    Args:
        db: Database connection
    """
    # Get table list
    query = """
        SELECT TABLE_NAME, TABLE_ROWS,
               ROUND(DATA_LENGTH / 1024, 2) AS size_kb
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'config'
        ORDER BY TABLE_NAME
    """
    tables = db.fetch_all(query, dictionary=True)

    if not tables:
        logger.warning("No tables found in config schema")
        return

    logger.info("\n" + "=" * 80)
    logger.info("Configuration Database Schema Summary")
    logger.info("=" * 80)
    logger.info(f"Total tables: {len(tables)}")
    logger.info("-" * 80)

    for table in tables:
        logger.info(
            f"  {table['TABLE_NAME']:<30} "
            f"Rows: {table['TABLE_ROWS']:>6} "
            f"Size: {table['size_kb']:>8.2f} KB"
        )

    logger.info("=" * 80)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Setup TA-RDM Source Ingestion configuration database'
    )
    parser.add_argument(
        '--schema-file',
        type=str,
        default='metadata/schema/config_schema.sql',
        help='Path to SQL schema file (default: metadata/schema/config_schema.sql)'
    )
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing schema before creation'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify schema, do not create'
    )

    args = parser.parse_args()

    # Resolve schema file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    schema_file = os.path.join(project_root, args.schema_file)

    if not os.path.exists(schema_file):
        logger.error(f"Schema file not found: {schema_file}")
        sys.exit(1)

    logger.info("=" * 80)
    logger.info("TA-RDM Source Ingestion - Configuration Database Setup")
    logger.info("=" * 80)
    logger.info(f"Schema file: {schema_file}")
    logger.info(f"Database: {CONFIG_DB_CONFIG['host']}:{CONFIG_DB_CONFIG['port']}")
    logger.info(f"Schema: config")
    logger.info("=" * 80)
    logger.info("")

    # Warn if dropping existing
    if args.drop_existing:
        logger.warning("⚠  WARNING: --drop-existing specified")
        logger.warning("⚠  This will DELETE all existing configuration data!")
        response = input("⚠  Continue? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted by user")
            sys.exit(0)

    # Connect to database
    try:
        logger.info("Connecting to configuration database...")
        db = DatabaseConnection(DatabaseType.MYSQL, CONFIG_DB_CONFIG)
        db.connect()
        logger.info("✓ Connected successfully")

    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        logger.error("Please check your .env configuration")
        sys.exit(1)

    try:
        # Verify only mode
        if args.verify_only:
            success = verify_schema(db)
            if success:
                display_schema_info(db)
            sys.exit(0 if success else 1)

        # Execute schema file
        execute_sql_file(db, schema_file, args.drop_existing)

        # Verify schema
        if not verify_schema(db):
            logger.error("✗ Schema verification failed")
            sys.exit(1)

        # Display summary
        display_schema_info(db)

        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Configuration database setup complete!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Import mapping configurations:")
        logger.info("     python scripts/import_mappings.py --config metadata/mappings/ramis_example.yaml")
        logger.info("  2. Verify configuration:")
        logger.info("     python scripts/setup_config_db.py --verify-only")
        logger.info("")

    except Exception as e:
        logger.error(f"✗ Setup failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)

    finally:
        db.disconnect()


if __name__ == '__main__':
    main()
