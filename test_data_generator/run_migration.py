#!/usr/bin/env python3
"""
Run database migration scripts.

Usage:
    python run_migration.py migrations/001_add_party_attributes.sql
"""

import sys
import logging
from pathlib import Path

from utils.db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def run_migration(migration_file: str) -> bool:
    """
    Run a SQL migration file.

    Args:
        migration_file: Path to SQL file

    Returns:
        bool: True if successful
    """
    migration_path = Path(migration_file)

    if not migration_path.exists():
        logger.error(f"Migration file not found: {migration_file}")
        return False

    logger.info(f"Running migration: {migration_path.name}")

    # Read migration SQL
    with open(migration_path, 'r') as f:
        sql_content = f.read()

    # Split by semicolons to get individual statements
    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

    # Filter out comments-only statements
    statements = [stmt for stmt in statements if not stmt.startswith('--')]

    logger.info(f"Found {len(statements)} SQL statements to execute")

    try:
        with get_db_connection() as db:
            for idx, statement in enumerate(statements, 1):
                # Skip USE statements and comments
                if statement.upper().startswith('USE') or statement.startswith('--'):
                    logger.debug(f"Skipping statement {idx}: {statement[:50]}...")
                    continue

                logger.info(f"Executing statement {idx}/{len(statements)}...")
                logger.debug(f"SQL: {statement[:100]}...")

                try:
                    db.execute_query(statement)
                except Exception as e:
                    # Check if it's a "duplicate column" error (column already exists)
                    if 'Duplicate column name' in str(e):
                        logger.warning(f"Column already exists (skipping): {e}")
                        continue
                    else:
                        raise

            db.commit()
            logger.info("Migration completed successfully")
            return True

    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_migration.py <migration_file.sql>")
        print("Example: python run_migration.py migrations/001_add_party_attributes.sql")
        return 1

    migration_file = sys.argv[1]
    success = run_migration(migration_file)

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
