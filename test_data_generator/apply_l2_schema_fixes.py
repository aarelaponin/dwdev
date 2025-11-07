#!/usr/bin/env python3
"""
Apply L2 Schema Fixes - Phase 1.3

Safely applies the DDL fix script to correct L2 schema issues.
Includes rollback capability and comprehensive logging.
"""

import logging
import sys
from pathlib import Path

from utils.db_utils import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def execute_sql_file(db, sql_file_path: str):
    """Execute SQL commands from a file with proper error handling."""

    logger.info(f"Reading SQL script: {sql_file_path}")

    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
    except FileNotFoundError:
        logger.error(f"SQL file not found: {sql_file_path}")
        return False
    except Exception as e:
        logger.error(f"Error reading SQL file: {e}")
        return False

    # Split into individual statements (simple split on semicolon)
    # Note: This is a simple approach; a real parser would be more robust
    parts = sql_content.split(';')
    statements = []

    for part in parts:
        # Remove leading/trailing whitespace
        part = part.strip()

        # Skip empty parts
        if not part:
            continue

        # Remove comment-only lines but keep statements with comments
        lines = part.split('\n')
        non_comment_lines = [line for line in lines if line.strip() and not line.strip().startswith('--')]

        # If there are non-comment lines, this is a real statement
        if non_comment_lines:
            statements.append(part)

    total = len(statements)
    executed = 0
    failed = 0

    logger.info(f"Found {total} SQL statements to execute")
    logger.info("="*80)

    for idx, statement in enumerate(statements, 1):
        # Remove leading comment lines from the statement
        lines = statement.split('\n')
        non_comment_lines = []
        for line in lines:
            # Skip lines that are only comments
            if line.strip().startswith('--'):
                continue
            non_comment_lines.append(line)

        # Reconstruct the statement without leading comments
        clean_statement = '\n'.join(non_comment_lines).strip()

        # Skip if nothing left after removing comments
        if not clean_statement:
            continue

        # Extract statement type for logging
        stmt_type = clean_statement.split()[0].upper() if clean_statement.split() else 'UNKNOWN'

        try:
            # Log what we're about to execute (truncated for readability)
            stmt_preview = clean_statement[:100] + '...' if len(clean_statement) > 100 else clean_statement
            logger.info(f"[{idx}/{total}] Executing {stmt_type}: {stmt_preview}")

            db.execute_query(clean_statement)
            executed += 1

        except Exception as e:
            error_msg = str(e)

            # Determine if error is critical or acceptable
            if 'already exists' in error_msg.lower():
                logger.warning(f"    ⚠️  Table/constraint already exists (skipping): {error_msg}")
            elif "doesn't exist" in error_msg.lower() or 'unknown' in error_msg.lower():
                logger.warning(f"    ⚠️  Object doesn't exist (skipping): {error_msg}")
            elif "can't drop" in error_msg.lower():
                logger.warning(f"    ⚠️  Cannot drop (doesn't exist, skipping): {error_msg}")
            elif "duplicate entry" in error_msg.lower():
                logger.warning(f"    ⚠️  Duplicate entry (already exists, skipping): {error_msg}")
            else:
                logger.error(f"    ❌ Error executing statement: {error_msg}")
                failed += 1

                # Decide whether to continue or stop
                if failed > 5:
                    logger.error("Too many errors encountered. Stopping execution.")
                    return False

    logger.info("="*80)
    logger.info(f"Execution Summary:")
    logger.info(f"  Total statements: {total}")
    logger.info(f"  Successfully executed: {executed}")
    logger.info(f"  Failed: {failed}")

    return failed == 0


def verify_fixes(db):
    """Verify that the fixes were applied correctly."""

    logger.info("\n" + "="*80)
    logger.info("VERIFYING SCHEMA FIXES")
    logger.info("="*80 + "\n")

    verification_passed = True

    # Check 1: Verify FK constraints are now correct (no invalid schema paths)
    logger.info("Checking FK constraints...")
    fks = db.fetch_all("""
        SELECT
            CONSTRAINT_NAME,
            TABLE_NAME,
            REFERENCED_TABLE_SCHEMA,
            REFERENCED_TABLE_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = 'filing_assessment'
          AND REFERENCED_TABLE_NAME IS NOT NULL
    """)

    invalid_fks = [fk for fk in fks if fk[2] and '.' in fk[2]]

    if invalid_fks:
        logger.error(f"❌ Found {len(invalid_fks)} FK constraints with invalid schema paths")
        verification_passed = False
    else:
        logger.info(f"✅ All {len(fks)} FK constraints have valid schema paths")

    # Check 2: Verify missing tables now exist
    logger.info("\nChecking for previously missing tables...")

    tables_to_check = [
        ('tax_framework', 'form_version'),
        ('tax_framework', 'form_line'),
    ]

    for schema, table in tables_to_check:
        result = db.fetch_one("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (schema, table))

        if result[0] > 0:
            logger.info(f"✅ Table {schema}.{table} exists")
        else:
            logger.error(f"❌ Table {schema}.{table} still missing")
            verification_passed = False

    # Check 3: Verify form_version has data
    logger.info("\nChecking form_version data...")
    version_count = db.fetch_one("SELECT COUNT(*) FROM tax_framework.form_version")
    if version_count[0] > 0:
        logger.info(f"✅ form_version table has {version_count[0]} records")
    else:
        logger.warning(f"⚠️  form_version table is empty")

    # Check 4: Verify form_line has data
    logger.info("Checking form_line data...")
    line_count = db.fetch_one("SELECT COUNT(*) FROM tax_framework.form_line")
    if line_count[0] > 0:
        logger.info(f"✅ form_line table has {line_count[0]} records")
    else:
        logger.warning(f"⚠️  form_line table is empty")

    logger.info("\n" + "="*80)
    if verification_passed:
        logger.info("✅ VERIFICATION PASSED - All fixes applied successfully")
    else:
        logger.error("❌ VERIFICATION FAILED - Some issues remain")
    logger.info("="*80 + "\n")

    return verification_passed


def main():
    """Main execution."""

    logger.info("="*80)
    logger.info("L2 SCHEMA FIX APPLICATION - PHASE 1.3")
    logger.info("="*80 + "\n")

    sql_file = "fix_l2_schema_v2.sql"

    if not Path(sql_file).exists():
        logger.error(f"SQL fix script not found: {sql_file}")
        logger.error("Please ensure fix_l2_schema.sql is in the current directory")
        return 1

    try:
        with get_db_connection() as db:
            # Apply fixes
            logger.info("Applying DDL fixes...")
            success = execute_sql_file(db, sql_file)

            if not success:
                logger.error("Failed to apply all fixes. Rolling back transaction.")
                db.rollback()
                return 1

            # Commit changes
            logger.info("\nCommitting changes...")
            db.commit()
            logger.info("✅ Changes committed successfully")

            # Verify fixes
            if verify_fixes(db):
                logger.info("\n" + "="*80)
                logger.info("✅ L2 SCHEMA FIXES COMPLETED SUCCESSFULLY")
                logger.info("="*80)
                logger.info("\nNext Step: Phase 2.1 - Generate Phase C Test Data")
                return 0
            else:
                logger.error("\n" + "="*80)
                logger.error("⚠️  FIXES APPLIED BUT VERIFICATION FAILED")
                logger.error("="*80)
                logger.error("Manual review required")
                return 1

    except Exception as e:
        logger.error(f"Fatal error during fix application: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
