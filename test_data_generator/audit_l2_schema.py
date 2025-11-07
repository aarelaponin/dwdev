#!/usr/bin/env python3
"""
L2 Schema Audit Tool - Phase 1.1

Comprehensively audits the L2 MySQL schema for DDL issues:
- FK constraint problems
- Missing referenced tables
- Column name inconsistencies
- Data type issues
- Constraint validation

This provides the foundation for creating a DDL fix script.
"""

import logging
from utils.db_utils import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def audit_fk_constraints(db, schema: str):
    """Audit foreign key constraints for schema path and reference issues."""

    logger.info(f"\n{'='*100}")
    logger.info(f"AUDITING FK CONSTRAINTS IN SCHEMA: {schema}")
    logger.info(f"{'='*100}\n")

    # Get all FK constraints in the schema
    fks = db.fetch_all("""
        SELECT
            kcu.TABLE_NAME,
            kcu.COLUMN_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.REFERENCED_TABLE_SCHEMA,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE kcu
        WHERE kcu.TABLE_SCHEMA = %s
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME
    """, (schema,))

    issues = []

    for table, column, constraint, ref_schema, ref_table, ref_column in fks:
        issue_found = False
        issue_details = {
            'table': table,
            'constraint': constraint,
            'column': column,
            'ref_schema': ref_schema,
            'ref_table': ref_table,
            'ref_column': ref_column,
            'problems': []
        }

        # Check 1: Schema path issue (e.g., filing_assessment.registration.tax_account)
        if ref_schema and '.' in ref_schema:
            issue_details['problems'].append(f"INVALID: Referenced schema contains dot: '{ref_schema}'")
            issue_found = True

        # Check 2: Referenced table exists
        try:
            table_check = db.fetch_one("""
                SELECT COUNT(*)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (ref_schema or schema, ref_table))

            if table_check[0] == 0:
                issue_details['problems'].append(f"MISSING: Referenced table '{ref_schema}.{ref_table}' does not exist")
                issue_found = True
        except:
            issue_details['problems'].append(f"ERROR: Could not verify table '{ref_schema}.{ref_table}'")
            issue_found = True

        # Check 3: Referenced column exists (if table exists)
        if not issue_found or 'MISSING' not in str(issue_details['problems']):
            try:
                column_check = db.fetch_one("""
                    SELECT COUNT(*)
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                """, (ref_schema or schema, ref_table, ref_column))

                if column_check[0] == 0:
                    issue_details['problems'].append(f"MISSING: Referenced column '{ref_column}' in '{ref_table}'")
                    issue_found = True
            except:
                pass

        if issue_found:
            issues.append(issue_details)

    # Report findings
    logger.info(f"Total FK Constraints Checked: {len(fks)}")
    logger.info(f"FK Constraints with Issues: {len(issues)}\n")

    if issues:
        logger.info("ISSUES FOUND:\n")
        for idx, issue in enumerate(issues, 1):
            logger.info(f"{idx}. Table: {issue['table']}")
            logger.info(f"   Constraint: {issue['constraint']}")
            logger.info(f"   Column: {issue['column']} -> {issue['ref_schema']}.{issue['ref_table']}.{issue['ref_column']}")
            for problem in issue['problems']:
                logger.info(f"   ❌ {problem}")
            logger.info("")

    return issues


def audit_table_structure(db, schema: str, table: str):
    """Audit a specific table's structure."""

    logger.info(f"\n{'='*100}")
    logger.info(f"AUDITING TABLE STRUCTURE: {schema}.{table}")
    logger.info(f"{'='*100}\n")

    # Get column definitions
    columns = db.fetch_all(f"DESCRIBE {schema}.{table}")

    issues = []

    for col in columns:
        field_name, field_type, null, key, default, extra = col

        # Check for potential issues
        issue_details = {
            'column': field_name,
            'type': field_type,
            'problems': []
        }

        # Check 1: Mandatory fields without default
        if null == 'NO' and default is None and 'auto_increment' not in extra.lower():
            if field_type.startswith('tinyint(1)'):
                issue_details['problems'].append(f"BOOLEAN field '{field_name}' is NOT NULL but has no DEFAULT")
            elif not field_type.startswith(('varchar', 'text', 'timestamp')):
                issue_details['problems'].append(f"NOT NULL field '{field_name}' has no DEFAULT value")

        # Check 2: Decimal precision consistency
        if field_type.startswith('decimal'):
            # Note precision for reporting
            issue_details['info'] = f"DECIMAL field: {field_type}"

        if issue_details['problems']:
            issues.append(issue_details)

    if issues:
        logger.info("POTENTIAL ISSUES:\n")
        for idx, issue in enumerate(issues, 1):
            logger.info(f"{idx}. Column: {issue['column']} ({issue['type']})")
            for problem in issue['problems']:
                logger.info(f"   ⚠️  {problem}")
            if 'info' in issue:
                logger.info(f"   ℹ️  {issue['info']}")
            logger.info("")
    else:
        logger.info("✅ No structural issues found\n")

    return issues


def audit_missing_tables(db):
    """Check for commonly missing tables."""

    logger.info(f"\n{'='*100}")
    logger.info("CHECKING FOR MISSING TABLES")
    logger.info(f"{'='*100}\n")

    # Tables that should exist based on FK references
    expected_tables = [
        ('tax_framework', 'form_version'),
        ('tax_framework', 'form_line'),
        ('tax_framework', 'tax_form'),
    ]

    missing = []

    for schema, table in expected_tables:
        result = db.fetch_one("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (schema, table))

        if result[0] == 0:
            missing.append((schema, table))
            logger.info(f"❌ MISSING: {schema}.{table}")
        else:
            logger.info(f"✅ EXISTS: {schema}.{table}")

    return missing


def main():
    """Main audit execution."""

    logger.info("="*100)
    logger.info("L2 SCHEMA AUDIT - PHASE 1.1")
    logger.info("="*100)

    all_issues = {
        'fk_issues': [],
        'structure_issues': {},
        'missing_tables': []
    }

    with get_db_connection() as db:
        # 1. Audit FK constraints in filing_assessment schema
        fk_issues = audit_fk_constraints(db, 'filing_assessment')
        all_issues['fk_issues'] = fk_issues

        # 2. Check for missing tables
        missing = audit_missing_tables(db)
        all_issues['missing_tables'] = missing

        # 3. Audit structure of key tables
        key_tables = [
            ('filing_assessment', 'tax_return'),
            ('filing_assessment', 'tax_return_line'),
            ('filing_assessment', 'assessment'),
            ('filing_assessment', 'assessment_line'),
            ('tax_framework', 'tax_form'),
        ]

        for schema, table in key_tables:
            # Check if table exists first
            exists = db.fetch_one("""
                SELECT COUNT(*)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (schema, table))

            if exists[0] > 0:
                issues = audit_table_structure(db, schema, table)
                if issues:
                    all_issues['structure_issues'][f"{schema}.{table}"] = issues

    # Summary Report
    logger.info("\n" + "="*100)
    logger.info("AUDIT SUMMARY")
    logger.info("="*100)
    logger.info(f"\n📊 Total FK Constraint Issues: {len(all_issues['fk_issues'])}")
    logger.info(f"📊 Total Missing Tables: {len(all_issues['missing_tables'])}")
    logger.info(f"📊 Tables with Structural Issues: {len(all_issues['structure_issues'])}")

    # Priority recommendations
    logger.info("\n" + "="*100)
    logger.info("PRIORITY RECOMMENDATIONS")
    logger.info("="*100)

    if all_issues['fk_issues']:
        logger.info("\n🔴 CRITICAL: Fix FK constraint schema paths")
        logger.info("   - All FK constraints in filing_assessment have incorrect schema prefixes")
        logger.info("   - This prevents data insertion even with correct data")
        logger.info("   - Recommendation: DROP and RECREATE FKs with correct paths")

    if all_issues['missing_tables']:
        logger.info("\n🟡 HIGH: Create missing tables")
        for schema, table in all_issues['missing_tables']:
            logger.info(f"   - {schema}.{table}")
        logger.info("   - Recommendation: Create minimal table structures or remove FK references")

    if all_issues['structure_issues']:
        logger.info("\n🟢 MEDIUM: Review table structures")
        logger.info("   - Some columns lack defaults but are required")
        logger.info("   - Recommendation: Add DEFAULT values or make columns nullable")

    logger.info("\n" + "="*100)
    logger.info("NEXT STEP: Phase 1.2 - Create DDL Fix Script")
    logger.info("="*100)

    return 0 if not all_issues['fk_issues'] else 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
