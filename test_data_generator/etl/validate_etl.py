#!/usr/bin/env python3
"""
ETL Validation Utility for TA-RDM L2→L3 Pipeline

Validates data consistency and integrity between MySQL Layer 2 (L2)
and ClickHouse Layer 3 (L3).

This utility compares input (L2 tables) vs output (L3 tables) to ensure:
- Row counts match (no data loss)
- Foreign key relationships are valid
- Data quality rules are met
- Business logic is correct

Usage:
    # Validate entire pipeline
    python etl/validate_etl.py

    # Validate specific table
    python etl/validate_etl.py --table dim_party

    # Generate JSON report
    python etl/validate_etl.py --format json --output validation.json

    # Generate HTML report
    python etl/validate_etl.py --format html --output validation.html
"""

import argparse
import sys
import os
import json
from datetime import datetime
from typing import Dict, Any, List
import logging

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import get_db_connection
from config.clickhouse_config import CLICKHOUSE_CONFIG
import clickhouse_connect

from etl.validators.row_count_validator import RowCountValidator
from etl.validators.integrity_validator import IntegrityValidator
from etl.validators.quality_validator import QualityValidator
from etl.validators.business_validator import BusinessValidator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def run_validation(table_filter: str = None) -> Dict[str, Any]:
    """
    Run comprehensive ETL validation.

    Args:
        table_filter: Optional table name to validate (validates all if None)

    Returns:
        Dict containing validation results
    """
    results = {
        'validation_timestamp': datetime.now(),
        'table_filter': table_filter,
        'validators': {},
        'summary': {}
    }

    logger.info("=" * 80)
    logger.info("Starting ETL Validation")
    logger.info("=" * 80)

    try:
        with get_db_connection() as mysql_conn:
            clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

            # 1. Row Count Validation (PRIMARY - input vs output)
            print("\n1. ROW COUNT VALIDATION (L2 Input → L3 Output)")
            print("-" * 80)
            row_count_validator = RowCountValidator(mysql_conn, clickhouse_client)
            row_count_results = row_count_validator.validate(table_filter)
            results['validators']['row_count'] = [r.to_dict() for r in row_count_results]

            # 2. Referential Integrity Validation
            print("\n2. REFERENTIAL INTEGRITY VALIDATION")
            print("-" * 80)
            integrity_validator = IntegrityValidator(mysql_conn, clickhouse_client)
            integrity_results = integrity_validator.validate(table_filter)
            results['validators']['integrity'] = [r.to_dict() for r in integrity_results]

            # 3. Data Quality Validation
            print("\n3. DATA QUALITY VALIDATION")
            print("-" * 80)
            quality_validator = QualityValidator(mysql_conn, clickhouse_client)
            quality_results = quality_validator.validate(table_filter)
            results['validators']['quality'] = [r.to_dict() for r in quality_results]

            # 4. Business Rule Validation
            print("\n4. BUSINESS RULE VALIDATION")
            print("-" * 80)
            business_validator = BusinessValidator(mysql_conn, clickhouse_client)
            business_results = business_validator.validate(table_filter)
            results['validators']['business'] = [r.to_dict() for r in business_results]

            # Calculate summary
            results['summary'] = calculate_summary(results['validators'])

    except Exception as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        results['error'] = str(e)
        results['summary'] = {'total_checks': 0, 'passed': 0, 'failed': 1, 'warnings': 0}

    return results


def calculate_summary(validators: Dict[str, List[Dict]]) -> Dict[str, Any]:
    """Calculate overall validation summary."""

    summary = {
        'total_checks': 0,
        'passed': 0,
        'failed': 0,
        'warnings': 0
    }

    for validator_name, validator_results in validators.items():
        for result in validator_results:
            summary['total_checks'] += 1

            if result['status'] == 'PASS':
                summary['passed'] += 1
            elif result['status'] == 'FAIL':
                summary['failed'] += 1
            elif result['status'] == 'WARNING':
                summary['warnings'] += 1

    summary['pass_rate'] = (
        summary['passed'] / summary['total_checks'] * 100
        if summary['total_checks'] > 0 else 0
    )

    return summary


def print_console_report(results: Dict[str, Any]):
    """Print validation results to console."""

    print("\n" + "=" * 100)
    print("ETL VALIDATION REPORT")
    print("=" * 100)
    print(f"Validation Time: {results['validation_timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")

    if results.get('table_filter'):
        print(f"Table Filter: {results['table_filter']}")

    print("=" * 100)

    # Overall Summary
    summary = results['summary']
    print("\nOVERALL SUMMARY")
    print("-" * 100)
    print(f"Total Checks:     {summary['total_checks']}")
    print(f"Passed:           {summary['passed']} ({summary['pass_rate']:.1f}%)")
    print(f"Failed:           {summary['failed']}")
    print(f"Warnings:         {summary['warnings']}")

    # Detailed results by validator
    for validator_name, validator_results in results['validators'].items():
        if not validator_results:
            continue

        print(f"\n{validator_name.upper().replace('_', ' ')}")
        print("-" * 100)

        for result in validator_results:
            status_icon = {
                'PASS': '✓',
                'FAIL': '✗',
                'WARNING': '⚠'
            }.get(result['status'], '?')

            print(f"{status_icon} {result['check_name']}")

            # Print details for row counts (always show), failures, and warnings
            if validator_name == 'row_count' or result['status'] in ['FAIL', 'WARNING']:
                details = result['details']

                # Special formatting for row count results
                if validator_name == 'row_count':
                    print(f"    L2 Source:    {details.get('l2_source_count', 'N/A')} rows")
                    print(f"    L3 Target:    {details.get('l3_target_count', 'N/A')} rows")
                    print(f"    Expected:     {details.get('expected_count', 'N/A')} rows")
                    print(f"    Difference:   {details.get('difference', 0):+d}")
                    print(f"    Ratio:        {details.get('ratio', 0):.2%}")
                    if details.get('description'):
                        print(f"    Description:  {details['description']}")
                else:
                    # Other validators
                    for key, value in details.items():
                        if key != 'description':
                            print(f"    {key}: {value}")

    print("\n" + "=" * 100)

    # Final verdict
    if summary['failed'] == 0 and summary['warnings'] == 0:
        print("✓ VALIDATION PASSED - All checks successful")
    elif summary['failed'] == 0:
        print(f"⚠ VALIDATION PASSED WITH WARNINGS - {summary['warnings']} warning(s)")
    else:
        print(f"✗ VALIDATION FAILED - {summary['failed']} check(s) failed")

    print("=" * 100 + "\n")


def generate_json_report(results: Dict[str, Any]) -> str:
    """Generate JSON report."""
    return json.dumps(results, indent=2, default=str)


def generate_html_report(results: Dict[str, Any]) -> str:
    """Generate simple HTML report."""

    summary = results['summary']
    status_color = 'green' if summary['failed'] == 0 else 'red'

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>ETL Validation Report - {results['validation_timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        .summary {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        .pass {{ color: green; }}
        .fail {{ color: red; }}
        .warning {{ color: orange; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .validator-section {{ margin: 30px 0; }}
    </style>
</head>
<body>
    <h1>ETL Validation Report</h1>
    <p>Generated: {results['validation_timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="summary">
        <h2>Summary</h2>
        <p>Total Checks: {summary['total_checks']}</p>
        <p class="pass">Passed: {summary['passed']} ({summary['pass_rate']:.1f}%)</p>
        <p class="fail">Failed: {summary['failed']}</p>
        <p class="warning">Warnings: {summary['warnings']}</p>
        <h3 class="{status_color}">{'PASS' if summary['failed'] == 0 else 'FAIL'}</h3>
    </div>
"""

    for validator_name, validator_results in results['validators'].items():
        html += f'<div class="validator-section"><h2>{validator_name.replace("_", " ").title()}</h2><table>'
        html += '<tr><th>Check</th><th>Status</th><th>Details</th></tr>'

        for result in validator_results:
            status_class = result['status'].lower()
            details_str = '<br>'.join([f"{k}: {v}" for k, v in result['details'].items()])
            html += f"""
                <tr>
                    <td>{result['check_name']}</td>
                    <td class="{status_class}">{result['status']}</td>
                    <td><small>{details_str}</small></td>
                </tr>
            """

        html += '</table></div>'

    html += '</body></html>'
    return html


def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description='ETL Validation Utility for TA-RDM L2→L3 Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl/validate_etl.py
  python etl/validate_etl.py --table dim_party
  python etl/validate_etl.py --format json --output validation.json
  python etl/validate_etl.py --format html --output validation.html
        """
    )

    parser.add_argument(
        '--table',
        help='Validate specific table only (e.g., dim_party, fact_registration)'
    )

    parser.add_argument(
        '--format',
        choices=['console', 'html', 'json'],
        default='console',
        help='Report output format (default: console)'
    )

    parser.add_argument(
        '--output',
        help='Output file path for report (stdout if not specified)'
    )

    args = parser.parse_args()

    try:
        # Run validation
        results = run_validation(table_filter=args.table)

        # Generate report
        if args.format == 'console':
            print_console_report(results)

        elif args.format == 'json':
            json_output = generate_json_report(results)

            if args.output:
                with open(args.output, 'w') as f:
                    f.write(json_output)
                print(f"JSON report saved to: {args.output}")
            else:
                print(json_output)

        elif args.format == 'html':
            html_output = generate_html_report(results)

            if args.output:
                with open(args.output, 'w') as f:
                    f.write(html_output)
                print(f"HTML report saved to: {args.output}")
            else:
                print(html_output)

        # Exit code based on validation results
        return 0 if results['summary']['failed'] == 0 else 1

    except Exception as e:
        logger.error(f"Validation failed with error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
