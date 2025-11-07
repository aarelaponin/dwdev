#!/usr/bin/env python3
"""
TA-RDM ETL Package Verification

Verifies that the customer package has all required files and structure.
Run this script to check package integrity before deployment.

Usage:
    python scripts/verify_package.py
"""

import os
import sys
from pathlib import Path

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


def check_file_exists(path, description):
    """Check if a file exists."""
    if os.path.isfile(path):
        print(f"  {GREEN}✓{RESET} {description}")
        return True
    else:
        print(f"  {RED}✗{RESET} {description} - MISSING: {path}")
        return False


def check_dir_exists(path, description):
    """Check if a directory exists."""
    if os.path.isdir(path):
        print(f"  {GREEN}✓{RESET} {description}")
        return True
    else:
        print(f"  {RED}✗{RESET} {description} - MISSING: {path}")
        return False


def check_no_test_data_refs():
    """Check that no test data generation references exist."""
    print(f"\n{BLUE}Checking for Test Data References...{RESET}")
    print("-" * 80)

    issues = []

    # Files that should NOT exist
    forbidden_files = [
        'generators',
        'main.py',
        'cleanup.py',
        'config/tax_config.py',
        'config/party_profiles.py',
        'utils/faker_sri_lanka.py',
    ]

    for file_path in forbidden_files:
        full_path = os.path.join(base_dir, file_path)
        if os.path.exists(full_path):
            print(f"  {RED}✗{RESET} Found test data file: {file_path}")
            issues.append(file_path)
        else:
            print(f"  {GREEN}✓{RESET} No {file_path}")

    # Check requirements.txt doesn't have Faker
    req_file = os.path.join(base_dir, 'requirements.txt')
    if os.path.isfile(req_file):
        with open(req_file, 'r') as f:
            content = f.read()
            if 'Faker' in content or 'faker' in content:
                print(f"  {RED}✗{RESET} requirements.txt contains Faker")
                issues.append('Faker in requirements.txt')
            else:
                print(f"  {GREEN}✓{RESET} requirements.txt does not contain Faker")

    return len(issues) == 0


def main():
    """Main verification function."""
    global base_dir
    base_dir = Path(__file__).parent.parent

    print("=" * 80)
    print("TA-RDM ETL PACKAGE VERIFICATION")
    print("=" * 80)
    print(f"Package Directory: {base_dir}")
    print()

    all_checks = []

    # Check root files
    print(f"{BLUE}Checking Root Files...{RESET}")
    print("-" * 80)
    all_checks.append(check_file_exists(os.path.join(base_dir, 'README.md'), 'README.md'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'requirements.txt'), 'requirements.txt'))
    all_checks.append(check_file_exists(os.path.join(base_dir, '.env.example'), '.env.example'))

    # Check directory structure
    print(f"\n{BLUE}Checking Directory Structure...{RESET}")
    print("-" * 80)
    all_checks.append(check_dir_exists(os.path.join(base_dir, 'etl'), 'etl/'))
    all_checks.append(check_dir_exists(os.path.join(base_dir, 'utils'), 'utils/'))
    all_checks.append(check_dir_exists(os.path.join(base_dir, 'config'), 'config/'))
    all_checks.append(check_dir_exists(os.path.join(base_dir, 'scripts'), 'scripts/'))
    all_checks.append(check_dir_exists(os.path.join(base_dir, 'docs'), 'docs/'))
    all_checks.append(check_dir_exists(os.path.join(base_dir, 'etl/validators'), 'etl/validators/'))

    # Check documentation
    print(f"\n{BLUE}Checking Documentation...{RESET}")
    print("-" * 80)
    docs = [
        'INSTALLATION.md',
        'ETL-EXECUTION.md',
        'CONFIGURATION.md',
        'TROUBLESHOOTING.md',
        'SCHEDULING.md',
        'ETL-Validation-Guide.md',
        'L2-L3-Mapping.md',
        'ta-rdm-mysql-ddl.sql',
        'ta_dw_clickhouse_schema.sql',
    ]
    for doc in docs:
        all_checks.append(check_file_exists(os.path.join(base_dir, 'docs', doc), f'docs/{doc}'))

    # Check ETL scripts
    print(f"\n{BLUE}Checking ETL Scripts...{RESET}")
    print("-" * 80)
    etl_scripts = [
        'generate_dim_time.py',
        'l2_to_l3_party.py',
        'l2_to_l3_tax_type.py',
        'l2_to_l3_tax_period.py',
        'l2_to_l3_fact_registration.py',
        'l2_to_l3_fact_filing.py',
        'l2_to_l3_fact_assessment.py',
        'l2_to_l3_fact_payment.py',
        'l2_to_l3_fact_account_balance.py',
        'l2_to_l3_fact_collection.py',
        'l2_to_l3_fact_refund.py',
        'l2_to_l3_fact_audit.py',
        'l2_to_l3_fact_objection.py',
        'l2_to_l3_fact_risk_assessment.py',
        'l2_to_l3_fact_taxpayer_activity.py',
        'validate_etl.py',
    ]
    for script in etl_scripts:
        all_checks.append(check_file_exists(os.path.join(base_dir, 'etl', script), f'etl/{script}'))

    # Check helper scripts
    print(f"\n{BLUE}Checking Helper Scripts...{RESET}")
    print("-" * 80)
    all_checks.append(check_file_exists(os.path.join(base_dir, 'scripts/run_full_etl.sh'), 'run_full_etl.sh'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'scripts/run_validation.sh'), 'run_validation.sh'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'scripts/test_connections.py'), 'test_connections.py'))

    # Check scripts are executable
    print(f"\n{BLUE}Checking Script Permissions...{RESET}")
    print("-" * 80)
    scripts_to_check = [
        'scripts/run_full_etl.sh',
        'scripts/run_validation.sh',
        'scripts/test_connections.py',
    ]
    for script in scripts_to_check:
        script_path = os.path.join(base_dir, script)
        if os.path.isfile(script_path):
            if os.access(script_path, os.X_OK):
                print(f"  {GREEN}✓{RESET} {script} is executable")
                all_checks.append(True)
            else:
                print(f"  {YELLOW}⚠{RESET} {script} is not executable (run: chmod +x {script})")
                all_checks.append(False)

    # Check utils and config
    print(f"\n{BLUE}Checking Utils and Config...{RESET}")
    print("-" * 80)
    all_checks.append(check_file_exists(os.path.join(base_dir, 'utils/db_utils.py'), 'db_utils.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'utils/clickhouse_utils.py'), 'clickhouse_utils.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'config/database_config.py'), 'database_config.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'config/clickhouse_config.py'), 'clickhouse_config.py'))

    # Check validators
    print(f"\n{BLUE}Checking Validators...{RESET}")
    print("-" * 80)
    all_checks.append(check_file_exists(os.path.join(base_dir, 'etl/validators/mapping_config.py'), 'mapping_config.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'etl/validators/base_validator.py'), 'base_validator.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'etl/validators/row_count_validator.py'), 'row_count_validator.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'etl/validators/integrity_validator.py'), 'integrity_validator.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'etl/validators/quality_validator.py'), 'quality_validator.py'))
    all_checks.append(check_file_exists(os.path.join(base_dir, 'etl/validators/business_validator.py'), 'business_validator.py'))

    # Check for test data references
    all_checks.append(check_no_test_data_refs())

    # Summary
    print()
    print("=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)

    total_checks = len(all_checks)
    passed_checks = sum(all_checks)
    failed_checks = total_checks - passed_checks

    if failed_checks == 0:
        print(f"{GREEN}✓ ALL CHECKS PASSED ({passed_checks}/{total_checks}){RESET}")
        print()
        print("Package is ready for deployment!")
        print()
        print("Next steps:")
        print("  1. Create virtual environment: python3 -m venv venv")
        print("  2. Activate venv: source venv/bin/activate")
        print("  3. Install dependencies: pip install -r requirements.txt")
        print("  4. Configure .env: cp .env.example .env && nano .env")
        print("  5. Test connections: python scripts/test_connections.py")
        print("  6. Run ETL: ./scripts/run_full_etl.sh")
        print()
        return 0
    else:
        print(f"{RED}✗ VERIFICATION FAILED{RESET}")
        print(f"Passed: {passed_checks}/{total_checks}")
        print(f"Failed: {failed_checks}/{total_checks}")
        print()
        print("Please fix the issues above before deployment.")
        print()
        return 1


if __name__ == '__main__':
    sys.exit(main())
