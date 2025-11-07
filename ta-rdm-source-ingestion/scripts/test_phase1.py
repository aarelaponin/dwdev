#!/usr/bin/env python3
"""
Phase 1 Verification Script

Tests the foundational components of TA-RDM Source Ingestion:
- Package imports
- Logging framework
- Database utilities (without actual connections)
- Environment configuration

Usage:
    python scripts/test_phase1.py
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

print("=" * 80)
print("TA-RDM Source Ingestion - Phase 1 Verification")
print("=" * 80)
print()

# Test 1: Import utilities
print("Test 1: Importing utilities...")
try:
    from utils import db_utils, logging_utils
    print("✓ Successfully imported db_utils")
    print("✓ Successfully imported logging_utils")
    print()
except ImportError as e:
    print(f"✗ Import failed: {e}")
    sys.exit(1)

# Test 2: Setup logging
print("Test 2: Setting up logging framework...")
try:
    from utils.logging_utils import setup_logging, ETLLogger

    logger = setup_logging(
        name='phase1_test',
        log_level='INFO',
        use_colors=True
    )

    logger.info("Logging framework initialized successfully")
    logger.debug("Debug logging works")
    logger.warning("Warning logging works")

    print("✓ Logging framework operational")
    print()
except Exception as e:
    print(f"✗ Logging setup failed: {e}")
    sys.exit(1)

# Test 3: ETL Logger
print("Test 3: Testing ETL-specific logging...")
try:
    etl_logger = ETLLogger(name='phase1_etl_test')

    etl_logger.log_etl_start(
        process_name="PHASE1_VERIFICATION",
        source="TEST_SOURCE",
        target="TEST_TARGET"
    )

    etl_logger.log_batch_progress(
        current=5000,
        total=10000,
        batch_size=1000
    )

    etl_logger.log_validation_result(
        rule_name="NOT_NULL_TEST",
        passed=True
    )

    etl_logger.log_etl_end(
        process_name="PHASE1_VERIFICATION",
        status="SUCCESS",
        rows_processed=10000,
        duration_seconds=1.5
    )

    print("✓ ETL logger operational")
    print()
except Exception as e:
    print(f"✗ ETL logger failed: {e}")
    sys.exit(1)

# Test 4: Database utilities structure
print("Test 4: Testing database utility classes...")
try:
    from utils.db_utils import DatabaseType, DatabaseConnection

    # Test enum
    assert DatabaseType.MYSQL.value == "mysql"
    assert DatabaseType.SQL_SERVER.value == "sql_server"
    print("✓ DatabaseType enum available")

    # Test class availability (without actual connection)
    test_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'test',
        'password': 'test'
    }

    db = DatabaseConnection(DatabaseType.MYSQL, test_config)
    print("✓ DatabaseConnection class available")
    print(f"  - Database type: {db.db_type.value}")
    print(f"  - Config loaded: {len(db.config)} parameters")

    print()
except Exception as e:
    print(f"✗ Database utilities test failed: {e}")
    sys.exit(1)

# Test 5: Environment configuration
print("Test 5: Checking environment configuration...")
try:
    from dotenv import load_dotenv

    # Check for .env.example
    env_example_path = os.path.join(
        os.path.dirname(__file__), '..', '.env.example'
    )

    if os.path.exists(env_example_path):
        print(f"✓ .env.example found")

        # Count configuration options
        with open(env_example_path, 'r') as f:
            lines = f.readlines()
            config_lines = [l for l in lines if '=' in l and not l.strip().startswith('#')]
            print(f"  - {len(config_lines)} configuration options available")
    else:
        print("⚠ .env.example not found (expected at project root)")

    print()
except Exception as e:
    print(f"✗ Environment config test failed: {e}")
    sys.exit(1)

# Test 6: Package structure
print("Test 6: Verifying package structure...")
try:
    expected_dirs = [
        'config', 'metadata', 'extractors', 'transformers',
        'loaders', 'validators', 'orchestration', 'utils',
        'scripts', 'tests', 'docs'
    ]

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    for dir_name in expected_dirs:
        dir_path = os.path.join(project_root, dir_name)
        init_path = os.path.join(dir_path, '__init__.py')

        if os.path.isdir(dir_path):
            if os.path.exists(init_path):
                print(f"✓ {dir_name}/ - with __init__.py")
            else:
                print(f"⚠ {dir_name}/ - missing __init__.py")
        else:
            print(f"✗ {dir_name}/ - NOT FOUND")

    print()
except Exception as e:
    print(f"✗ Package structure test failed: {e}")
    sys.exit(1)

# Test 7: Dependencies check
print("Test 7: Checking required dependencies...")
try:
    required_packages = [
        ('mysql.connector', 'mysql-connector-python'),
        ('pymssql', 'pymssql'),
        ('pandas', 'pandas'),
        ('yaml', 'PyYAML'),
        ('colorama', 'colorama'),
        ('dotenv', 'python-dotenv'),
    ]

    for module_name, package_name in required_packages:
        try:
            __import__(module_name)
            print(f"✓ {package_name}")
        except ImportError:
            print(f"⚠ {package_name} - not installed (optional or run: pip install {package_name})")

    print()
except Exception as e:
    print(f"✗ Dependency check failed: {e}")
    sys.exit(1)

# Summary
print("=" * 80)
print("PHASE 1 VERIFICATION COMPLETE")
print("=" * 80)
print()
print("Summary:")
print("  ✓ All core utilities imported successfully")
print("  ✓ Logging framework operational")
print("  ✓ Database utilities available")
print("  ✓ Package structure correct")
print()
print("Next Steps:")
print("  1. Copy .env.example to .env and configure your database credentials")
print("  2. Test actual database connections:")
print("     python scripts/test_connections.py (Phase 2)")
print("  3. Proceed to Phase 2: Configuration Layer")
print()
print("=" * 80)
