"""
Pytest Fixtures for TA-RDM Source Ingestion Tests.

Provides shared test fixtures, mock objects, and test data.
"""

import pytest
import os
import sys
from typing import Dict, List, Any
from unittest.mock import Mock, MagicMock
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import DatabaseConnection, DatabaseType
from metadata.catalog import MetadataCatalog


# ============================================================================
# Mock Database Connections
# ============================================================================

@pytest.fixture
def mock_mysql_connection():
    """Mock MySQL database connection."""
    mock_conn = Mock(spec=DatabaseConnection)
    mock_conn.db_type = DatabaseType.MYSQL
    mock_conn.connection = MagicMock()
    mock_conn.cursor = MagicMock()
    mock_conn.is_connected = True

    # Mock methods
    mock_conn.connect = Mock(return_value=True)
    mock_conn.disconnect = Mock(return_value=True)
    mock_conn.execute = Mock(return_value=True)
    mock_conn.fetch_one = Mock(return_value={})
    mock_conn.fetch_all = Mock(return_value=[])
    mock_conn.execute_many = Mock(return_value=0)
    mock_conn.commit = Mock()
    mock_conn.rollback = Mock()

    return mock_conn


@pytest.fixture
def mock_sqlserver_connection():
    """Mock SQL Server database connection."""
    mock_conn = Mock(spec=DatabaseConnection)
    mock_conn.db_type = DatabaseType.SQL_SERVER
    mock_conn.connection = MagicMock()
    mock_conn.cursor = MagicMock()
    mock_conn.is_connected = True

    # Mock methods
    mock_conn.connect = Mock(return_value=True)
    mock_conn.disconnect = Mock(return_value=True)
    mock_conn.execute = Mock(return_value=True)
    mock_conn.fetch_one = Mock(return_value={})
    mock_conn.fetch_all = Mock(return_value=[])
    mock_conn.execute_many = Mock(return_value=0)
    mock_conn.commit = Mock()
    mock_conn.rollback = Mock()

    return mock_conn


# ============================================================================
# Mock Metadata Catalog
# ============================================================================

@pytest.fixture
def mock_catalog():
    """Mock metadata catalog."""
    mock_cat = Mock(spec=MetadataCatalog)

    # Mock methods
    mock_cat.connect = Mock(return_value=True)
    mock_cat.disconnect = Mock(return_value=True)
    mock_cat.get_table_mapping = Mock(return_value=sample_table_mapping())
    mock_cat.get_column_mappings = Mock(return_value=sample_column_mappings())
    mock_cat.get_lookup_mappings = Mock(return_value=sample_lookup_mappings())
    mock_cat.get_dq_rules = Mock(return_value=sample_dq_rules())
    mock_cat.get_dependencies = Mock(return_value=[])
    mock_cat.start_execution = Mock(return_value=1)
    mock_cat.end_execution = Mock()
    mock_cat.log_dq_violation = Mock()

    return mock_cat


# ============================================================================
# Sample Test Data
# ============================================================================

def sample_table_mapping() -> Dict[str, Any]:
    """Sample table mapping configuration."""
    return {
        'mapping_id': 1,
        'mapping_code': 'RAMIS_TAXPAYER_TO_PARTY',
        'mapping_name': 'RAMIS Taxpayer to Party',
        'source_system_id': 1,
        'source_schema': 'dbo',
        'source_table': 'TAXPAYER',
        'source_filter_condition': None,
        'target_schema': 'party',
        'target_table': 'party',
        'target_key_columns': '["party_id"]',
        'load_strategy': 'FULL',
        'merge_strategy': 'UPSERT',
        'is_active': True,
        'created_at': datetime.now(),
        'updated_at': datetime.now()
    }


def sample_column_mappings() -> List[Dict[str, Any]]:
    """Sample column mappings."""
    return [
        {
            'column_mapping_id': 1,
            'mapping_id': 1,
            'source_column': 'taxpayer_id',
            'target_column': 'party_id',
            'transformation_type': 'DIRECT',
            'transformation_logic': None,
            'data_type': 'VARCHAR(50)',
            'is_nullable': False,
            'is_key': True,
            'default_value': None
        },
        {
            'column_mapping_id': 2,
            'mapping_id': 1,
            'source_column': 'taxpayer_name',
            'target_column': 'party_name',
            'transformation_type': 'EXPRESSION',
            'transformation_logic': 'UPPER(TRIM({taxpayer_name}))',
            'data_type': 'VARCHAR(255)',
            'is_nullable': False,
            'is_key': False,
            'default_value': None
        },
        {
            'column_mapping_id': 3,
            'mapping_id': 1,
            'source_column': 'taxpayer_type',
            'target_column': 'party_type',
            'transformation_type': 'LOOKUP',
            'transformation_logic': None,
            'data_type': 'VARCHAR(20)',
            'is_nullable': False,
            'is_key': False,
            'default_value': None
        }
    ]


def sample_lookup_mappings() -> List[Dict[str, Any]]:
    """Sample lookup mappings."""
    return [
        {
            'lookup_id': 1,
            'mapping_id': 1,
            'column_mapping_id': 3,
            'source_value': 'I',
            'target_value': 'INDIVIDUAL',
            'description': 'Individual taxpayer'
        },
        {
            'lookup_id': 2,
            'mapping_id': 1,
            'column_mapping_id': 3,
            'source_value': 'C',
            'target_value': 'COMPANY',
            'description': 'Company taxpayer'
        }
    ]


def sample_dq_rules() -> List[Dict[str, Any]]:
    """Sample data quality rules."""
    return [
        {
            'rule_id': 1,
            'mapping_id': 1,
            'rule_code': 'PARTY_ID_NOT_NULL',
            'rule_name': 'Party ID Not Null',
            'rule_type': 'NOT_NULL',
            'column_name': 'party_id',
            'rule_parameters': None,
            'severity': 'ERROR',
            'is_active': True
        },
        {
            'rule_id': 2,
            'mapping_id': 1,
            'rule_code': 'PARTY_NAME_MIN_LENGTH',
            'rule_name': 'Party Name Minimum Length',
            'rule_type': 'LENGTH',
            'column_name': 'party_name',
            'rule_parameters': '{"min_length": 2, "max_length": 255}',
            'severity': 'WARNING',
            'is_active': True
        }
    ]


@pytest.fixture
def sample_source_rows():
    """Sample source data rows."""
    return [
        {
            'taxpayer_id': 'TP001',
            'taxpayer_name': '  john doe  ',
            'taxpayer_type': 'I',
            'registration_date': '2023-01-15'
        },
        {
            'taxpayer_id': 'TP002',
            'taxpayer_name': 'ABC Company Ltd',
            'taxpayer_type': 'C',
            'registration_date': '2023-02-20'
        },
        {
            'taxpayer_id': 'TP003',
            'taxpayer_name': 'Jane Smith',
            'taxpayer_type': 'I',
            'registration_date': '2023-03-10'
        }
    ]


@pytest.fixture
def sample_transformed_rows():
    """Sample transformed data rows."""
    return [
        {
            'party_id': 'TP001',
            'party_name': 'JOHN DOE',
            'party_type': 'INDIVIDUAL'
        },
        {
            'party_id': 'TP002',
            'party_name': 'ABC COMPANY LTD',
            'party_type': 'COMPANY'
        },
        {
            'party_id': 'TP003',
            'party_name': 'JANE SMITH',
            'party_type': 'INDIVIDUAL'
        }
    ]


# ============================================================================
# Database Configuration Fixtures
# ============================================================================

@pytest.fixture
def test_db_config():
    """Test database configuration."""
    return {
        'host': 'localhost',
        'port': 3308,
        'user': 'test_user',
        'password': 'test_pass',
        'database': 'test_db'
    }


@pytest.fixture
def test_sqlserver_config():
    """Test SQL Server configuration."""
    return {
        'host': 'localhost',
        'port': 1433,
        'user': 'test_user',
        'password': 'test_pass',
        'database': 'test_db'
    }


# ============================================================================
# Cleanup Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables before each test."""
    # Store original environment
    original_env = os.environ.copy()

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


# ============================================================================
# Temporary File Fixtures
# ============================================================================

@pytest.fixture
def temp_log_file(tmp_path):
    """Create temporary log file."""
    log_file = tmp_path / "test.log"
    return str(log_file)


# ============================================================================
# Helper Functions
# ============================================================================

def assert_dict_contains(actual: Dict, expected: Dict):
    """Assert that actual dict contains all keys/values from expected dict."""
    for key, value in expected.items():
        assert key in actual, f"Key '{key}' not found in actual dict"
        assert actual[key] == value, f"Value mismatch for key '{key}': {actual[key]} != {value}"


def assert_row_equal(actual: Dict, expected: Dict, ignore_keys: List[str] = None):
    """Assert two rows are equal, optionally ignoring certain keys."""
    ignore_keys = ignore_keys or []

    actual_filtered = {k: v for k, v in actual.items() if k not in ignore_keys}
    expected_filtered = {k: v for k, v in expected.items() if k not in ignore_keys}

    assert actual_filtered == expected_filtered


# Make helper functions available to all tests
pytest.assert_dict_contains = assert_dict_contains
pytest.assert_row_equal = assert_row_equal
