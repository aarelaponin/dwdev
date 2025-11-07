"""
Database configuration for TA-RDM Source Ingestion.

Manages database connection settings for:
- L2 MySQL Canonical Database (target)
- Configuration MySQL Database (metadata)
- RAMIS SQL Server (source)
- Staging MySQL Database (intermediate layer)

Loads configuration from environment variables (.env file).
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# ==============================================================================
# L2 MySQL Canonical Database Configuration (Target)
# ==============================================================================

L2_DB_CONFIG: Dict[str, Any] = {
    'host': os.getenv('L2_DB_HOST', 'localhost'),
    'port': int(os.getenv('L2_DB_PORT', 3308)),
    'user': os.getenv('L2_DB_USER', 'ta_user'),
    'password': os.getenv('L2_DB_PASSWORD', ''),
    'charset': 'utf8mb4',
    'use_unicode': True,
    'autocommit': False,  # Use transactions
    'raise_on_warnings': True,
    'pool_size': int(os.getenv('L2_DB_POOL_SIZE', 5)),
    'pool_recycle': int(os.getenv('L2_DB_POOL_RECYCLE', 3600)),
    'connect_timeout': int(os.getenv('L2_DB_CONNECT_TIMEOUT', 10))
}

# All 12 schemas in the TA-RDM L2 canonical model
L2_SCHEMAS = [
    'reference',           # Reference/lookup tables
    'party',              # Party management (individuals, enterprises)
    'tax_framework',      # Tax types, periods, rates, forms
    'registration',       # Taxpayer registration
    'filing_assessment',  # Returns, assessments, penalties
    'accounting',         # Accounts, transactions, balances
    'payment_refund',     # Payments, refunds, allocations
    'compliance_control', # Audits, collection, appeals, risk profiles
    'document_management',# Documents, workflows, communications
    'withholding_tax',    # PAYE, employment withholding
    'vat',               # VAT-specific tables
    'income_tax'         # Income tax-specific tables
]


# ==============================================================================
# Configuration MySQL Database (Metadata)
# ==============================================================================

CONFIG_DB_CONFIG: Dict[str, Any] = {
    'host': os.getenv('CONFIG_DB_HOST', 'localhost'),
    'port': int(os.getenv('CONFIG_DB_PORT', 3308)),
    'user': os.getenv('CONFIG_DB_USER', 'ta_user'),
    'password': os.getenv('CONFIG_DB_PASSWORD', ''),
    'database': os.getenv('CONFIG_DB_DATABASE', 'ta_rdm_config'),
    'charset': 'utf8mb4',
    'use_unicode': True,
    'autocommit': False,
    'raise_on_warnings': True,
    'pool_size': int(os.getenv('CONFIG_DB_POOL_SIZE', 3)),
    'pool_recycle': int(os.getenv('CONFIG_DB_POOL_RECYCLE', 3600))
}


# ==============================================================================
# RAMIS SQL Server Source Database
# ==============================================================================

RAMIS_DB_CONFIG: Dict[str, Any] = {
    'host': os.getenv('RAMIS_DB_HOST', 'localhost'),
    'port': int(os.getenv('RAMIS_DB_PORT', 1433)),
    'user': os.getenv('RAMIS_DB_USER', 'ramis_reader'),
    'password': os.getenv('RAMIS_DB_PASSWORD', ''),
    'database': os.getenv('RAMIS_DB_DATABASE', 'RAMIS'),
    'timeout': int(os.getenv('RAMIS_DB_TIMEOUT', 30)),
    'login_timeout': 10
}


# ==============================================================================
# Staging MySQL Database (Layer 1)
# ==============================================================================

STAGING_DB_CONFIG: Dict[str, Any] = {
    'host': os.getenv('STAGING_DB_HOST', 'localhost'),
    'port': int(os.getenv('STAGING_DB_PORT', 3308)),
    'user': os.getenv('STAGING_DB_USER', 'ta_user'),
    'password': os.getenv('STAGING_DB_PASSWORD', ''),
    'database': os.getenv('STAGING_DB_DATABASE', 'staging'),
    'charset': 'utf8mb4',
    'use_unicode': True,
    'autocommit': False,
    'raise_on_warnings': True
}


# ==============================================================================
# ETL Configuration
# ==============================================================================

ETL_CONFIG: Dict[str, Any] = {
    'batch_size': int(os.getenv('ETL_BATCH_SIZE', 10000)),
    'dry_run': os.getenv('ETL_DRY_RUN', 'false').lower() == 'true',
    'rollback_on_error': os.getenv('ETL_ROLLBACK_ON_ERROR', 'true').lower() == 'true',
    'max_retries': int(os.getenv('ETL_MAX_RETRIES', 3)),
    'retry_delay': int(os.getenv('ETL_RETRY_DELAY', 5))
}


# ==============================================================================
# Data Quality Configuration
# ==============================================================================

DQ_CONFIG: Dict[str, Any] = {
    'validation_mode': os.getenv('DQ_VALIDATION_MODE', 'REJECT').upper(),
    'log_violations': os.getenv('DQ_LOG_VIOLATIONS', 'true').lower() == 'true',
    'max_violations_log': int(os.getenv('DQ_MAX_VIOLATIONS_LOG', 1000))
}


# ==============================================================================
# Incremental Load Configuration
# ==============================================================================

INCREMENTAL_CONFIG: Dict[str, Any] = {
    'enabled': os.getenv('INCREMENTAL_LOAD_ENABLED', 'true').lower() == 'true',
    'timestamp_column': os.getenv('INCREMENTAL_TIMESTAMP_COLUMN', 'last_modified_date'),
    'watermark_table': os.getenv('INCREMENTAL_WATERMARK_TABLE', 'ta_rdm_config.etl_watermarks')
}


# ==============================================================================
# Logging Configuration
# ==============================================================================

LOGGING_CONFIG: Dict[str, Any] = {
    'level': os.getenv('LOG_LEVEL', 'INFO').upper(),
    'file_path': os.getenv('LOG_FILE_PATH', ''),
    'colored_output': os.getenv('LOG_COLORED_OUTPUT', 'true').lower() == 'true',
    'log_sql': os.getenv('LOG_SQL_QUERIES', 'false').lower() == 'true'
}


# ==============================================================================
# Helper Functions
# ==============================================================================

def get_connection_string(config: Dict[str, Any], include_password: bool = False) -> str:
    """
    Generate a connection string for logging purposes.

    Args:
        config: Database configuration dictionary
        include_password: Whether to include password (default: False for security)

    Returns:
        str: Connection string
    """
    host = config.get('host', 'unknown')
    port = config.get('port', 0)
    user = config.get('user', 'unknown')
    database = config.get('database', '')

    conn_str = f"{user}@{host}:{port}"
    if database:
        conn_str += f"/{database}"

    if include_password and 'password' in config:
        conn_str = f"{user}:{config['password']}@{host}:{port}"
        if database:
            conn_str += f"/{database}"

    return conn_str


def validate_config(config_name: str, config: Dict[str, Any]) -> bool:
    """
    Validate that required database configuration is present.

    Args:
        config_name: Name of configuration (for error messages)
        config: Database configuration dictionary

    Returns:
        bool: True if configuration is valid

    Raises:
        ValueError: If required configuration is missing
    """
    required_fields = ['host', 'port', 'user', 'password']
    missing_fields = [
        field for field in required_fields
        if field not in config or not config[field]
    ]

    if missing_fields:
        raise ValueError(
            f"Missing required {config_name} configuration: {', '.join(missing_fields)}. "
            f"Please check your .env file."
        )

    return True


def validate_all_configs() -> bool:
    """
    Validate all database configurations.

    Returns:
        bool: True if all configurations are valid

    Raises:
        ValueError: If any required configuration is missing
    """
    configs_to_validate = [
        ('L2_DB', L2_DB_CONFIG),
        ('CONFIG_DB', CONFIG_DB_CONFIG),
        ('RAMIS_DB', RAMIS_DB_CONFIG),
        ('STAGING_DB', STAGING_DB_CONFIG)
    ]

    for config_name, config in configs_to_validate:
        validate_config(config_name, config)

    return True


def get_db_config(db_name: str) -> Dict[str, Any]:
    """
    Get database configuration by name.

    Args:
        db_name: Database name ('l2', 'config', 'ramis', 'staging')

    Returns:
        Dict[str, Any]: Database configuration

    Raises:
        ValueError: If database name is invalid
    """
    db_configs = {
        'l2': L2_DB_CONFIG,
        'config': CONFIG_DB_CONFIG,
        'ramis': RAMIS_DB_CONFIG,
        'staging': STAGING_DB_CONFIG
    }

    db_name_lower = db_name.lower()
    if db_name_lower not in db_configs:
        raise ValueError(
            f"Invalid database name: {db_name}. "
            f"Valid options: {', '.join(db_configs.keys())}"
        )

    return db_configs[db_name_lower]


# ==============================================================================
# Validation on Import (Optional - Comment out if too strict)
# ==============================================================================

try:
    validate_all_configs()
except ValueError as e:
    print(f"WARNING: {e}")
    print("Some database configurations are missing. Please update your .env file.")
