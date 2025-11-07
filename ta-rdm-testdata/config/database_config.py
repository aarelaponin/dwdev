"""
Database configuration for TA-RDM Test Data Generator.

Manages MySQL connection settings and schema definitions.
Uses schema-qualified table names to access all 12 schemas.
"""

import os
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# MySQL connection configuration
# NOTE: We do NOT specify 'database' - we use schema-qualified table names
DATABASE_CONFIG: Dict[str, any] = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3308)),
    'user': os.getenv('DB_USER', 'ta_testdata'),
    'password': os.getenv('DB_PASSWORD', ''),
    'charset': 'utf8mb4',
    'use_unicode': True,
    'autocommit': False,  # Use transactions
    'raise_on_warnings': True
}


# All 12 schemas in the TA-RDM model
SCHEMAS: List[str] = [
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


# Schema order for data generation (respects foreign key dependencies)
GENERATION_ORDER: List[str] = [
    'reference',           # First - all reference data
    'party',              # Second - party data depends on reference
    'compliance_control', # Third - risk profiles depend on party
    'tax_framework',      # Fourth - tax framework (Phase B)
    'registration',       # Fifth - registration depends on party + tax_framework
    'filing_assessment',  # Sixth - filings depend on registration
    'accounting',         # Seventh - accounting depends on filings
    'payment_refund',     # Eighth - payments depend on accounting
    'withholding_tax',    # Ninth - WHT transactions
    'vat',               # Tenth - VAT transactions
    'income_tax',        # Eleventh - Income tax transactions
    'document_management' # Last - documents can reference any entity
]


def get_connection_string() -> str:
    """
    Returns a connection string for logging purposes (without password).

    Returns:
        str: Safe connection string
    """
    return (
        f"mysql://{DATABASE_CONFIG['user']}@"
        f"{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}"
    )


def validate_config() -> bool:
    """
    Validates that required database configuration is present.

    Returns:
        bool: True if configuration is valid

    Raises:
        ValueError: If required configuration is missing
    """
    required_fields = ['host', 'port', 'user', 'password']
    missing_fields = [
        field for field in required_fields
        if not DATABASE_CONFIG.get(field)
    ]

    if missing_fields:
        raise ValueError(
            f"Missing required database configuration: {', '.join(missing_fields)}. "
            f"Please check your .env file."
        )

    return True


# Validate configuration on import
try:
    validate_config()
except ValueError as e:
    print(f"WARNING: {e}")
