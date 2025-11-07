"""
ClickHouse configuration for TA-RDM Test Data Generator.

Manages ClickHouse (Layer 3) connection settings and table definitions.
Layer 3 is the dimensional data warehouse (star schema).
"""

import os
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# ClickHouse connection configuration
CLICKHOUSE_CONFIG: Dict[str, any] = {
    'host': os.getenv('CH_HOST', 'localhost'),
    'port': int(os.getenv('CH_PORT', 8123)),
    'database': os.getenv('CH_DATABASE', 'ta_dw'),
    'username': os.getenv('CH_USER', 'default'),
    'password': os.getenv('CH_PASSWORD', ''),
    'secure': False,  # Use HTTPS for secure connections
    'compress': True  # Enable compression for better performance
}


# Dimension tables in ta_dw database
DIMENSION_TABLES: List[str] = [
    'dim_party',              # Taxpayer/party dimension (SCD Type 2)
    'dim_tax_type',           # Tax types dimension
    'dim_tax_period',         # Tax periods dimension
    'dim_geography',          # Geographic dimension (country, region, district, locality)
    'dim_industry',           # Industry classification dimension
    'dim_risk_rating',        # Risk rating dimension
    'dim_payment_method',     # Payment method dimension
    'dim_filing_status',      # Filing status dimension
    'dim_account_type',       # Account type dimension
    'dim_transaction_type',   # Transaction type dimension
    'dim_document_type',      # Document type dimension
    'dim_date'               # Date dimension (for time-series analysis)
]


# Fact tables in ta_dw database
FACT_TABLES: List[str] = [
    'fact_tax_registration',     # Tax registration facts
    'fact_tax_filing',            # Tax filing facts
    'fact_tax_assessment',        # Tax assessment facts
    'fact_tax_payment',           # Tax payment facts
    'fact_tax_refund',            # Tax refund facts
    'fact_account_balance',       # Account balance facts
    'fact_accounting_transaction',# Accounting transaction facts
    'fact_penalty',               # Penalty facts
    'fact_compliance_audit',      # Audit facts
    'fact_withholding_transaction',# Withholding tax facts
    'fact_vat_transaction',       # VAT transaction facts
    'fact_risk_assessment'        # Risk assessment facts
]


# All tables (dimensions + facts)
ALL_TABLES: List[str] = DIMENSION_TABLES + FACT_TABLES


def get_connection_string() -> str:
    """
    Returns a connection string for logging purposes (without password).

    Returns:
        str: Safe connection string
    """
    return (
        f"clickhouse://{CLICKHOUSE_CONFIG['username']}@"
        f"{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}"
        f"/{CLICKHOUSE_CONFIG['database']}"
    )


def validate_config() -> bool:
    """
    Validates that required ClickHouse configuration is present.

    Returns:
        bool: True if configuration is valid

    Raises:
        ValueError: If required configuration is missing
    """
    required_fields = ['host', 'port', 'database', 'username', 'password']
    missing_fields = [
        field for field in required_fields
        if not CLICKHOUSE_CONFIG.get(field) and field != 'password'
    ]

    if missing_fields:
        raise ValueError(
            f"Missing required ClickHouse configuration: {', '.join(missing_fields)}. "
            f"Please check your .env file."
        )

    return True


# Validate configuration on import
try:
    validate_config()
except ValueError as e:
    print(f"WARNING: {e}")
