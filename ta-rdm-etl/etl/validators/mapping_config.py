"""
L2 → L3 Mapping Configuration

Defines the relationships between MySQL L2 tables and ClickHouse L3 tables
for validation purposes.
"""

from typing import Dict, List, Any


# L2 → L3 ETL Mappings
ETL_MAPPINGS: Dict[str, Dict[str, Any]] = {

    'dim_party': {
        'l2_sources': [
            {
                'schema': 'party',
                'table': 'party',
                'join_type': 'main',
                'key_column': 'party_id'
            },
            {
                'schema': 'party',
                'table': 'individual',
                'join_type': 'left',
                'key_column': 'party_id'
            },
            {
                'schema': 'compliance_control',
                'table': 'taxpayer_risk_profile',
                'join_type': 'left',
                'key_column': 'party_id'
            }
        ],
        'l3_target': 'ta_dw.dim_party',
        'natural_key': 'party_id',
        'surrogate_key': 'party_key',
        'filters': {},
        'expected_ratio': 1.0,  # 1:1 mapping expected
        'mandatory_fields': ['party_id', 'tin', 'party_name', 'party_type'],
        'description': 'Party dimension with individuals and enterprises'
    },

    'dim_tax_type': {
        'l2_sources': [
            {
                'schema': 'tax_framework',
                'table': 'tax_type',
                'join_type': 'main',
                'key_column': 'tax_type_id'
            }
        ],
        'l3_target': 'ta_dw.dim_tax_type',
        'natural_key': 'tax_type_code',
        'surrogate_key': 'tax_type_key',
        'filters': {'is_active': True},
        'expected_ratio': 1.0,
        'mandatory_fields': ['tax_type_code', 'tax_type_name', 'tax_category'],
        'description': 'Tax type dimension (VAT, CIT, PIT, etc.)'
    },

    'dim_time': {
        'l2_sources': [],  # Generated, not from L2
        'l3_target': 'ta_dw.dim_time',
        'natural_key': 'date_key',
        'surrogate_key': 'date_key',
        'filters': {},
        'expected_count': 1096,  # 2023-01-01 to 2025-12-31
        'mandatory_fields': ['date_key', 'full_date', 'year', 'month', 'day_of_month'],
        'description': 'Time dimension (daily grain, 2023-2025)'
    },

    'fact_registration': {
        'l2_sources': [
            {
                'schema': 'registration',
                'table': 'tax_account',
                'join_type': 'main',
                'key_column': 'tax_account_id'
            },
            {
                'schema': 'party',
                'table': 'party',
                'join_type': 'inner',
                'key_column': 'party_id'
            }
        ],
        'l3_target': 'ta_dw.fact_registration',
        'natural_key': 'tax_account_id',
        'filters': {'account_status_code': 'ACTIVE'},
        'expected_ratio': 1.0,
        'mandatory_fields': ['tax_account_id', 'dim_party_key', 'dim_tax_type_key', 'dim_date_key'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Tax account registration facts'
    },

    'dim_tax_period': {
        'l2_sources': [
            {
                'schema': 'tax_framework',
                'table': 'tax_period',
                'join_type': 'main',
                'key_column': 'tax_period_id'
            }
        ],
        'l3_target': 'ta_dw.dim_tax_period',
        'natural_key': 'tax_period_id',
        'surrogate_key': 'period_key',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['tax_period_id', 'period_code', 'tax_type_code', 'period_year',
                             'period_start_date', 'period_end_date', 'filing_due_date'],
        'description': 'Tax period dimension (90 periods, 2023-2025)'
    },

    'fact_filing': {
        'l2_sources': [
            {
                'schema': 'filing_assessment',
                'table': 'tax_return',
                'join_type': 'main',
                'key_column': 'tax_return_id'
            }
        ],
        'l3_target': 'ta_dw.fact_filing',
        'natural_key': 'return_id',
        'filters': {'is_current_version': True},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_tax_period_key', 'dim_date_key',
                             'return_id', 'return_number', 'filing_date', 'due_date'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_tax_period_key': 'dim_tax_period',
            'dim_date_key': 'dim_time'
        },
        'description': 'Tax filing facts (69 returns)'
    },

    'fact_assessment': {
        'l2_sources': [
            {
                'schema': 'filing_assessment',
                'table': 'assessment',
                'join_type': 'main',
                'key_column': 'assessment_id'
            }
        ],
        'l3_target': 'ta_dw.fact_assessment',
        'natural_key': 'assessment_id',
        'filters': {'is_current_version': True},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_tax_period_key', 'dim_date_key',
                             'assessment_id', 'assessment_number', 'assessment_date', 'assessed_amount'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_tax_period_key': 'dim_tax_period',
            'dim_date_key': 'dim_time'
        },
        'description': 'Tax assessment facts (69 assessments)'
    },

    'fact_payment': {
        'l2_sources': [
            {
                'schema': 'payment_refund',
                'table': 'payment',
                'join_type': 'main',
                'key_column': 'payment_id'
            }
        ],
        'l3_target': 'ta_dw.fact_payment',
        'natural_key': 'payment_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_date_key',
                             'payment_id', 'payment_reference', 'payment_date', 'payment_amount'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Payment facts (78 payments)'
    },

    'fact_account_balance': {
        'l2_sources': [
            {
                'schema': 'accounting',
                'table': 'account_balance',
                'join_type': 'main',
                'key_column': 'account_balance_id'
            }
        ],
        'l3_target': 'ta_dw.fact_account_balance',
        'natural_key': 'tax_account_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_date_key',
                             'tax_account_id', 'snapshot_date', 'opening_balance', 'closing_balance'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Account balance snapshots (222 balances)'
    },

    'fact_collection': {
        'l2_sources': [
            {
                'schema': 'compliance_control',
                'table': 'enforcement_action',
                'join_type': 'main',
                'key_column': 'enforcement_action_id'
            },
            {
                'schema': 'compliance_control',
                'table': 'collection_case',
                'join_type': 'inner',
                'key_column': 'collection_case_id'
            }
        ],
        'l3_target': 'ta_dw.fact_collection',
        'natural_key': 'enforcement_action_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_date_key',
                             'collection_case_id', 'enforcement_action_id', 'case_number',
                             'debt_amount', 'collected_amount', 'action_date'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Collection and enforcement action facts (35 actions)'
    },

    'fact_refund': {
        'l2_sources': [
            {
                'schema': 'payment_refund',
                'table': 'refund',
                'join_type': 'main',
                'key_column': 'refund_id'
            }
        ],
        'l3_target': 'ta_dw.fact_refund',
        'natural_key': 'refund_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_date_key',
                             'refund_id', 'refund_number', 'refund_amount',
                             'net_refund_amount', 'request_date'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Refund transaction facts (9 refunds)'
    },

    'fact_audit': {
        'l2_sources': [
            {
                'schema': 'compliance_control',
                'table': 'audit_case',
                'join_type': 'main',
                'key_column': 'audit_case_id'
            }
        ],
        'l3_target': 'ta_dw.fact_audit',
        'natural_key': 'audit_case_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_date_key',
                             'audit_case_id', 'audit_case_number', 'audit_status',
                             'selection_date'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Audit accumulating snapshot facts (5 audits)'
    },

    'fact_objection': {
        'l2_sources': [
            {
                'schema': 'compliance_control',
                'table': 'objection_case',
                'join_type': 'main',
                'key_column': 'objection_case_id'
            }
        ],
        'l3_target': 'ta_dw.fact_objection',
        'natural_key': 'objection_case_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_tax_type_key', 'dim_date_key',
                             'objection_id', 'objection_number', 'disputed_amount',
                             'filing_date', 'objection_status'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_tax_type_key': 'dim_tax_type',
            'dim_date_key': 'dim_time'
        },
        'description': 'Objection and appeal facts (1 objection)'
    },

    'fact_risk_assessment': {
        'l2_sources': [
            {
                'schema': 'compliance_control',
                'table': 'taxpayer_risk_profile',
                'join_type': 'main',
                'key_column': 'taxpayer_risk_profile_id'
            }
        ],
        'l3_target': 'ta_dw.fact_risk_assessment',
        'natural_key': 'risk_profile_id',
        'filters': {},
        'expected_ratio': 1.0,
        'mandatory_fields': ['dim_party_key', 'dim_date_key', 'risk_profile_id',
                             'party_id', 'overall_risk_score', 'assessment_date',
                             'risk_rating'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_date_key': 'dim_time'
        },
        'description': 'Risk assessment snapshot facts (5 risk profiles)'
    },

    'fact_taxpayer_activity': {
        'l2_sources': [],  # Derived from L3 fact tables, no direct L2 source
        'l3_target': 'ta_dw.fact_taxpayer_activity',
        'natural_key': 'party_id',
        'filters': {},
        'expected_count': 30,  # Phase J: 5 parties × 6 months = 30 snapshots
        'mandatory_fields': ['dim_party_key', 'dim_date_key', 'snapshot_date',
                             'party_id', 'total_filings', 'total_payments',
                             'overall_compliance_score', 'current_risk_rating'],
        'dimension_fks': {
            'dim_party_key': 'dim_party',
            'dim_date_key': 'dim_time'
        },
        'description': 'Taxpayer activity snapshot facts (30 monthly snapshots for 5 parties)'
    }
}


def get_l2_main_table(mapping_name: str) -> Dict[str, str]:
    """
    Get the main L2 source table for a mapping.

    Args:
        mapping_name: Name of the mapping (e.g., 'dim_party')

    Returns:
        Dictionary with schema and table keys
    """
    mapping = ETL_MAPPINGS.get(mapping_name)
    if not mapping:
        raise ValueError(f"Unknown mapping: {mapping_name}")

    if not mapping['l2_sources']:
        return None

    main_source = [s for s in mapping['l2_sources'] if s['join_type'] == 'main']
    if not main_source:
        return None

    return {
        'schema': main_source[0]['schema'],
        'table': main_source[0]['table']
    }


def get_l3_target_table(mapping_name: str) -> str:
    """
    Get the L3 target table for a mapping.

    Args:
        mapping_name: Name of the mapping

    Returns:
        Fully qualified L3 table name
    """
    mapping = ETL_MAPPINGS.get(mapping_name)
    if not mapping:
        raise ValueError(f"Unknown mapping: {mapping_name}")

    return mapping['l3_target']
