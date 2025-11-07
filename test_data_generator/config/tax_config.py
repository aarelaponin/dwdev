"""
Tax configuration for Sri Lankan Tax Administration.

Contains tax rates, thresholds, filing frequencies, and other
tax-related constants for the TA-RDM test data generator.
"""

from typing import Dict, List
from datetime import date


# Sri Lankan Tax Types
TAX_TYPES: List[Dict] = [
    {
        'code': 'CIT',
        'name': 'Corporate Income Tax',
        'category': 'INCOME',
        'applies_to_individuals': False,
        'applies_to_enterprises': True,
        'filing_frequency': 'ANNUAL',
        'rate': 30.0
    },
    {
        'code': 'PIT',
        'name': 'Personal Income Tax',
        'category': 'INCOME',
        'applies_to_individuals': True,
        'applies_to_enterprises': False,
        'filing_frequency': 'ANNUAL',
        'rate': 24.0  # Top rate (progressive)
    },
    {
        'code': 'VAT',
        'name': 'Value Added Tax',
        'category': 'CONSUMPTION',
        'applies_to_individuals': False,
        'applies_to_enterprises': True,
        'filing_frequency': 'MONTHLY',
        'rate': 18.0  # Standard rate (as of 2024)
    },
    {
        'code': 'WHT',
        'name': 'Withholding Tax',
        'category': 'INCOME',
        'applies_to_individuals': True,
        'applies_to_enterprises': True,
        'filing_frequency': 'MONTHLY',
        'rate': 5.0  # Standard WHT rate
    },
    {
        'code': 'PAYE',
        'name': 'Pay As You Earn',
        'category': 'INCOME',
        'applies_to_individuals': True,
        'applies_to_enterprises': False,
        'filing_frequency': 'MONTHLY',
        'rate': 18.0  # Variable based on income
    }
]


# Filing Types
FILING_TYPES: List[Dict] = [
    {
        'code': 'ORIGINAL',
        'name': 'Original Return',
        'description': 'First-time filing for a period'
    },
    {
        'code': 'AMENDED',
        'name': 'Amended Return',
        'description': 'Correction to previously filed return'
    },
    {
        'code': 'LATE',
        'name': 'Late Return',
        'description': 'Filing after the due date'
    }
]


# Payment Methods
PAYMENT_METHODS: List[Dict] = [
    {'code': 'BANK_TRANSFER', 'name': 'Bank Transfer'},
    {'code': 'ONLINE', 'name': 'Online Payment'},
    {'code': 'CHECK', 'name': 'Check/Cheque'},
    {'code': 'CASH', 'name': 'Cash Payment'},
    {'code': 'CARD', 'name': 'Credit/Debit Card'}
]


# Party Types
PARTY_TYPES: List[Dict] = [
    {
        'code': 'INDIVIDUAL',
        'name': 'Individual',
        'description': 'Natural person'
    },
    {
        'code': 'ENTERPRISE',
        'name': 'Enterprise',
        'description': 'Business entity (company, partnership, etc.)'
    }
]


# Party Segments (based on revenue/size)
PARTY_SEGMENTS: List[Dict] = [
    {
        'code': 'LARGE',
        'name': 'Large Taxpayer',
        'description': 'Annual revenue > 1 billion LKR',
        'min_revenue': 1_000_000_000
    },
    {
        'code': 'MEDIUM',
        'name': 'Medium Taxpayer',
        'description': 'Annual revenue 100M - 1B LKR',
        'min_revenue': 100_000_000,
        'max_revenue': 1_000_000_000
    },
    {
        'code': 'SMALL',
        'name': 'Small Taxpayer',
        'description': 'Annual revenue 10M - 100M LKR',
        'min_revenue': 10_000_000,
        'max_revenue': 100_000_000
    },
    {
        'code': 'MICRO',
        'name': 'Micro Taxpayer',
        'description': 'Annual revenue < 10M LKR',
        'max_revenue': 10_000_000
    }
]


# Risk Ratings
RISK_RATINGS: List[Dict] = [
    {'code': 'LOW', 'name': 'Low Risk', 'score': 1},
    {'code': 'MEDIUM', 'name': 'Medium Risk', 'score': 2},
    {'code': 'HIGH', 'name': 'High Risk', 'score': 3},
    {'code': 'VERY_HIGH', 'name': 'Very High Risk', 'score': 4},
    {'code': 'CRITICAL', 'name': 'Critical Risk', 'score': 5}
]


# Status Codes (subset for Phase A)
STATUS_CODES: List[Dict] = [
    # Party statuses
    {'code': 'ACTIVE', 'name': 'Active', 'category': 'PARTY'},
    {'code': 'INACTIVE', 'name': 'Inactive', 'category': 'PARTY'},
    {'code': 'SUSPENDED', 'name': 'Suspended', 'category': 'PARTY'},
    {'code': 'CLOSED', 'name': 'Closed', 'category': 'PARTY'},

    # Registration statuses
    {'code': 'REGISTERED', 'name': 'Registered', 'category': 'REGISTRATION'},
    {'code': 'PENDING', 'name': 'Pending', 'category': 'REGISTRATION'},
    {'code': 'DEREGISTERED', 'name': 'Deregistered', 'category': 'REGISTRATION'},

    # Filing statuses
    {'code': 'FILED', 'name': 'Filed', 'category': 'FILING'},
    {'code': 'ASSESSED', 'name': 'Assessed', 'category': 'FILING'},
    {'code': 'OVERDUE', 'name': 'Overdue', 'category': 'FILING'},

    # Payment statuses
    {'code': 'PAID', 'name': 'Paid', 'category': 'PAYMENT'},
    {'code': 'UNPAID', 'name': 'Unpaid', 'category': 'PAYMENT'},
    {'code': 'PARTIAL', 'name': 'Partially Paid', 'category': 'PAYMENT'},

    # Generic statuses
    {'code': 'APPROVED', 'name': 'Approved', 'category': 'GENERIC'},
    {'code': 'REJECTED', 'name': 'Rejected', 'category': 'GENERIC'},
    {'code': 'DRAFT', 'name': 'Draft', 'category': 'GENERIC'}
]


# ISIC Industry Codes (top 25 for Sri Lanka)
INDUSTRY_CODES: List[Dict] = [
    {'code': 'A01', 'name': 'Crop and animal production', 'section': 'A'},
    {'code': 'C10', 'name': 'Manufacture of food products', 'section': 'C'},
    {'code': 'C13', 'name': 'Manufacture of textiles', 'section': 'C'},
    {'code': 'C14', 'name': 'Manufacture of wearing apparel', 'section': 'C'},
    {'code': 'C24', 'name': 'Manufacture of basic metals', 'section': 'C'},
    {'code': 'D35', 'name': 'Electricity, gas, steam', 'section': 'D'},
    {'code': 'E38', 'name': 'Waste collection, treatment', 'section': 'E'},
    {'code': 'F41', 'name': 'Construction of buildings', 'section': 'F'},
    {'code': 'G45', 'name': 'Wholesale/retail of motor vehicles', 'section': 'G'},
    {'code': 'G46', 'name': 'Wholesale trade', 'section': 'G'},
    {'code': 'G47', 'name': 'Retail trade', 'section': 'G'},
    {'code': 'H49', 'name': 'Land transport', 'section': 'H'},
    {'code': 'H52', 'name': 'Warehousing and support', 'section': 'H'},
    {'code': 'I55', 'name': 'Accommodation', 'section': 'I'},
    {'code': 'I56', 'name': 'Food and beverage service', 'section': 'I'},
    {'code': 'J58', 'name': 'Publishing activities', 'section': 'J'},
    {'code': 'J62', 'name': 'Computer programming', 'section': 'J'},
    {'code': 'J63', 'name': 'Information service activities', 'section': 'J'},
    {'code': 'K64', 'name': 'Financial service activities', 'section': 'K'},
    {'code': 'L68', 'name': 'Real estate activities', 'section': 'L'},
    {'code': 'M69', 'name': 'Legal and accounting', 'section': 'M'},
    {'code': 'M70', 'name': 'Management consultancy', 'section': 'M'},
    {'code': 'N77', 'name': 'Rental and leasing', 'section': 'N'},
    {'code': 'P85', 'name': 'Education', 'section': 'P'},
    {'code': 'Q86', 'name': 'Human health activities', 'section': 'Q'}
]


# Currency
CURRENCY: Dict = {
    'iso_code': 'LKR',
    'iso_numeric': '144',
    'name': 'Sri Lankan Rupee',
    'symbol': 'Rs.',
    'minor_unit': 2
}


# Tax Year Configuration
TAX_YEAR_START_MONTH = 1  # January (calendar year)
TAX_YEAR_START_DAY = 1

# Test Data Date Range
TEST_DATA_START_DATE = date(2023, 1, 1)
TEST_DATA_END_DATE = date(2024, 12, 31)


# TIN Configuration
TIN_START_NUMBER = 100000001  # Starting TIN for test data
TIN_LENGTH = 9


# Default user for audit fields
DEFAULT_USER = 'system_generator'
