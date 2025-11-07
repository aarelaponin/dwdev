"""
Party profile archetypes for test data generation.

Defines different types of taxpayers (individuals and enterprises)
with realistic characteristics for Sri Lankan context.
"""

from typing import Dict, List


# Individual Profiles
INDIVIDUAL_PROFILES: List[Dict] = [
    {
        'profile_name': 'Salaried Employee',
        'description': 'Regular employee with monthly salary',
        'party_segment': 'MICRO',
        'risk_rating': 'LOW',
        'annual_income_range': (1_200_000, 3_600_000),  # LKR 100K-300K/month
        'gender': 'M',
        'age_range': (25, 55),
        'marital_status': 'MARRIED',
        'resident_flag': True,
        'tax_types': ['PAYE', 'PIT']
    },
    {
        'profile_name': 'Self-Employed Professional',
        'description': 'Doctor, lawyer, accountant, consultant',
        'party_segment': 'SMALL',
        'risk_rating': 'MEDIUM',
        'annual_income_range': (5_000_000, 15_000_000),  # LKR 5-15M
        'gender': 'F',
        'age_range': (30, 60),
        'marital_status': 'SINGLE',
        'resident_flag': True,
        'tax_types': ['PIT', 'VAT']
    },
    {
        'profile_name': 'Small Business Owner',
        'description': 'Owner of small shop/restaurant',
        'party_segment': 'SMALL',
        'risk_rating': 'MEDIUM',
        'annual_income_range': (8_000_000, 25_000_000),  # LKR 8-25M
        'gender': 'M',
        'age_range': (35, 65),
        'marital_status': 'MARRIED',
        'resident_flag': True,
        'tax_types': ['PIT', 'VAT']
    }
]


# Enterprise Profiles
ENTERPRISE_PROFILES: List[Dict] = [
    {
        'profile_name': 'Textile Manufacturer',
        'description': 'Medium-sized garment factory',
        'party_segment': 'MEDIUM',
        'risk_rating': 'MEDIUM',
        'annual_revenue_range': (150_000_000, 500_000_000),  # LKR 150-500M
        'industry_code': 'C14',  # Manufacture of wearing apparel
        'employee_count_range': (100, 500),
        'registered_date_years_ago': 10,
        'tax_types': ['CIT', 'VAT', 'WHT', 'PAYE']
    },
    {
        'profile_name': 'IT Services Company',
        'description': 'Software development and IT consulting',
        'party_segment': 'SMALL',
        'risk_rating': 'LOW',
        'annual_revenue_range': (30_000_000, 80_000_000),  # LKR 30-80M
        'industry_code': 'J62',  # Computer programming
        'employee_count_range': (20, 50),
        'registered_date_years_ago': 5,
        'tax_types': ['CIT', 'VAT', 'WHT', 'PAYE']
    }
]


# Sri Lankan Geographic Distribution
# Most taxpayers are in Western Province (Colombo area)
GEOGRAPHIC_DISTRIBUTION: Dict = {
    'Western': 0.45,      # 45% in Western Province (Colombo, Gampaha)
    'Central': 0.15,      # 15% in Central Province (Kandy)
    'Southern': 0.12,     # 12% in Southern Province (Galle, Matara)
    'Northern': 0.08,     # 8% in Northern Province (Jaffna)
    'Eastern': 0.07,      # 7% in Eastern Province (Batticaloa)
    'North Western': 0.05,# 5% in North Western (Kurunegala)
    'North Central': 0.03,# 3% in North Central (Anuradhapura)
    'Sabaragamuwa': 0.03, # 3% in Sabaragamuwa (Ratnapura)
    'Uva': 0.02          # 2% in Uva Province
}


# Sri Lankan Name Components
SRI_LANKAN_NAMES: Dict = {
    'male_first_names': [
        'Kasun', 'Nuwan', 'Chaminda', 'Rohan', 'Sampath', 'Tharindu',
        'Lahiru', 'Damith', 'Ruwan', 'Asanka', 'Mahinda', 'Priyantha',
        'Ajith', 'Anura', 'Bandula', 'Chandana', 'Dilshan', 'Gamini'
    ],
    'female_first_names': [
        'Chamari', 'Dilini', 'Hashini', 'Kavindi', 'Nimali', 'Sanduni',
        'Anusha', 'Madhavi', 'Nadeeka', 'Samanthi', 'Thilini', 'Upeksha',
        'Amali', 'Binara', 'Chathuri', 'Dulani', 'Fathima', 'Hasara'
    ],
    'last_names': [
        'Fernando', 'Silva', 'Perera', 'Jayawardena', 'Dissanayake',
        'Gunasekara', 'Wickramasinghe', 'Bandara', 'Jayasuriya', 'Amarasinghe',
        'Kumara', 'Rathnayake', 'Mendis', 'Wijesinghe', 'Rodrigo',
        'De Silva', 'Gunawardena', 'Liyanage', 'Seneviratne', 'Rajapaksa'
    ]
}


# Sri Lankan Enterprise Names
ENTERPRISE_NAME_PATTERNS: List[str] = [
    '{word1} {word2} (Pvt) Ltd',
    '{location} {industry} Company',
    '{founder} & Sons',
    '{founder} {industry} Enterprises',
    'Ceylon {industry} Mills',
    'Lanka {industry} Corporation',
    'Sri Lankan {industry} Group'
]

ENTERPRISE_NAME_WORDS: Dict = {
    'word1': [
        'Golden', 'Premier', 'Elite', 'Royal', 'Supreme', 'United',
        'National', 'Modern', 'Classic', 'Quality'
    ],
    'word2': [
        'Textiles', 'Trading', 'Holdings', 'Industries', 'Services',
        'Solutions', 'Enterprises', 'Ventures', 'Group', 'Corporation'
    ],
    'industry': [
        'Textile', 'Garment', 'Trading', 'Hardware', 'Software',
        'Construction', 'Transport', 'Food', 'Tea', 'Rubber'
    ]
}


# Address Components
ADDRESS_COMPONENTS: Dict = {
    'street_prefixes': [
        'Galle Road', 'Duplication Road', 'Baseline Road', 'High Level Road',
        'Kandy Road', 'Main Street', 'Station Road', 'Temple Road',
        'Park Road', 'Lake Road', 'Hill Street', 'Market Street'
    ],
    'building_types': [
        '', 'Building', 'Apartment', 'House', 'Complex', 'Plaza',
        'Tower', 'Mansion', 'Villa', 'Residence'
    ]
}


# Contact Information Patterns
CONTACT_PATTERNS: Dict = {
    'mobile_prefixes': [
        '071', '072', '075', '076', '077', '078'  # Sri Lankan mobile prefixes
    ],
    'landline_area_codes': {
        'Colombo': '011',
        'Gampaha': '033',
        'Kandy': '081',
        'Galle': '091',
        'Matara': '041',
        'Jaffna': '021',
        'Batticaloa': '065',
        'Kurunegala': '037',
        'Anuradhapura': '025',
        'Ratnapura': '045'
    },
    'email_domains': [
        'gmail.com', 'yahoo.com', 'outlook.com', 'slt.lk', 'email.lk'
    ]
}


# NIC (National Identity Card) Patterns
NIC_PATTERNS: Dict = {
    'old_format': '{yy}{days}{gender}V',  # e.g., 925671234V (born 1992, day 256)
    'new_format': '{yyyy}{days}{gender}',  # e.g., 199256712345 (born 1992, day 256)
    'gender_offset': {
        'M': 0,    # Days 1-366
        'F': 500   # Days 501-866
    }
}


# BRN (Business Registration Number) Pattern
BRN_PATTERN = 'PV{number:08d}'  # e.g., PV00012345 for private limited companies


def get_profile_by_name(profile_name: str, profile_type: str = 'individual') -> Dict:
    """
    Get a specific profile by name.

    Args:
        profile_name: Name of the profile
        profile_type: 'individual' or 'enterprise'

    Returns:
        Dict: Profile configuration

    Raises:
        ValueError: If profile not found
    """
    profiles = INDIVIDUAL_PROFILES if profile_type == 'individual' else ENTERPRISE_PROFILES

    for profile in profiles:
        if profile['profile_name'] == profile_name:
            return profile

    raise ValueError(f"Profile '{profile_name}' not found in {profile_type} profiles")
