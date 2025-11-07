"""
Custom Faker provider for Sri Lankan data.

Generates realistic Sri Lankan names, addresses, phone numbers,
NIC numbers, and other localized data.
"""

import random
from datetime import date, timedelta
from faker import Faker
from faker.providers import BaseProvider

from config.party_profiles import (
    SRI_LANKAN_NAMES,
    ADDRESS_COMPONENTS,
    CONTACT_PATTERNS,
    NIC_PATTERNS
)


class SriLankaProvider(BaseProvider):
    """
    Custom Faker provider for Sri Lankan data.
    """

    # Sri Lankan provinces (9 provinces)
    provinces = [
        'Western', 'Central', 'Southern', 'Northern', 'Eastern',
        'North Western', 'North Central', 'Sabaragamuwa', 'Uva'
    ]

    # Major districts (top 25)
    districts = {
        'Western': ['Colombo', 'Gampaha', 'Kalutara'],
        'Central': ['Kandy', 'Matale', 'Nuwara Eliya'],
        'Southern': ['Galle', 'Matara', 'Hambantota'],
        'Northern': ['Jaffna', 'Kilinochchi', 'Mannar', 'Vavuniya', 'Mullaitivu'],
        'Eastern': ['Batticaloa', 'Ampara', 'Trincomalee'],
        'North Western': ['Kurunegala', 'Puttalam'],
        'North Central': ['Anuradhapura', 'Polonnaruwa'],
        'Sabaragamuwa': ['Ratnapura', 'Kegalle'],
        'Uva': ['Badulla', 'Monaragala']
    }

    # Major cities/localities
    localities = {
        'Colombo': ['Colombo 1', 'Colombo 2', 'Colombo 3', 'Colombo 4', 'Colombo 5',
                    'Colombo 7', 'Dehiwala', 'Mount Lavinia', 'Nugegoda', 'Maharagama'],
        'Gampaha': ['Gampaha', 'Negombo', 'Ja-Ela', 'Kadawatha', 'Ragama'],
        'Kandy': ['Kandy', 'Peradeniya', 'Katugastota', 'Gampola'],
        'Galle': ['Galle', 'Hikkaduwa', 'Ambalangoda'],
        'Matara': ['Matara', 'Weligama', 'Akuressa'],
        'Jaffna': ['Jaffna', 'Chavakachcheri', 'Point Pedro'],
        'Batticaloa': ['Batticaloa', 'Kalmunai'],
        'Kurunegala': ['Kurunegala', 'Kuliyapitiya'],
        'Anuradhapura': ['Anuradhapura'],
        'Ratnapura': ['Ratnapura', 'Embilipitiya']
    }

    def sri_lankan_first_name(self, gender: str = None) -> str:
        """
        Generate a Sri Lankan first name.

        Args:
            gender: 'M' for male, 'F' for female, None for random

        Returns:
            str: First name
        """
        if gender is None:
            gender = random.choice(['M', 'F'])

        if gender == 'M':
            return random.choice(SRI_LANKAN_NAMES['male_first_names'])
        else:
            return random.choice(SRI_LANKAN_NAMES['female_first_names'])

    def sri_lankan_last_name(self) -> str:
        """
        Generate a Sri Lankan last name.

        Returns:
            str: Last name
        """
        return random.choice(SRI_LANKAN_NAMES['last_names'])

    def sri_lankan_full_name(self, gender: str = None) -> str:
        """
        Generate a full Sri Lankan name.

        Args:
            gender: 'M' for male, 'F' for female, None for random

        Returns:
            str: Full name
        """
        first_name = self.sri_lankan_first_name(gender)
        last_name = self.sri_lankan_last_name()
        return f"{first_name} {last_name}"

    def sri_lankan_province(self) -> str:
        """Generate a Sri Lankan province name."""
        return random.choice(self.provinces)

    def sri_lankan_district(self, province: str = None) -> str:
        """
        Generate a Sri Lankan district name.

        Args:
            province: Province name (if None, random province)

        Returns:
            str: District name
        """
        if province is None:
            province = self.sri_lankan_province()

        return random.choice(self.districts.get(province, ['Colombo']))

    def sri_lankan_locality(self, district: str = None) -> str:
        """
        Generate a Sri Lankan locality (city/town).

        Args:
            district: District name (if None, random district)

        Returns:
            str: Locality name
        """
        if district is None:
            district = random.choice(list(self.localities.keys()))

        return random.choice(self.localities.get(district, ['Colombo']))

    def sri_lankan_address(self, province: str = None) -> dict:
        """
        Generate a complete Sri Lankan address.

        Args:
            province: Province name (if None, random)

        Returns:
            dict: Address components
        """
        if province is None:
            province = self.sri_lankan_province()

        district = self.sri_lankan_district(province)
        locality = self.sri_lankan_locality(district)

        # Generate street address
        building_number = random.randint(1, 999)
        street_prefix = random.choice(ADDRESS_COMPONENTS['street_prefixes'])
        building_type = random.choice(ADDRESS_COMPONENTS['building_types'])

        if building_type:
            street_address = f"{building_number} {street_prefix}, {building_type}"
        else:
            street_address = f"{building_number} {street_prefix}"

        return {
            'street_address': street_address,
            'locality': locality,
            'district': district,
            'province': province,
            'postal_code': str(random.randint(10000, 99999)),
            'country': 'Sri Lanka'
        }

    def sri_lankan_mobile_number(self) -> str:
        """
        Generate a Sri Lankan mobile phone number.

        Returns:
            str: Mobile number in format +94 7X XXX XXXX
        """
        prefix = random.choice(CONTACT_PATTERNS['mobile_prefixes'])
        number = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        return f"+94 {prefix} {number[:3]} {number[3:]}"

    def sri_lankan_landline_number(self, locality: str = 'Colombo') -> str:
        """
        Generate a Sri Lankan landline phone number.

        Args:
            locality: Locality name for area code

        Returns:
            str: Landline number in format +94 XX XXX XXXX
        """
        area_codes = CONTACT_PATTERNS['landline_area_codes']
        area_code = area_codes.get(locality, '011')  # Default to Colombo
        number = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        return f"+94 {area_code} {number[:3]} {number[3:]}"

    def sri_lankan_nic_old(self, birth_date: date, gender: str) -> str:
        """
        Generate old format Sri Lankan NIC (before 2016).

        Format: YYDDDGGGGV
        - YY: Last 2 digits of year
        - DDD: Day of year (001-366 for males, 501-866 for females)
        - GGGG: Sequential number
        - V: Validation character

        Args:
            birth_date: Date of birth
            gender: 'M' or 'F'

        Returns:
            str: Old format NIC (e.g., 925671234V)
        """
        year_2digit = birth_date.year % 100
        day_of_year = birth_date.timetuple().tm_yday

        # Add gender offset
        if gender == 'F':
            day_of_year += 500

        # Generate sequential number
        sequential = random.randint(1000, 9999)

        return f"{year_2digit:02d}{day_of_year:03d}{sequential}V"

    def sri_lankan_nic_new(self, birth_date: date, gender: str) -> str:
        """
        Generate new format Sri Lankan NIC (2016 onwards).

        Format: YYYYDDDGGGGG
        - YYYY: Full year
        - DDD: Day of year (001-366 for males, 501-866 for females)
        - GGGGG: Sequential number

        Args:
            birth_date: Date of birth
            gender: 'M' or 'F'

        Returns:
            str: New format NIC (e.g., 199256712345)
        """
        year = birth_date.year
        day_of_year = birth_date.timetuple().tm_yday

        # Add gender offset
        if gender == 'F':
            day_of_year += 500

        # Generate sequential number
        sequential = random.randint(10000, 99999)

        return f"{year}{day_of_year:03d}{sequential}"

    def sri_lankan_nic(self, birth_date: date = None, gender: str = None,
                       old_format: bool = None) -> str:
        """
        Generate a Sri Lankan NIC number (either format).

        Args:
            birth_date: Date of birth (if None, random age 20-65)
            gender: 'M' or 'F' (if None, random)
            old_format: True for old format, False for new, None for random

        Returns:
            str: NIC number
        """
        # Generate random birth date if not provided
        if birth_date is None:
            age_years = random.randint(20, 65)
            birth_date = date.today() - timedelta(days=age_years * 365)

        # Random gender if not provided
        if gender is None:
            gender = random.choice(['M', 'F'])

        # Choose format based on birth year or random
        if old_format is None:
            # Old format for people born before 2000
            old_format = birth_date.year < 2000

        if old_format:
            return self.sri_lankan_nic_old(birth_date, gender)
        else:
            return self.sri_lankan_nic_new(birth_date, gender)

    def sri_lankan_tin(self, start_number: int = 100000001) -> str:
        """
        Generate a Sri Lankan TIN (Tax Identification Number).

        Format: 9 digits

        Args:
            start_number: Starting TIN number

        Returns:
            str: TIN (e.g., 100000001)
        """
        # This will be incremented in the actual generator
        return str(start_number)

    def sri_lankan_brn(self, sequential: int) -> str:
        """
        Generate a Sri Lankan BRN (Business Registration Number).

        Format: PV########

        Args:
            sequential: Sequential number

        Returns:
            str: BRN (e.g., PV00012345)
        """
        return f"PV{sequential:08d}"


def get_faker_sri_lanka(seed: int = None) -> Faker:
    """
    Get a Faker instance with Sri Lankan provider.

    Args:
        seed: Random seed for reproducibility

    Returns:
        Faker: Faker instance with Sri Lankan provider
    """
    fake = Faker()

    # Add custom Sri Lankan provider
    fake.add_provider(SriLankaProvider)

    # Set seed for deterministic generation
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    return fake
