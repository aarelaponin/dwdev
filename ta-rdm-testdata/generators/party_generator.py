"""
Party Data Generator for TA-RDM (Phase A.2).

Generates 5 test parties (3 individuals, 2 "enterprises") with
risk profiles for validation testing.
"""

import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Dict, List
import random

from utils.db_utils import DatabaseConnection
from utils.faker_sri_lanka import get_faker_sri_lanka
from utils.validators import validate_tin, validate_nic
from config.tax_config import TIN_START_NUMBER, DEFAULT_USER
from config.party_profiles import (
    INDIVIDUAL_PROFILES,
    ENTERPRISE_PROFILES
)


logger = logging.getLogger(__name__)


class PartyDataGenerator:
    """
    Generates party data for TA-RDM database.

    For Phase A: 5 parties (3 individuals, 2 enterprises)
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize party data generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.fake = get_faker_sri_lanka(seed)
        random.seed(seed)
        self.generated_ids: Dict[str, List] = {'party': [], 'individual': []}
        self.tin_counter = TIN_START_NUMBER

        # Cache reference data for Phase B
        self._cache_reference_data()

    def _cache_reference_data(self):
        """Cache reference data for efficient lookups (Phase B)."""
        logger.debug("Caching reference data...")

        # Get industries
        industry_rows = self.db.fetch_all(
            "SELECT industry_code FROM reference.ref_industry"
        )
        self.industries = [row[0] for row in industry_rows]

        # Get legal forms
        legal_form_rows = self.db.fetch_all(
            "SELECT legal_form_code FROM reference.ref_legal_form"
        )
        self.legal_forms = [row[0] for row in legal_form_rows]

        # Get localities
        locality_rows = self.db.fetch_all(
            "SELECT locality_code FROM reference.ref_locality"
        )
        self.localities = [row[0] for row in locality_rows]

        # Get districts
        district_rows = self.db.fetch_all(
            "SELECT district_code FROM reference.ref_district"
        )
        self.districts = [row[0] for row in district_rows]

        logger.debug(f"Cached {len(self.industries)} industries, {len(self.legal_forms)} legal forms, "
                    f"{len(self.localities)} localities, {len(self.districts)} districts")

    def generate_all(self, count: int = 5):
        """
        Generate all party data.

        Args:
            count: Number of parties to generate (default: 5)
        """
        logger.info("=" * 60)
        logger.info(f"Starting Party Data Generation (Phase A.2) - {count} parties")
        logger.info("=" * 60)

        try:
            # Calculate individual vs enterprise counts
            # For 5 parties: 3 individuals, 2 enterprises
            individual_count = int(count * 0.6)  # 60% individuals
            enterprise_count = count - individual_count

            # Generate individuals
            for i in range(individual_count):
                profile = INDIVIDUAL_PROFILES[i % len(INDIVIDUAL_PROFILES)]
                self.generate_individual(profile)

            # Generate enterprises (as party records without individual subtype)
            for i in range(enterprise_count):
                profile = ENTERPRISE_PROFILES[i % len(ENTERPRISE_PROFILES)]
                self.generate_enterprise(profile)

            # Generate risk profiles for all parties
            self.generate_risk_profiles()

            logger.info("=" * 60)
            logger.info("Party Data Generation Complete")
            logger.info("=" * 60)
            self._print_summary()

        except Exception as e:
            logger.error(f"Error generating party data: {e}")
            raise

    def generate_individual(self, profile: Dict):
        """
        Generate an individual party with individual subtype.

        Args:
            profile: Individual profile configuration
        """
        logger.info(f"Generating individual: {profile['profile_name']}")

        # Generate basic data
        gender = profile.get('gender', random.choice(['M', 'F']))
        first_name = self.fake.sri_lankan_first_name(gender)
        last_name = self.fake.sri_lankan_last_name()
        full_name = f"{first_name} {last_name}"

        # Generate birth date based on age range
        age = random.randint(*profile.get('age_range', (25, 65)))
        birth_date = date.today() - timedelta(days=age * 365)

        # Generate TIN
        tin = str(self.tin_counter)
        self.tin_counter += 1

        # Generate NIC
        nic = self.fake.sri_lankan_nic(birth_date, gender)

        # Phase B: Assign industry (individuals typically in service sectors)
        # Service sectors: G46 (Wholesale), M69 (Legal/accounting), N77 (Rental)
        individual_industries = ['G46', 'M69', 'N77', 'S96']  # Add personal services
        industry_code = random.choice(individual_industries) if self.industries else None

        # Phase B: Assign location
        locality_code = random.choice(self.localities) if self.localities else None
        district_code = random.choice(self.districts) if self.districts else None

        # Insert party record
        party_id = self._insert_party(
            party_type='INDIVIDUAL',
            party_name=full_name,
            party_short_name=f"{first_name[0]}. {last_name}",
            status='ACTIVE',
            tin=tin,
            industry_code=industry_code,
            country_code='LKA',
            district_code=district_code,
            locality_code=locality_code
        )

        # Insert individual record
        self._insert_individual(
            party_id=party_id,
            tin=tin,
            first_name=first_name,
            last_name=last_name,
            birth_date=birth_date,
            gender=gender,
            resident_flag=profile.get('resident_flag', True),
            marital_status=profile.get('marital_status', 'SINGLE')
        )

        self.generated_ids['party'].append(party_id)
        self.generated_ids['individual'].append(party_id)

        logger.info(f"  Created individual: {full_name} (TIN: {tin}, NIC: {nic})")

    def generate_enterprise(self, profile: Dict):
        """
        Generate an enterprise party (without individual subtype).

        Note: DDL doesn't have party.enterprise table, so we only create
        party record with party_type='ENTERPRISE'

        Args:
            profile: Enterprise profile configuration
        """
        logger.info(f"Generating enterprise: {profile['profile_name']}")

        # Generate enterprise name
        enterprise_name = self._generate_enterprise_name(profile)

        # Generate TIN
        tin = str(self.tin_counter)
        self.tin_counter += 1

        # Phase B: Assign industry (enterprises in manufacturing/retail/construction)
        # Manufacturing: C10 (Food), C13 (Textiles), C24 (Metals)
        # Retail: G45 (Motor vehicles), G46 (Wholesale)
        # Construction: F41
        enterprise_industries = ['C10', 'C13', 'C24', 'G45', 'G46', 'F41']
        industry_code = random.choice(enterprise_industries) if self.industries else None

        # Phase B: Assign legal form (enterprises only)
        # Most common: LTD (Private Limited), PLC (Public Limited)
        common_legal_forms = ['LTD', 'PLC', 'PARTNER']
        legal_form_code = random.choice(common_legal_forms) if self.legal_forms else None

        # Phase B: Generate registration date (2020-2023)
        years_ago = random.randint(2, 5)
        registration_date = date.today() - timedelta(days=years_ago * 365)

        # Phase B: Assign location
        locality_code = random.choice(self.localities) if self.localities else None
        district_code = random.choice(self.districts) if self.districts else None

        # Insert party record
        party_id = self._insert_party(
            party_type='ENTERPRISE',
            party_name=enterprise_name,
            party_short_name=enterprise_name[:50] if len(enterprise_name) > 50 else None,
            status='ACTIVE',
            tin=tin,
            industry_code=industry_code,
            legal_form_code=legal_form_code,
            registration_date=registration_date,
            country_code='LKA',
            district_code=district_code,
            locality_code=locality_code
        )

        self.generated_ids['party'].append(party_id)

        logger.info(f"  Created enterprise: {enterprise_name} (TIN: {tin}, Legal Form: {legal_form_code}, Industry: {industry_code})")

    def _generate_enterprise_name(self, profile: Dict) -> str:
        """
        Generate a realistic Sri Lankan enterprise name.

        Args:
            profile: Enterprise profile configuration

        Returns:
            str: Enterprise name
        """
        from config.party_profiles import ENTERPRISE_NAME_PATTERNS, ENTERPRISE_NAME_WORDS

        pattern = random.choice(ENTERPRISE_NAME_PATTERNS)
        last_name = self.fake.sri_lankan_last_name()

        name = pattern.format(
            word1=random.choice(ENTERPRISE_NAME_WORDS['word1']),
            word2=random.choice(ENTERPRISE_NAME_WORDS['word2']),
            industry=random.choice(ENTERPRISE_NAME_WORDS['industry']),
            location=self.fake.sri_lankan_locality(),
            founder=last_name
        )

        return name

    def _insert_party(self, party_type: str, party_name: str,
                     party_short_name: str = None, status: str = 'ACTIVE',
                     tin: str = None, industry_code: str = None,
                     legal_form_code: str = None, registration_date: date = None,
                     country_code: str = 'LKA', district_code: str = None,
                     locality_code: str = None) -> int:
        """
        Insert a party record (Phase B: now includes TIN, industry, location, etc.).

        Args:
            party_type: 'INDIVIDUAL' or 'ENTERPRISE'
            party_name: Full party name
            party_short_name: Abbreviated name
            status: Party status
            tin: Tax Identification Number
            industry_code: Industry classification code
            legal_form_code: Legal form code (enterprises only)
            registration_date: Business registration date (enterprises only)
            country_code: ISO 3166-1 alpha-3 country code
            district_code: District code
            locality_code: Locality/city code

        Returns:
            int: party_id of inserted record
        """
        query = """
            INSERT INTO party.party
            (party_uuid, party_type_code, party_status_code, party_name,
             party_short_name, tax_identification_number, industry_code,
             legal_form_code, registration_date, country_code, district_code,
             locality_code, valid_from, valid_to, is_current,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        party_uuid = str(uuid.uuid4())

        params = (
            party_uuid, party_type, status, party_name,
            party_short_name, tin, industry_code,
            legal_form_code, registration_date, country_code, district_code,
            locality_code, date.today(), None, True,
            DEFAULT_USER, datetime.now()
        )

        self.db.execute_query(query, params)
        return self.db.get_last_insert_id()

    def _insert_individual(self, party_id: int, tin: str, first_name: str,
                          last_name: str, birth_date: date, gender: str,
                          resident_flag: bool = True,
                          marital_status: str = 'SINGLE'):
        """
        Insert an individual record.

        Args:
            party_id: Foreign key to party
            tin: Tax Identification Number
            first_name: First name
            last_name: Last name
            birth_date: Date of birth
            gender: 'M' or 'F'
            resident_flag: Tax resident flag
            marital_status: Marital status code
        """
        query = """
            INSERT INTO party.individual
            (individual_id, party_id, tax_identification_number,
             first_name, last_name, middle_name, birth_date, gender_code,
             deceased_date, citizenship_country_code, resident_flag,
             marital_status_code, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            party_id, party_id, tin,
            first_name, last_name, None, birth_date, gender,
            None, 'LKA', resident_flag,
            marital_status, DEFAULT_USER, datetime.now()
        )

        self.db.execute_query(query, params)

    def generate_risk_profiles(self):
        """
        Generate risk profiles for all parties.
        """
        logger.info("Generating risk profiles...")

        # Risk rating distribution: mostly low-medium with some high
        risk_ratings = ['LOW', 'LOW', 'MEDIUM', 'MEDIUM', 'HIGH']

        for i, party_id in enumerate(self.generated_ids['party']):
            risk_rating = risk_ratings[i % len(risk_ratings)]
            self._insert_risk_profile(party_id, risk_rating)

        logger.info(f"  Generated {len(self.generated_ids['party'])} risk profile(s)")

    def _insert_risk_profile(self, party_id: int, risk_rating: str):
        """
        Insert a taxpayer risk profile.

        Args:
            party_id: Party ID
            risk_rating: Risk rating code
        """
        # Map risk ratings to scores
        risk_scores = {
            'LOW': random.uniform(10, 30),
            'MEDIUM': random.uniform(30, 60),
            'HIGH': random.uniform(60, 85),
            'VERY_HIGH': random.uniform(85, 95),
            'CRITICAL': random.uniform(95, 100)
        }

        overall_score = risk_scores.get(risk_rating, 50)

        # Generate component scores around overall score
        filing_score = overall_score + random.uniform(-10, 10)
        payment_score = overall_score + random.uniform(-10, 10)
        accuracy_score = overall_score + random.uniform(-10, 10)

        # Clamp scores to 0-100 and round to 2 decimal places (for DECIMAL(5,2))
        overall_score = round(max(0, min(100, overall_score)), 2)
        filing_score = round(max(0, min(100, filing_score)), 2)
        payment_score = round(max(0, min(100, payment_score)), 2)
        accuracy_score = round(max(0, min(100, accuracy_score)), 2)

        query = """
            INSERT INTO compliance_control.taxpayer_risk_profile
            (party_id, overall_risk_score, risk_rating_code,
             filing_risk_score, payment_risk_score, accuracy_risk_score,
             industry_risk_score, complexity_risk_score,
             last_audit_date, last_audit_outcome_code,
             audit_adjustment_history, late_filing_count_12m,
             non_filing_count_12m, late_payment_count_12m,
             current_arrears_amount, has_active_objection,
             objection_count_history, third_party_discrepancy_count,
             risk_factors, treatment_recommendation,
             profile_last_updated, created_by, created_date,
             modified_by, modified_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Generate risk factors narrative
        risk_factors = self._generate_risk_factors(risk_rating)

        # Determine treatment recommendation
        treatment = {
            'LOW': 'STANDARD_SERVICE',
            'MEDIUM': 'INCREASED_MONITORING',
            'HIGH': 'AUDIT_SELECTION',
            'VERY_HIGH': 'ENFORCEMENT_ACTION',
            'CRITICAL': 'ENFORCEMENT_ACTION'
        }.get(risk_rating, 'STANDARD_SERVICE')

        # Note: created_by and modified_by are BIGINT in this table (user IDs, not usernames)
        default_user_id = 1  # System user ID

        # Round currency amounts to 2 decimal places (for DECIMAL(19,2))
        current_arrears = 0.0 if risk_rating == 'LOW' else round(random.uniform(0, 100000), 2)

        params = (
            party_id, overall_score, risk_rating,
            filing_score, payment_score, accuracy_score,
            50.0, 50.0,  # Industry and complexity scores
            None, None,  # Last audit date and outcome
            0.0,  # Audit adjustment history
            0 if risk_rating == 'LOW' else random.randint(0, 3),  # Late filing count
            0 if risk_rating == 'LOW' else random.randint(0, 2),  # Non-filing count
            0 if risk_rating == 'LOW' else random.randint(0, 3),  # Late payment count
            current_arrears,  # Current arrears
            False,  # Has active objection
            0,  # Objection count history
            0 if risk_rating == 'LOW' else random.randint(0, 2),  # Third party discrepancy
            risk_factors, treatment,
            datetime.now(), default_user_id, datetime.now(),
            default_user_id, datetime.now()
        )

        self.db.execute_query(query, params)

    def _generate_risk_factors(self, risk_rating: str) -> str:
        """
        Generate narrative risk factors based on rating.

        Args:
            risk_rating: Risk rating code

        Returns:
            str: Risk factors narrative
        """
        risk_factors_map = {
            'LOW': 'Consistent filing and payment history. No compliance issues detected.',
            'MEDIUM': 'Occasional late filings. Generally compliant but requires monitoring.',
            'HIGH': 'History of late payments and filings. Potential audit candidate.',
            'VERY_HIGH': 'Significant compliance issues. Active enforcement required.',
            'CRITICAL': 'Serious non-compliance. Immediate enforcement action recommended.'
        }

        return risk_factors_map.get(risk_rating, 'Standard risk profile.')

    def _print_summary(self):
        """Print summary of generated party data."""
        logger.info("")
        logger.info("Summary of Generated Party Data:")
        logger.info(f"  Total Parties: {len(self.generated_ids['party'])}")
        logger.info(f"  Individuals: {len(self.generated_ids['individual'])}")
        logger.info(f"  Enterprises: {len(self.generated_ids['party']) - len(self.generated_ids['individual'])}")
        logger.info(f"  Risk Profiles: {len(self.generated_ids['party'])}")
        logger.info(f"  TINs Assigned: {TIN_START_NUMBER} to {self.tin_counter - 1}")
