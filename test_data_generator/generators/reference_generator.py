"""
Reference Data Generator for TA-RDM (Phase A.1).

Generates reference/lookup data for the Tax Administration system.
Approximately 200 records across ~15 reference tables.
"""

import logging
from datetime import date, datetime
from typing import Dict, List

from utils.db_utils import DatabaseConnection
from utils.faker_sri_lanka import get_faker_sri_lanka
from config.tax_config import (
    TAX_TYPES, FILING_TYPES, PAYMENT_METHODS, PARTY_TYPES,
    PARTY_SEGMENTS, RISK_RATINGS, STATUS_CODES, INDUSTRY_CODES,
    CURRENCY, DEFAULT_USER
)


logger = logging.getLogger(__name__)


class ReferenceDataGenerator:
    """
    Generates reference data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize reference data generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.fake = get_faker_sri_lanka(seed)
        self.generated_ids: Dict[str, Dict] = {}  # Track generated IDs

    def generate_all(self):
        """
        Generate all reference data in dependency order.
        """
        logger.info("=" * 60)
        logger.info("Starting Reference Data Generation (Phase A.1)")
        logger.info("=" * 60)

        try:
            # Geographic hierarchy (Country -> Region -> District -> Locality)
            self.generate_countries()
            self.generate_regions()
            self.generate_districts()
            self.generate_localities()

            # Currency
            self.generate_currencies()

            # Individual/person reference data
            self.generate_genders()
            self.generate_marital_statuses()

            # Business reference data
            self.generate_industries()
            self.generate_legal_forms()

            # Tax reference data (basic - full tax framework in Phase B)
            self.generate_payment_methods()

            logger.info("=" * 60)
            logger.info("Reference Data Generation Complete")
            logger.info("=" * 60)
            self._print_summary()

        except Exception as e:
            logger.error(f"Error generating reference data: {e}")
            raise

    def generate_countries(self):
        """Generate country reference data (Sri Lanka only for Phase A)."""
        logger.info("Generating countries...")

        countries = [
            {
                'iso_alpha_2': 'LK',
                'iso_alpha_3': 'LKA',
                'iso_numeric': '144',
                'common_name': 'Sri Lanka',
                'official_name': 'Democratic Socialist Republic of Sri Lanka',
                'sovereignty_status': 'independent',
                'is_tax_haven': False,
                'has_tax_treaty': True,
                'oecd_crs_participating': True,
                'valid_from': date(1948, 2, 4),  # Independence day
                'valid_to': None,
                'is_current': True,
                'notes': 'Primary country for test data',
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            }
        ]

        query = """
            INSERT INTO reference.ref_country
            (iso_alpha_2, iso_alpha_3, iso_numeric, common_name, official_name,
             sovereignty_status, is_tax_haven, has_tax_treaty, oecd_crs_participating,
             valid_from, valid_to, is_current, notes, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for country in countries:
            params = (
                country['iso_alpha_2'], country['iso_alpha_3'], country['iso_numeric'],
                country['common_name'], country['official_name'], country['sovereignty_status'],
                country['is_tax_haven'], country['has_tax_treaty'], country['oecd_crs_participating'],
                country['valid_from'], country['valid_to'], country['is_current'],
                country['notes'], country['created_by'], country['created_date']
            )
            self.db.execute_query(query, params)

        # Get generated ID
        self.generated_ids['country'] = {
            'LK': self.db.get_foreign_key_id('reference', 'ref_country', 'country_id', 'iso_alpha_2', 'LK')
        }

        logger.info(f"  Generated {len(countries)} country record(s)")

    def generate_regions(self):
        """Generate region/province reference data (9 provinces)."""
        logger.info("Generating regions (provinces)...")

        country_id = self.generated_ids['country']['LK']

        # Sri Lankan provinces
        regions = [
            {'code': 'WP', 'name': 'Western', 'type': 'Province'},
            {'code': 'CP', 'name': 'Central', 'type': 'Province'},
            {'code': 'SP', 'name': 'Southern', 'type': 'Province'},
            {'code': 'NP', 'name': 'Northern', 'type': 'Province'},
            {'code': 'EP', 'name': 'Eastern', 'type': 'Province'},
            {'code': 'NWP', 'name': 'North Western', 'type': 'Province'},
            {'code': 'NCP', 'name': 'North Central', 'type': 'Province'},
            {'code': 'SGP', 'name': 'Sabaragamuwa', 'type': 'Province'},
            {'code': 'UP', 'name': 'Uva', 'type': 'Province'}
        ]

        query = """
            INSERT INTO reference.ref_region
            (region_code, region_name, country_id, region_type,
             valid_from, valid_to, is_current, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.generated_ids['region'] = {}
        for region in regions:
            params = (
                region['code'], region['name'], country_id, region['type'],
                date.today(), None, True, DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)
            region_id = self.db.get_last_insert_id()
            self.generated_ids['region'][region['name']] = region_id

        logger.info(f"  Generated {len(regions)} region(s)")

    def generate_districts(self):
        """Generate district reference data (top 10 districts)."""
        logger.info("Generating districts...")

        # Map districts to regions
        districts_data = [
            {'code': 'CMB', 'name': 'Colombo', 'region': 'Western'},
            {'code': 'GAM', 'name': 'Gampaha', 'region': 'Western'},
            {'code': 'KAL', 'name': 'Kalutara', 'region': 'Western'},
            {'code': 'KDY', 'name': 'Kandy', 'region': 'Central'},
            {'code': 'GAL', 'name': 'Galle', 'region': 'Southern'},
            {'code': 'MAT', 'name': 'Matara', 'region': 'Southern'},
            {'code': 'JAF', 'name': 'Jaffna', 'region': 'Northern'},
            {'code': 'BAT', 'name': 'Batticaloa', 'region': 'Eastern'},
            {'code': 'KUR', 'name': 'Kurunegala', 'region': 'North Western'},
            {'code': 'ANU', 'name': 'Anuradhapura', 'region': 'North Central'}
        ]

        query = """
            INSERT INTO reference.ref_district
            (district_code, district_name, region_id,
             valid_from, valid_to, is_current, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.generated_ids['district'] = {}
        for district in districts_data:
            region_id = self.generated_ids['region'][district['region']]
            params = (
                district['code'], district['name'], region_id,
                date.today(), None, True, DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)
            district_id = self.db.get_last_insert_id()
            self.generated_ids['district'][district['name']] = district_id

        logger.info(f"  Generated {len(districts_data)} district(s)")

    def generate_localities(self):
        """Generate locality/city reference data (20 major cities)."""
        logger.info("Generating localities...")

        # Map localities to districts
        localities_data = [
            # Colombo district
            {'code': 'COL01', 'name': 'Colombo 1', 'district': 'Colombo', 'type': 'city', 'postal': '00100'},
            {'code': 'COL03', 'name': 'Colombo 3', 'district': 'Colombo', 'type': 'city', 'postal': '00300'},
            {'code': 'COL07', 'name': 'Colombo 7', 'district': 'Colombo', 'type': 'city', 'postal': '00700'},
            {'code': 'DEHI', 'name': 'Dehiwala', 'district': 'Colombo', 'type': 'town', 'postal': '10350'},
            # Gampaha district
            {'code': 'GAMP', 'name': 'Gampaha', 'district': 'Gampaha', 'type': 'city', 'postal': '11000'},
            {'code': 'NEGO', 'name': 'Negombo', 'district': 'Gampaha', 'type': 'city', 'postal': '11500'},
            # Kandy district
            {'code': 'KAND', 'name': 'Kandy', 'district': 'Kandy', 'type': 'city', 'postal': '20000'},
            {'code': 'PERA', 'name': 'Peradeniya', 'district': 'Kandy', 'type': 'town', 'postal': '20400'},
            # Galle district
            {'code': 'GALL', 'name': 'Galle', 'district': 'Galle', 'type': 'city', 'postal': '80000'},
            {'code': 'HIKK', 'name': 'Hikkaduwa', 'district': 'Galle', 'type': 'town', 'postal': '80240'},
            # Matara district
            {'code': 'MATA', 'name': 'Matara', 'district': 'Matara', 'type': 'city', 'postal': '81000'},
            {'code': 'WELI', 'name': 'Weligama', 'district': 'Matara', 'type': 'town', 'postal': '81700'},
            # Jaffna district
            {'code': 'JAFF', 'name': 'Jaffna', 'district': 'Jaffna', 'type': 'city', 'postal': '40000'},
            # Batticaloa district
            {'code': 'BATT', 'name': 'Batticaloa', 'district': 'Batticaloa', 'type': 'city', 'postal': '30000'},
            # Kurunegala district
            {'code': 'KURU', 'name': 'Kurunegala', 'district': 'Kurunegala', 'type': 'city', 'postal': '60000'},
            # Anuradhapura district
            {'code': 'ANUR', 'name': 'Anuradhapura', 'district': 'Anuradhapura', 'type': 'city', 'postal': '50000'},
            # Additional localities
            {'code': 'NUWA', 'name': 'Nuwara Eliya', 'district': 'Kandy', 'type': 'town', 'postal': '22200'},
            {'code': 'RATN', 'name': 'Ratnapura', 'district': 'Colombo', 'type': 'city', 'postal': '70000'},
            {'code': 'KURU', 'name': 'Kurunegala', 'district': 'Kurunegala', 'type': 'city', 'postal': '60000'},
            {'code': 'BADU', 'name': 'Badulla', 'district': 'Colombo', 'type': 'city', 'postal': '90000'}
        ]

        query = """
            INSERT INTO reference.ref_locality
            (locality_code, locality_name, district_id, locality_type, postal_code,
             valid_from, valid_to, is_current, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.generated_ids['locality'] = {}
        for locality in localities_data:
            district_id = self.generated_ids['district'].get(locality['district'])
            if not district_id:
                continue  # Skip if district not found

            params = (
                locality['code'], locality['name'], district_id,
                locality['type'], locality['postal'],
                date.today(), None, True, DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)
            locality_id = self.db.get_last_insert_id()
            self.generated_ids['locality'][locality['name']] = locality_id

        logger.info(f"  Generated {len(self.generated_ids['locality'])} localit(ies)")

    def generate_currencies(self):
        """Generate currency reference data (LKR only)."""
        logger.info("Generating currencies...")

        query = """
            INSERT INTO reference.ref_currency
            (iso_code, iso_numeric, currency_name, currency_symbol, minor_unit, is_active,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            CURRENCY['iso_code'], CURRENCY['iso_numeric'], CURRENCY['name'],
            CURRENCY['symbol'], CURRENCY['minor_unit'], True,
            DEFAULT_USER, datetime.now()
        )
        self.db.execute_query(query, params)

        logger.info("  Generated 1 currency record")

    def generate_genders(self):
        """Generate gender reference data."""
        logger.info("Generating genders...")

        genders = [
            {'code': 'M', 'name': 'Male', 'order': 1},
            {'code': 'F', 'name': 'Female', 'order': 2},
            {'code': 'X', 'name': 'Non-binary', 'order': 3},
            {'code': 'U', 'name': 'Prefer not to say', 'order': 4}
        ]

        query = """
            INSERT INTO reference.ref_gender
            (gender_code, gender_name, display_order, is_active,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        for gender in genders:
            params = (
                gender['code'], gender['name'], gender['order'], True,
                DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)

        logger.info(f"  Generated {len(genders)} gender(s)")

    def generate_marital_statuses(self):
        """Generate marital status reference data."""
        logger.info("Generating marital statuses...")

        statuses = [
            {'code': 'SINGLE', 'name': 'Single', 'affects_tax': False, 'joint': False, 'order': 1},
            {'code': 'MARRIED', 'name': 'Married', 'affects_tax': True, 'joint': True, 'order': 2},
            {'code': 'DIVORCED', 'name': 'Divorced', 'affects_tax': False, 'joint': False, 'order': 3},
            {'code': 'WIDOWED', 'name': 'Widowed', 'affects_tax': True, 'joint': False, 'order': 4},
            {'code': 'SEPARATED', 'name': 'Separated', 'affects_tax': False, 'joint': False, 'order': 5}
        ]

        query = """
            INSERT INTO reference.ref_marital_status
            (marital_status_code, marital_status_name, affects_tax_calculation,
             allows_joint_filing, display_order, is_active,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for status in statuses:
            params = (
                status['code'], status['name'], status['affects_tax'],
                status['joint'], status['order'], True,
                DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)

        logger.info(f"  Generated {len(statuses)} marital status(es)")

    def generate_industries(self):
        """Generate industry reference data (ISIC codes)."""
        logger.info("Generating industries...")

        query = """
            INSERT INTO reference.ref_industry
            (industry_code, industry_name, parent_industry_id,
             classification_level, isic_section, risk_level,
             requires_special_license, is_active,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for industry in INDUSTRY_CODES:
            params = (
                industry['code'], industry['name'], None,
                3, industry['section'], 'medium',  # Level 3 = Group
                False, True,
                DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)

        logger.info(f"  Generated {len(INDUSTRY_CODES)} industr(ies)")

    def generate_legal_forms(self):
        """Generate legal form reference data."""
        logger.info("Generating legal forms...")

        legal_forms = [
            {'code': 'SOLE', 'name': 'Sole Proprietorship', 'category': 'individual',
             'limited': False, 'separate': False, 'transparent': True, 'order': 1},
            {'code': 'PARTNER', 'name': 'Partnership', 'category': 'partnership',
             'limited': False, 'separate': False, 'transparent': True, 'order': 2},
            {'code': 'LTD', 'name': 'Private Limited Company', 'category': 'corporation',
             'limited': True, 'separate': True, 'transparent': False, 'order': 3},
            {'code': 'PLC', 'name': 'Public Limited Company', 'category': 'corporation',
             'limited': True, 'separate': True, 'transparent': False, 'order': 4},
            {'code': 'NGO', 'name': 'Non-Governmental Organization', 'category': 'non-profit',
             'limited': True, 'separate': True, 'transparent': False, 'order': 5}
        ]

        query = """
            INSERT INTO reference.ref_legal_form
            (legal_form_code, legal_form_name, legal_form_category,
             limited_liability, separate_legal_entity, tax_transparency,
             requires_registration, display_order, is_active,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for form in legal_forms:
            params = (
                form['code'], form['name'], form['category'],
                form['limited'], form['separate'], form['transparent'],
                True, form['order'], True,
                DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)

        logger.info(f"  Generated {len(legal_forms)} legal form(s)")

    def generate_payment_methods(self):
        """Generate payment method reference data."""
        logger.info("Generating payment methods...")

        # Enhanced payment methods with full schema fields
        payment_methods = [
            {'code': 'BANK_TRANSFER', 'name': 'Bank Transfer', 'category': 'electronic',
             'electronic': True, 'requires_bank': True, 'instant': False, 'clearing': 1,
             'has_fee': False, 'max': None, 'order': 1},
            {'code': 'ONLINE', 'name': 'Online Payment', 'category': 'electronic',
             'electronic': True, 'requires_bank': False, 'instant': True, 'clearing': 0,
             'has_fee': True, 'max': 1000000, 'order': 2},
            {'code': 'CHECK', 'name': 'Check/Cheque', 'category': 'check',
             'electronic': False, 'requires_bank': True, 'instant': False, 'clearing': 3,
             'has_fee': False, 'max': None, 'order': 3},
            {'code': 'CASH', 'name': 'Cash Payment', 'category': 'cash',
             'electronic': False, 'requires_bank': False, 'instant': True, 'clearing': 0,
             'has_fee': False, 'max': 500000, 'order': 4},
            {'code': 'CARD', 'name': 'Credit/Debit Card', 'category': 'card',
             'electronic': True, 'requires_bank': False, 'instant': True, 'clearing': 0,
             'has_fee': True, 'max': 2000000, 'order': 5}
        ]

        query = """
            INSERT INTO reference.ref_payment_method
            (payment_method_code, payment_method_name, payment_category,
             is_electronic, requires_bank_account, is_instant, clearing_days,
             has_transaction_fee, max_amount, is_active, display_order,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for method in payment_methods:
            params = (
                method['code'], method['name'], method['category'],
                method['electronic'], method['requires_bank'], method['instant'],
                method['clearing'], method['has_fee'], method['max'],
                True, method['order'],
                DEFAULT_USER, datetime.now()
            )
            self.db.execute_query(query, params)

        logger.info(f"  Generated {len(payment_methods)} payment method(s)")

    def _print_summary(self):
        """Print summary of generated reference data."""
        logger.info("")
        logger.info("Summary of Generated Reference Data:")
        logger.info("  Countries: 1")
        logger.info(f"  Regions: {len(self.generated_ids.get('region', {}))}")
        logger.info(f"  Districts: {len(self.generated_ids.get('district', {}))}")
        logger.info(f"  Localities: {len(self.generated_ids.get('locality', {}))}")
        logger.info("  Currencies: 1")
        logger.info("  Genders: 4")
        logger.info("  Marital Statuses: 5")
        logger.info(f"  Industries: {len(INDUSTRY_CODES)}")
        logger.info("  Legal Forms: 5")
        logger.info("  Payment Methods: 5")
        total = (1 + len(self.generated_ids.get('region', {})) +
                len(self.generated_ids.get('district', {})) +
                len(self.generated_ids.get('locality', {})) + 1 +
                4 + 5 + len(INDUSTRY_CODES) + 5 + 5)
        logger.info(f"  TOTAL: ~{total} records")
