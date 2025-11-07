"""
Tax Framework Generator for TA-RDM (Phase B).

Generates tax framework data including tax types, tax periods, and tax rates.
"""

import logging
import random
from datetime import date, datetime, timedelta
from typing import Dict, List

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)


class TaxFrameworkGenerator:
    """
    Generates tax framework data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize tax framework generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_ids: Dict[str, int] = {}  # Track generated tax_type_ids
        self.registration_count = 0  # Track generated registrations

    def generate_all(self, include_registrations: bool = False):
        """
        Generate all tax framework data in dependency order.

        Args:
            include_registrations: If True, also generate tax account registrations
        """
        logger.info("=" * 60)
        logger.info("Starting Tax Framework Generation (Phase B)")
        logger.info("=" * 60)

        try:
            # Generate tax types
            self.generate_tax_types()

            # Generate registrations if requested
            if include_registrations:
                self.generate_tax_account_registrations()

            logger.info("=" * 60)
            logger.info("Tax Framework Generation Complete")
            logger.info("=" * 60)
            self._print_summary()

        except Exception as e:
            logger.error(f"Error generating tax framework data: {e}")
            raise

    def generate_tax_types(self):
        """Generate 5 core tax types for Sri Lankan tax system."""
        logger.info("Generating tax types...")

        # Sri Lankan tax types for Phase B
        # Based on plan: VAT, CIT, PIT, WHT, ESL
        tax_types = [
            {
                'code': 'VAT',
                'name': 'Value Added Tax',
                'description': 'Tax on supply of goods and services',
                'category': 'CONSUMPTION',
                'legal_basis': 'Value Added Tax Act No. 14 of 2002',
                'authority': 'Inland Revenue Department',
                'is_federal': True,
                'is_withholding': False,
                'requires_registration': True,
                'requires_filing': True,
                'filing_frequency': 'MONTHLY',
                'self_assessment': True,
                'installments': False,
                'applies_individuals': False,
                'applies_enterprises': True,
                'penalty_rate': 2.00,
                'interest_rate': 1.00,
                'valid_from': date(2002, 8, 1),
                'sort_order': 1
            },
            {
                'code': 'CIT',
                'name': 'Corporate Income Tax',
                'description': 'Tax on corporate profits and income',
                'category': 'INCOME',
                'legal_basis': 'Inland Revenue Act No. 24 of 2017',
                'authority': 'Inland Revenue Department',
                'is_federal': True,
                'is_withholding': False,
                'requires_registration': True,
                'requires_filing': True,
                'filing_frequency': 'ANNUAL',
                'self_assessment': True,
                'installments': True,
                'applies_individuals': False,
                'applies_enterprises': True,
                'penalty_rate': 2.00,
                'interest_rate': 1.00,
                'valid_from': date(2018, 4, 1),
                'sort_order': 2
            },
            {
                'code': 'PIT',
                'name': 'Personal Income Tax',
                'description': 'Tax on individual income (salaries, business, investments)',
                'category': 'INCOME',
                'legal_basis': 'Inland Revenue Act No. 24 of 2017',
                'authority': 'Inland Revenue Department',
                'is_federal': True,
                'is_withholding': False,
                'requires_registration': True,
                'requires_filing': True,
                'filing_frequency': 'ANNUAL',
                'self_assessment': True,
                'installments': True,
                'applies_individuals': True,
                'applies_enterprises': False,
                'penalty_rate': 2.00,
                'interest_rate': 1.00,
                'valid_from': date(2018, 4, 1),
                'sort_order': 3
            },
            {
                'code': 'WHT',
                'name': 'Withholding Tax',
                'description': 'Tax withheld at source on payments',
                'category': 'INCOME',
                'legal_basis': 'Inland Revenue Act No. 24 of 2017',
                'authority': 'Inland Revenue Department',
                'is_federal': True,
                'is_withholding': True,
                'requires_registration': True,
                'requires_filing': True,
                'filing_frequency': 'MONTHLY',
                'self_assessment': True,
                'installments': False,
                'applies_individuals': True,
                'applies_enterprises': True,
                'penalty_rate': 2.00,
                'interest_rate': 1.00,
                'valid_from': date(2018, 4, 1),
                'sort_order': 4
            },
            {
                'code': 'ESL',
                'name': 'Economic Service Levy',
                'description': 'Levy on turnover for specific economic activities',
                'category': 'OTHER',
                'legal_basis': 'Finance Act No. 11 of 2004',
                'authority': 'Inland Revenue Department',
                'is_federal': True,
                'is_withholding': False,
                'requires_registration': True,
                'requires_filing': True,
                'filing_frequency': 'QUARTERLY',
                'self_assessment': True,
                'installments': False,
                'applies_individuals': False,
                'applies_enterprises': True,
                'penalty_rate': 2.00,
                'interest_rate': 1.00,
                'valid_from': date(2004, 1, 1),
                'sort_order': 5
            }
        ]

        query = """
            INSERT INTO tax_framework.tax_type
            (tax_type_code, tax_type_name, description, tax_category,
             legal_basis, administering_authority, is_federal, is_withholding_tax,
             requires_registration, requires_periodic_filing, default_filing_frequency,
             is_self_assessment, supports_installments,
             applies_to_individuals, applies_to_enterprises,
             penalty_rate_default, interest_rate_default,
             valid_from, valid_to, is_current, is_active,
             sort_order, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for tax_type in tax_types:
            params = (
                tax_type['code'],
                tax_type['name'],
                tax_type['description'],
                tax_type['category'],
                tax_type['legal_basis'],
                tax_type['authority'],
                tax_type['is_federal'],
                tax_type['is_withholding'],
                tax_type['requires_registration'],
                tax_type['requires_filing'],
                tax_type['filing_frequency'],
                tax_type['self_assessment'],
                tax_type['installments'],
                tax_type['applies_individuals'],
                tax_type['applies_enterprises'],
                tax_type['penalty_rate'],
                tax_type['interest_rate'],
                tax_type['valid_from'],
                None,  # valid_to (NULL = current)
                True,  # is_current
                True,  # is_active
                tax_type['sort_order'],
                DEFAULT_USER,
                datetime.now()
            )
            self.db.execute_query(query, params)
            tax_type_id = self.db.get_last_insert_id()
            self.generated_ids[tax_type['code']] = tax_type_id
            logger.info(f"  Generated tax type: {tax_type['code']} - {tax_type['name']} (ID: {tax_type_id})")

        logger.info(f"  Generated {len(tax_types)} tax type(s)")

    def generate_tax_account_registrations(self):
        """
        Generate 10 tax account registrations for the 5 parties.
        Each party registers for 2 tax types based on their party type.
        """
        logger.info("Generating tax account registrations...")

        # Get parties
        parties = self.db.fetch_all("""
            SELECT party_id, party_type_code, party_name,
                   tax_identification_number
            FROM party.party
            ORDER BY party_id
        """)

        if not parties:
            logger.warning("  No parties found - skipping registration generation")
            return

        # Registration mapping based on party type
        # Individuals: PIT + WHT
        # Enterprises: CIT + (VAT or ESL)
        registrations = []

        for party_id, party_type, party_name, tin in parties:
            # Determine tax types based on party type
            if party_type == 'INDIVIDUAL':
                tax_types = ['PIT', 'WHT']
            else:  # ENTERPRISE
                # First enterprise gets CIT + VAT, second gets CIT + ESL
                if party_id % 2 == 0:
                    tax_types = ['CIT', 'VAT']
                else:
                    tax_types = ['CIT', 'ESL']

            # Generate registration for each tax type
            for tax_type_code in tax_types:
                # Registration date: between 6 months and 2 years ago
                days_ago = random.randint(180, 730)
                reg_date = date.today() - timedelta(days=days_ago)

                # Generate unique tax account number: TAX_TYPE-TIN
                account_number = f"{tax_type_code}-{tin}"

                # Get filing frequency from tax type
                filing_freq_row = self.db.fetch_one("""
                    SELECT default_filing_frequency
                    FROM tax_framework.tax_type
                    WHERE tax_type_code = %s
                """, (tax_type_code,))
                filing_frequency = filing_freq_row[0] if filing_freq_row else 'ANNUAL'

                registrations.append({
                    'party_id': party_id,
                    'party_name': party_name,
                    'tax_type_code': tax_type_code,
                    'account_number': account_number,
                    'status': 'ACTIVE',
                    'registration_date': reg_date,
                    'registration_method': 'ONLINE',
                    'registration_reason': f'Mandatory registration for {tax_type_code}',
                    'effective_from': reg_date,
                    'segmentation': 'SMALL',
                    'office_code': 'OFFICE_01',
                    'filing_frequency': filing_frequency,
                    'payment_due_day': 15,
                    'is_group': False,
                    'is_exporter': False
                })

        # Insert registrations
        query = """
            INSERT INTO registration.tax_account
            (party_id, tax_type_code, tax_account_number, account_status_code,
             registration_date, registration_method_code, registration_reason,
             effective_from_date, effective_to_date, segmentation_code,
             assigned_office_code, filing_frequency_code, payment_due_day,
             is_group_filing, is_exporter, valid_from, valid_to, is_current,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for reg in registrations:
            params = (
                reg['party_id'],
                reg['tax_type_code'],
                reg['account_number'],
                reg['status'],
                reg['registration_date'],
                reg['registration_method'],
                reg['registration_reason'],
                reg['effective_from'],
                None,  # effective_to_date (NULL = still active)
                reg['segmentation'],
                reg['office_code'],
                reg['filing_frequency'],
                reg['payment_due_day'],
                reg['is_group'],
                reg['is_exporter'],
                date.today(),  # valid_from
                None,  # valid_to (NULL = current)
                True,  # is_current
                DEFAULT_USER,
                datetime.now()
            )
            self.db.execute_query(query, params)
            account_id = self.db.get_last_insert_id()
            self.registration_count += 1
            logger.info(f"  Registered: {reg['party_name'][:30]:<30} for {reg['tax_type_code']:<5} "
                       f"(Account: {reg['account_number']}, ID: {account_id})")

        logger.info(f"  Generated {len(registrations)} tax account registration(s)")

    def generate_tax_periods(self, start_year: int = 2023, end_year: int = 2025):
        """
        Generate tax periods for all tax types based on their filing frequency.

        Args:
            start_year: Starting year for period generation (default: 2023)
            end_year: Ending year for period generation (default: 2025)
        """
        logger.info(f"Generating tax periods ({start_year}-{end_year})...")

        # Get all active tax types
        tax_types = self.db.fetch_all("""
            SELECT tax_type_id, tax_type_code, tax_type_name, default_filing_frequency
            FROM tax_framework.tax_type
            WHERE is_active = TRUE
            ORDER BY tax_type_id
        """)

        if not tax_types:
            logger.warning("  No tax types found - skipping period generation")
            return

        total_periods = 0

        for tax_type_id, tax_code, tax_name, filing_frequency in tax_types:
            logger.info(f"  Generating periods for {tax_code} ({filing_frequency})...")

            periods = []

            for year in range(start_year, end_year + 1):
                if filing_frequency == 'MONTHLY':
                    # Generate 12 monthly periods
                    for month in range(1, 13):
                        period_start = date(year, month, 1)

                        # Calculate period end date (last day of month)
                        if month == 12:
                            period_end = date(year, 12, 31)
                        else:
                            period_end = date(year, month + 1, 1) - timedelta(days=1)

                        # Filing due date: 15th of next month
                        if month == 12:
                            filing_due = date(year + 1, 1, 15)
                        else:
                            filing_due = date(year, month + 1, 15)

                        # Payment due date: same as filing due date
                        payment_due = filing_due

                        period_name = f"{tax_code} {year}-{month:02d}"
                        period_code = f"{tax_code}-{year}{month:02d}"

                        periods.append({
                            'tax_type_id': tax_type_id,
                            'period_year': year,
                            'period_month': month,
                            'period_number': month,
                            'period_name': period_name,
                            'period_code': period_code,
                            'period_start_date': period_start,
                            'period_end_date': period_end,
                            'filing_due_date': filing_due,
                            'payment_due_date': payment_due,
                            'period_status': 'OPEN' if year == 2025 else 'CLOSED',
                            'is_year_end_period': (month == 12),
                            'grace_period_days': 5
                        })

                elif filing_frequency == 'QUARTERLY':
                    # Generate 4 quarterly periods
                    quarters = [
                        (1, 1, 3, 31, 'Q1'),   # Jan-Mar
                        (2, 4, 6, 30, 'Q2'),   # Apr-Jun
                        (3, 7, 9, 30, 'Q3'),   # Jul-Sep
                        (4, 10, 12, 31, 'Q4')  # Oct-Dec
                    ]

                    for quarter_num, start_month, end_month, end_day, quarter_name in quarters:
                        period_start = date(year, start_month, 1)
                        period_end = date(year, end_month, end_day)

                        # Filing due date: 15th of next month after quarter end
                        if end_month == 12:
                            filing_due = date(year + 1, 1, 15)
                        else:
                            filing_due = date(year, end_month + 1, 15)

                        payment_due = filing_due

                        period_name_str = f"{tax_code} {year}-{quarter_name}"
                        period_code = f"{tax_code}-{year}Q{quarter_num}"

                        periods.append({
                            'tax_type_id': tax_type_id,
                            'period_year': year,
                            'period_month': end_month,
                            'period_number': quarter_num,
                            'period_name': period_name_str,
                            'period_code': period_code,
                            'period_start_date': period_start,
                            'period_end_date': period_end,
                            'filing_due_date': filing_due,
                            'payment_due_date': payment_due,
                            'period_status': 'OPEN' if year == 2025 else 'CLOSED',
                            'is_year_end_period': (quarter_num == 4),
                            'grace_period_days': 10
                        })

                elif filing_frequency == 'ANNUAL':
                    # Generate 1 annual period per year
                    period_start = date(year, 4, 1)  # Sri Lanka tax year: Apr 1 - Mar 31
                    period_end = date(year + 1, 3, 31)

                    # Filing due date: 6 months after year end (Sep 30)
                    filing_due = date(year + 1, 9, 30)
                    payment_due = filing_due

                    period_name = f"{tax_code} FY {year}/{year+1}"
                    period_code = f"{tax_code}-{year}"

                    periods.append({
                        'tax_type_id': tax_type_id,
                        'period_year': year,
                        'period_month': None,  # Annual period, no specific month
                        'period_number': 1,
                        'period_name': period_name,
                        'period_code': period_code,
                        'period_start_date': period_start,
                        'period_end_date': period_end,
                        'filing_due_date': filing_due,
                        'payment_due_date': payment_due,
                        'period_status': 'OPEN' if year >= 2024 else 'CLOSED',
                        'is_year_end_period': True,
                        'grace_period_days': 30
                    })

            # Insert all periods for this tax type
            query = """
                INSERT INTO tax_framework.tax_period
                (tax_type_id, period_year, period_month, period_number,
                 period_name, period_code, period_start_date, period_end_date,
                 filing_due_date, payment_due_date, period_status,
                 is_year_end_period, grace_period_days, extension_deadline,
                 created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for period in periods:
                params = (
                    period['tax_type_id'],
                    period['period_year'],
                    period['period_month'],
                    period['period_number'],
                    period['period_name'],
                    period['period_code'],
                    period['period_start_date'],
                    period['period_end_date'],
                    period['filing_due_date'],
                    period['payment_due_date'],
                    period['period_status'],
                    period['is_year_end_period'],
                    period['grace_period_days'],
                    None,  # extension_deadline (can be set later if needed)
                    DEFAULT_USER,
                    datetime.now()
                )
                self.db.execute_query(query, params)
                total_periods += 1

            logger.info(f"    Generated {len(periods)} period(s) for {tax_code}")

        logger.info(f"  Generated {total_periods} total tax period(s)")
        return total_periods

    def generate_tax_forms(self):
        """Generate minimal tax forms for each tax type."""
        logger.info("Generating tax forms...")

        # Get all active tax types
        tax_types = self.db.fetch_all("""
            SELECT tax_type_id, tax_type_code, tax_type_name, default_filing_frequency
            FROM tax_framework.tax_type
            WHERE is_active = TRUE
            ORDER BY tax_type_id
        """)

        if not tax_types:
            logger.warning("  No tax types found - skipping form generation")
            return

        forms = []

        for tax_type_id, tax_code, tax_name, filing_frequency in tax_types:
            form_code = f"{tax_code}-RETURN-01"
            form_name = f"{tax_code} Tax Return Form"

            forms.append({
                'tax_type_id': tax_type_id,
                'form_code': form_code,
                'form_name': form_name,
                'description': f"Standard {tax_name} return form",
                'form_category': 'RETURN',
                'filing_frequency': filing_frequency,
                'is_mandatory': True,
                'is_electronic_filing_enabled': True,
                'is_paper_filing_enabled': True,
                'applies_to_individuals': tax_code in ['PIT', 'WHT'],
                'applies_to_enterprises': tax_code in ['CIT', 'VAT', 'WHT', 'ESL'],
                'requires_attachment': False,
                'allows_amendments': True,
                'max_amendments_allowed': 3,
                'sort_order': 1,
                'is_active': True
            })

        query = """
            INSERT INTO tax_framework.tax_form
            (tax_type_id, form_code, form_name, description,
             form_category, filing_frequency,
             is_mandatory, is_electronic_filing_enabled, is_paper_filing_enabled,
             applies_to_individuals, applies_to_enterprises,
             requires_attachment, allows_amendments, max_amendments_allowed,
             sort_order, is_active, created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for form in forms:
            params = (
                form['tax_type_id'],
                form['form_code'],
                form['form_name'],
                form['description'],
                form['form_category'],
                form['filing_frequency'],
                form['is_mandatory'],
                form['is_electronic_filing_enabled'],
                form['is_paper_filing_enabled'],
                form['applies_to_individuals'],
                form['applies_to_enterprises'],
                form['requires_attachment'],
                form['allows_amendments'],
                form['max_amendments_allowed'],
                form['sort_order'],
                form['is_active'],
                DEFAULT_USER,
                datetime.now()
            )
            self.db.execute_query(query, params)
            form_id = self.db.get_last_insert_id()
            logger.info(f"  Generated form: {form['form_code']} - {form['form_name']} (ID: {form_id})")

        logger.info(f"  Generated {len(forms)} tax form(s)")

    def _print_summary(self):
        """Print summary of generated tax framework data."""
        logger.info("")
        logger.info("Summary of Generated Tax Framework Data:")
        logger.info(f"  Tax Types: {len(self.generated_ids)}")
        for code, tax_id in self.generated_ids.items():
            logger.info(f"    {code}: ID {tax_id}")
        if self.registration_count > 0:
            logger.info(f"  Tax Account Registrations: {self.registration_count}")
