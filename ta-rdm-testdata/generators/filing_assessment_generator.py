"""
Filing & Assessment Generator for TA-RDM (Phase C).

Generates tax returns, assessments, penalties, and interest for test data.
"""

import logging
import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)


class FilingAssessmentGenerator:
    """
    Generates filing and assessment data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize filing & assessment generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'tax_returns': 0,
            'tax_return_lines': 0,
            'assessments': 0,
            'assessment_lines': 0,
            'penalties': 0,
            'interests': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all filing and assessment data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Filing & Assessment Generation (Phase C)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug in tax_return FK constraints
            # (FKs reference filing_assessment.registration.tax_account instead of registration.tax_account)
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS=0")

            # Generate returns and assessments
            self.generate_tax_returns_and_assessments()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS=1")

            logger.info("=" * 80)
            logger.info("Filing & Assessment Generation Complete")
            logger.info("=" * 80)
            self._print_summary()

        except Exception as e:
            logger.error(f"Error generating filing/assessment data: {e}")
            # Make sure to re-enable FK checks even if there's an error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS=1")
            except:
                pass
            raise

    def generate_tax_returns_and_assessments(self):
        """
        Generate tax returns with assessments for registered taxpayers.
        Generates returns for CLOSED periods (2023-2024) only.
        """
        logger.info("Generating tax returns and assessments...")

        # Get tax accounts (registrations)
        accounts = self.db.fetch_all("""
            SELECT ta.tax_account_id, ta.party_id, ta.tax_type_code,
                   ta.tax_account_number, ta.filing_frequency_code,
                   p.party_name
            FROM registration.tax_account ta
            INNER JOIN party.party p ON ta.party_id = p.party_id
            WHERE ta.account_status_code = 'ACTIVE'
            ORDER BY ta.tax_account_id
        """)

        if not accounts:
            logger.warning("  No tax accounts found - skipping filing generation")
            return

        for account_id, party_id, tax_code, account_num, filing_freq, party_name in accounts:
            # Get CLOSED periods for this tax type (2023-2024 only, not 2025)
            periods = self.db.fetch_all("""
                SELECT tp.tax_period_id, tp.period_code, tp.period_name,
                       tp.period_start_date, tp.period_end_date,
                       tp.filing_due_date, tp.payment_due_date
                FROM tax_framework.tax_period tp
                INNER JOIN tax_framework.tax_type tt ON tp.tax_type_id = tt.tax_type_id
                WHERE tt.tax_type_code = %s
                  AND tp.period_status = 'CLOSED'
                  AND tp.period_year < 2025
                ORDER BY tp.period_start_date
            """, (tax_code,))

            # Generate returns for 70% of periods (not 100% - simulate some non-compliance)
            selected_periods = random.sample(periods, int(len(periods) * 0.7))

            logger.info(f"  Generating {len(selected_periods)} returns for {party_name[:30]} ({tax_code})...")

            for period_id, period_code, period_name, period_start, period_end, filing_due, payment_due in selected_periods:
                # Generate return
                return_id = self._generate_tax_return(
                    account_id, period_id, filing_due, payment_due, tax_code
                )

                # Generate assessment
                if return_id:
                    self._generate_assessment(
                        return_id, account_id, period_id, payment_due, tax_code
                    )

        logger.info(f"  Generated {self.generated_counts['tax_returns']} tax returns")
        logger.info(f"  Generated {self.generated_counts['assessments']} assessments")

    def _generate_tax_return(self, account_id: int, period_id: int,
                            filing_due: date, payment_due: date, tax_code: str) -> int:
        """Generate a single tax return."""

        # Filing date: 50% on time, 30% late, 20% early
        days_adjustment = random.choice([
            0, 0, 0, 0, 0,  # On time (50%)
            1, 2, 3, 5, 7,  # Late (30%)
            -7, -5, -3      # Early (20%)
        ])
        filing_date = filing_due + timedelta(days=days_adjustment)

        is_late = filing_date > filing_due
        days_late = (filing_date - filing_due).days if is_late else 0

        # Return number: TAX-PERIOD-ACCOUNT
        return_number = f"RTN-{tax_code}-{period_id}-{account_id}"

        # Get the correct form_id for this tax type
        form_data = self.db.fetch_one("""
            SELECT tf.tax_form_id
            FROM tax_framework.tax_form tf
            INNER JOIN tax_framework.tax_type tt ON tf.tax_type_id = tt.tax_type_id
            WHERE tt.tax_type_code = %s
            LIMIT 1
        """, (tax_code,))

        form_id = form_data[0] if form_data else 1

        query = """
            INSERT INTO filing_assessment.tax_return
            (tax_account_id, tax_period_id, form_id, form_version_id,
             return_number, return_version, previous_return_id,
             filing_method_code, filing_date, filing_due_date,
             is_filing_late, days_late, return_status_code,
             is_amended, is_current_version, risk_score, risk_category_code,
             is_flagged_for_review, ip_address,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            account_id,
            period_id,
            form_id,  # Use actual form_id from tax_form table
            form_id,  # Use same as form_version_id for now (until we create form versions)
            return_number,
            1,  # return_version
            None,  # previous_return_id (not amended)
            random.choice(['ONLINE', 'PAPER', 'EFILE', 'AGENT']),
            filing_date,
            filing_due,
            is_late,
            days_late if is_late else None,
            'PROCESSED',
            False,  # is_amended
            True,  # is_current_version
            round(random.uniform(0, 100), 2),  # risk_score (DECIMAL(5,2))
            random.choice(['LOW', 'MEDIUM', 'HIGH']) if random.random() < 0.3 else None,
            random.random() < 0.1,  # 10% flagged for review
            f"192.168.1.{random.randint(1,254)}",
            DEFAULT_USER,
            datetime.now()
        )

        self.db.execute_query(query, params)
        return_id = self.db.get_last_insert_id()
        self.generated_counts['tax_returns'] += 1

        # Generate return lines (simplified - just 3 key lines per return)
        self._generate_tax_return_lines(return_id, tax_code)

        return return_id

    def _generate_tax_return_lines(self, return_id: int, tax_code: str):
        """Generate tax return lines for a return."""

        # Simplified line structure based on tax type
        if tax_code == 'VAT':
            lines = [
                ('L001', 1, 'Taxable Sales', 'numeric', round(random.uniform(50000, 500000), 2)),
                ('L002', 2, 'Output VAT @15%', 'numeric', None),  # Calculated
                ('L003', 3, 'Input VAT', 'numeric', round(random.uniform(5000, 50000), 2)),
                ('L004', 4, 'Net VAT Payable', 'numeric', None),  # Calculated
            ]
        elif tax_code in ['CIT', 'PIT']:
            lines = [
                ('L001', 1, 'Gross Income', 'numeric', round(random.uniform(100000, 1000000), 2)),
                ('L002', 2, 'Allowable Deductions', 'numeric', round(random.uniform(20000, 200000), 2)),
                ('L003', 3, 'Taxable Income', 'numeric', None),  # Calculated
                ('L004', 4, 'Tax Liability', 'numeric', None),  # Calculated
            ]
        elif tax_code == 'WHT':
            lines = [
                ('L001', 1, 'Total Payments Subject to WHT', 'numeric', round(random.uniform(20000, 200000), 2)),
                ('L002', 2, 'Total WHT Withheld', 'numeric', None),  # Calculated
            ]
        elif tax_code == 'ESL':
            lines = [
                ('L001', 1, 'Gross Turnover', 'numeric', round(random.uniform(100000, 1000000), 2)),
                ('L002', 2, 'ESL @1.5%', 'numeric', None),  # Calculated
            ]
        else:
            lines = []

        query = """
            INSERT INTO filing_assessment.tax_return_line
            (tax_return_id, form_line_id, line_number, line_sequence,
             line_value_numeric, is_calculated, is_overridden, validation_status_code,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for line_num, seq, desc, val_type, value in lines:
            params = (
                return_id,
                seq,  # form_line_id (placeholder)
                line_num,
                seq,
                value if value is not None else 0.00,  # Placeholder for calculated
                value is None,  # is_calculated
                False,  # is_overridden
                'VALID',
                DEFAULT_USER,
                datetime.now()
            )
            self.db.execute_query(query, params)
            self.generated_counts['tax_return_lines'] += 1

    def _generate_assessment(self, return_id: int, account_id: int,
                            period_id: int, payment_due: date, tax_code: str):
        """Generate assessment for a tax return."""

        # Get return details
        return_data = self.db.fetch_one("""
            SELECT filing_date, is_filing_late, days_late
            FROM filing_assessment.tax_return
            WHERE tax_return_id = %s
        """, (return_id,))

        filing_date, is_late, days_late = return_data

        # Calculate tax amount based on tax type
        if tax_code == 'VAT':
            assessed_tax = self.round_decimal(Decimal(str(random.uniform(1000, 50000))))
        elif tax_code in ['CIT', 'PIT']:
            assessed_tax = self.round_decimal(Decimal(str(random.uniform(10000, 200000))))
        elif tax_code == 'WHT':
            assessed_tax = self.round_decimal(Decimal(str(random.uniform(500, 10000))))
        elif tax_code == 'ESL':
            assessed_tax = self.round_decimal(Decimal(str(random.uniform(1000, 15000))))
        else:
            assessed_tax = Decimal('1000.00')

        # Calculate penalty (2% per month for late filing)
        penalty_amount = Decimal(0)
        if is_late and days_late:
            months_late = (days_late // 30) + 1
            penalty_amount = self.round_decimal(assessed_tax * Decimal('0.02') * months_late)
            max_penalty = self.round_decimal(assessed_tax * Decimal('0.50'))  # Max 50%
            penalty_amount = min(penalty_amount, max_penalty)

        # Calculate interest (1% per month)
        interest_amount = Decimal(0)
        if assessed_tax > 0:
            months_overdue = random.randint(0, 3)  # 0-3 months overdue
            if months_overdue > 0:
                interest_amount = self.round_decimal(assessed_tax * Decimal('0.01') * months_overdue)

        net_amount = self.round_decimal(assessed_tax + penalty_amount + interest_amount)

        # 80% paid, 20% unpaid
        if random.random() < 0.8:
            amount_paid = net_amount
            balance = Decimal(0)
        else:
            amount_paid = self.round_decimal(net_amount * Decimal(str(random.uniform(0, 0.5))))
            balance = self.round_decimal(net_amount - amount_paid)

        assessment_number = f"ASS-{tax_code}-{period_id}-{account_id}"
        assessment_date = filing_date + timedelta(days=random.randint(1, 30))

        query = """
            INSERT INTO filing_assessment.assessment
            (tax_account_id, tax_period_id, tax_return_id,
             assessment_number, assessment_type_code, assessment_version,
             is_current_version, assessment_status_code,
             assessment_date, finalization_date, payment_due_date,
             assessed_tax_amount, penalty_amount, interest_amount,
             credit_amount, net_assessment_amount,
             amount_paid, balance_outstanding,
             has_objection, is_refund_due,
             posted_to_accounting, assessed_by_user_id,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            account_id,
            period_id,
            return_id,
            assessment_number,
            'SELF_ASSESSMENT',
            1,  # version
            True,  # is_current
            'FINAL' if balance == 0 else 'ASSESSED',
            assessment_date,
            assessment_date,
            payment_due,
            assessed_tax,
            penalty_amount,
            interest_amount,
            Decimal(0),  # credit_amount
            net_amount,
            amount_paid,
            balance,
            False,  # has_objection
            False,  # is_refund_due
            True,  # posted_to_accounting
            'SYSTEM_AUTO_ASSESS',
            DEFAULT_USER,
            datetime.now()
        )

        self.db.execute_query(query, params)
        assessment_id = self.db.get_last_insert_id()
        self.generated_counts['assessments'] += 1

        # Generate assessment lines
        self._generate_assessment_lines(assessment_id, assessed_tax)

        # Generate penalty record if penalty exists
        if penalty_amount > 0:
            self._generate_penalty(assessment_id, penalty_amount, days_late, assessment_date, payment_due)

        # Generate interest record if interest exists
        if interest_amount > 0:
            self._generate_interest(assessment_id, interest_amount, assessed_tax, assessment_date, payment_due)

    def _generate_assessment_lines(self, assessment_id: int, assessed_tax: Decimal):
        """Generate assessment lines."""

        lines = [
            ('L001', 1, 'Tax Assessed', assessed_tax, assessed_tax, Decimal(0)),
        ]

        query = """
            INSERT INTO filing_assessment.assessment_line
            (assessment_id, form_line_id, line_number, line_sequence,
             line_description, submitted_amount, assessed_amount, adjustment_amount,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for line_num, seq, desc, submitted, assessed, adjustment in lines:
            params = (
                assessment_id,
                seq,  # form_line_id
                line_num,
                seq,
                desc,
                submitted,
                assessed,
                adjustment,
                DEFAULT_USER,
                datetime.now()
            )
            self.db.execute_query(query, params)
            self.generated_counts['assessment_lines'] += 1

    def _generate_penalty(self, assessment_id: int, penalty_amount: Decimal,
                         days_late: int, assessment_date: date, payment_due: date):
        """Generate penalty assessment."""

        query = """
            INSERT INTO filing_assessment.penalty_assessment
            (assessment_id, penalty_type_code, penalty_calculation_method_code,
             base_amount, penalty_rate, days_late,
             calculated_penalty_amount, final_penalty_amount,
             penalty_status_code, is_automatic,
             waiver_requested, assessment_date, payment_due_date,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            assessment_id,
            'LATE_FILING',
            'PERCENTAGE_BASED',
            self.round_decimal(penalty_amount / Decimal('0.02')),  # base amount
            Decimal('2.00'),  # 2% rate
            days_late,
            penalty_amount,
            penalty_amount,
            'ASSESSED',
            True,  # is_automatic
            False,  # waiver_requested
            assessment_date,
            payment_due,
            DEFAULT_USER,
            datetime.now()
        )

        self.db.execute_query(query, params)
        self.generated_counts['penalties'] += 1

    def _generate_interest(self, assessment_id: int, interest_amount: Decimal,
                          principal: Decimal, assessment_date: date, payment_due: date):
        """Generate interest assessment."""

        days_overdue = random.randint(30, 90)
        period_start = payment_due + timedelta(days=1)
        period_end = payment_due + timedelta(days=days_overdue)

        query = """
            INSERT INTO filing_assessment.interest_assessment
            (assessment_id, interest_period_start_date, interest_period_end_date,
             days_overdue, principal_amount, annual_interest_rate, daily_interest_rate,
             calculated_interest_amount, final_interest_amount,
             interest_status_code, is_compounded,
             waiver_requested, calculation_date, payment_due_date,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            assessment_id,
            period_start,
            period_end,
            days_overdue,
            principal,
            Decimal('12.00'),  # 12% annual rate
            self.round_decimal(Decimal('12.00') / Decimal('365'), places=6),  # daily rate
            interest_amount,
            interest_amount,
            'ASSESSED',
            False,  # not compounded
            False,  # no waiver requested
            assessment_date,
            payment_due,
            DEFAULT_USER,
            datetime.now()
        )

        self.db.execute_query(query, params)
        self.generated_counts['interests'] += 1

    def _print_summary(self):
        """Print summary of generated filing/assessment data."""
        logger.info("")
        logger.info("Summary of Generated Filing & Assessment Data:")
        for key, count in self.generated_counts.items():
            logger.info(f"  {key.replace('_', ' ').title()}: {count}")
