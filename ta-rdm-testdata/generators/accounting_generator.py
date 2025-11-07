"""
Accounting Generator for TA-RDM (Phase D).

Generates tax transactions and account balances for test data.
Builds on Phase C assessments and Phase D payments to create full accounting picture.

SIMPLIFIED for Phase D: Generates one transaction per assessment component and payment allocation.
"""

import logging
import random
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)


class AccountingGenerator:
    """
    Generates accounting data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize accounting generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'transactions': 0,
            'account_balances': 0,
            'fiscal_years': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all accounting data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Accounting Generation (Phase D)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            self.db.commit()

            # Step 1: Generate fiscal years
            self._generate_fiscal_years()

            # Step 2: Query existing assessments and payments with allocations
            assessment_components = self._query_assessment_components()
            payment_allocations = self._query_payment_allocations()
            logger.info(f"Found {len(assessment_components)} assessment components and {len(payment_allocations)} payment allocations")

            # Step 3: Generate transactions for assessment components (debits)
            self._generate_assessment_transactions(assessment_components)

            # Step 4: Generate transactions for payment allocations (credits)
            self._generate_payment_transactions(payment_allocations)

            # Step 5: Calculate and generate account balances
            self._generate_account_balances()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            self.db.commit()

            # Step 6: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Accounting Generation Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Accounting generation failed: {e}", exc_info=True)
            # Try to re-enable FK checks even on error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
            except:
                pass
            raise

    def _generate_fiscal_years(self):
        """Generate fiscal year master data."""
        logger.info("Generating fiscal years...")

        # Check if fiscal years already exist
        existing = self.db.fetch_one("SELECT COUNT(*) FROM accounting.fiscal_year")
        if existing and existing[0] > 0:
            logger.info(f"  Fiscal years already exist ({existing[0]} records), skipping generation")
            return

        fiscal_years = [
            {
                'fiscal_year': 2023,
                'start_date': '2023-01-01',
                'end_date': '2023-12-31',
                'fiscal_year_status': 'CLOSED',
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            },
            {
                'fiscal_year': 2024,
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'fiscal_year_status': 'OPEN',
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            },
            {
                'fiscal_year': 2025,
                'start_date': '2025-01-01',
                'end_date': '2025-12-31',
                'fiscal_year_status': 'PLANNED',
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            }
        ]

        for fy in fiscal_years:
            self.db.execute_query("""
                INSERT INTO accounting.fiscal_year
                (fiscal_year, start_date, end_date, fiscal_year_status,
                 created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                fy['fiscal_year'], fy['start_date'], fy['end_date'],
                fy['fiscal_year_status'], fy['created_by'], fy['created_date']
            ))
            self.generated_counts['fiscal_years'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['fiscal_years']} fiscal years")

    def _query_assessment_components(self) -> List[Dict]:
        """
        Query assessment components (principal, penalty, interest) from Phase C.
        Each non-zero component becomes a separate transaction.
        """
        query = """
            SELECT
                a.assessment_id,
                a.tax_account_id,
                a.tax_period_id,
                a.assessment_date,
                a.assessed_tax_amount,
                a.penalty_amount,
                a.interest_amount,
                YEAR(a.assessment_date) as fiscal_year
            FROM filing_assessment.assessment a
            WHERE a.is_current_version = 1
            AND a.net_assessment_amount > 0
            ORDER BY a.assessment_date
        """

        rows = self.db.fetch_all(query)
        components = []

        for row in rows:
            assessment_id = row[0]
            tax_account_id = row[1]
            tax_period_id = row[2]
            assessment_date = row[3]
            fiscal_year = row[7]

            # Create separate component for principal tax
            if row[4] > 0:
                components.append({
                    'assessment_id': assessment_id,
                    'tax_account_id': tax_account_id,
                    'tax_period_id': tax_period_id,
                    'transaction_date': assessment_date,
                    'charge_type_code': 'PRINCIPAL_TAX',
                    'amount': row[4],
                    'fiscal_year': fiscal_year
                })

            # Create separate component for penalty
            if row[5] > 0:
                components.append({
                    'assessment_id': assessment_id,
                    'tax_account_id': tax_account_id,
                    'tax_period_id': tax_period_id,
                    'transaction_date': assessment_date,
                    'charge_type_code': 'PENALTY',
                    'amount': row[5],
                    'fiscal_year': fiscal_year
                })

            # Create separate component for interest
            if row[6] > 0:
                components.append({
                    'assessment_id': assessment_id,
                    'tax_account_id': tax_account_id,
                    'tax_period_id': tax_period_id,
                    'transaction_date': assessment_date,
                    'charge_type_code': 'INTEREST',
                    'amount': row[6],
                    'fiscal_year': fiscal_year
                })

        return components

    def _query_payment_allocations(self) -> List[Dict]:
        """Query payment allocations from Phase D."""
        query = """
            SELECT
                pa.payment_id,
                pa.assessment_id,
                p.tax_account_id,
                a.tax_period_id,
                p.payment_date,
                pa.charge_type_code,
                pa.allocated_amount,
                YEAR(p.payment_date) as fiscal_year
            FROM payment_refund.payment_allocation pa
            INNER JOIN payment_refund.payment p ON pa.payment_id = p.payment_id
            INNER JOIN filing_assessment.assessment a ON pa.assessment_id = a.assessment_id
            ORDER BY p.payment_date, pa.allocation_sequence
        """

        rows = self.db.fetch_all(query)
        allocations = []

        for row in rows:
            allocations.append({
                'payment_id': row[0],
                'assessment_id': row[1],
                'tax_account_id': row[2],
                'tax_period_id': row[3],
                'transaction_date': row[4],
                'charge_type_code': row[5],
                'amount': row[6],
                'fiscal_year': row[7]
            })

        return allocations

    def _generate_assessment_transactions(self, components: List[Dict]):
        """
        Generate accounting transactions for assessment components (debits to tax account).
        Each component (principal, penalty, interest) gets its own transaction.
        """
        logger.info("Generating assessment transactions...")

        for component in components:
            # Get fiscal_year_id
            fiscal_year_id = self._get_fiscal_year_id(component['fiscal_year'])

            # Get current running balance
            current_balance = self._get_current_balance(
                component['tax_account_id'],
                component['tax_period_id'],
                component['charge_type_code']
            )

            # Debit increases liability (positive amount)
            new_balance = current_balance + component['amount']

            # Insert transaction
            self.db.execute_query("""
                INSERT INTO accounting.tax_transaction
                (tax_account_id, tax_period_id, transaction_type_code, charge_type_code,
                 transaction_date, posting_date, amount, balance_after_transaction,
                 source_document_type, source_document_id, transaction_status_code,
                 fiscal_year_id, is_automatically_allocated,
                 created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                component['tax_account_id'], component['tax_period_id'],
                'LIABILITY', component['charge_type_code'],
                component['transaction_date'], component['transaction_date'],
                component['amount'], new_balance,
                'ASSESSMENT', component['assessment_id'], 'POSTED',
                fiscal_year_id, True,
                DEFAULT_USER, datetime.now()
            ))

            self.generated_counts['transactions'] += 1

        self.db.commit()
        logger.info(f"  Generated {len(components)} assessment transactions")

    def _generate_payment_transactions(self, allocations: List[Dict]):
        """
        Generate accounting transactions for payment allocations (credits to tax account).
        Each allocation gets its own transaction.
        """
        logger.info("Generating payment transactions...")

        for allocation in allocations:
            # Get fiscal_year_id
            fiscal_year_id = self._get_fiscal_year_id(allocation['fiscal_year'])

            # Get current running balance
            current_balance = self._get_current_balance(
                allocation['tax_account_id'],
                allocation['tax_period_id'],
                allocation['charge_type_code']
            )

            # Credit reduces liability (negative amount)
            payment_amount = -allocation['amount']  # Negative for credit
            new_balance = current_balance + payment_amount

            # Insert transaction
            self.db.execute_query("""
                INSERT INTO accounting.tax_transaction
                (tax_account_id, tax_period_id, transaction_type_code, charge_type_code,
                 transaction_date, posting_date, amount, balance_after_transaction,
                 source_document_type, source_document_id, transaction_status_code,
                 fiscal_year_id, is_automatically_allocated,
                 created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                allocation['tax_account_id'], allocation['tax_period_id'],
                'PAYMENT', allocation['charge_type_code'],
                allocation['transaction_date'], allocation['transaction_date'],
                payment_amount, new_balance,
                'PAYMENT_RECEIPT', allocation['payment_id'], 'POSTED',
                fiscal_year_id, True,
                DEFAULT_USER, datetime.now()
            ))

            self.generated_counts['transactions'] += 1

        self.db.commit()
        logger.info(f"  Generated {len(allocations)} payment transactions")

    def _generate_account_balances(self):
        """
        Calculate and generate account balances per tax account, period, and charge type.
        """
        logger.info("Generating account balances...")

        # Get all unique combinations
        balances_query = """
            SELECT DISTINCT
                t.tax_account_id,
                t.tax_period_id,
                t.charge_type_code
            FROM accounting.tax_transaction t
            ORDER BY t.tax_account_id, t.tax_period_id, t.charge_type_code
        """

        balance_keys = self.db.fetch_all(balances_query)

        for key in balance_keys:
            tax_account_id = key[0]
            tax_period_id = key[1]
            charge_type_code = key[2]

            # Calculate totals for this combination
            totals_query = """
                SELECT
                    IFNULL(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) as debit_total,
                    IFNULL(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 0) as credit_total
                FROM accounting.tax_transaction
                WHERE tax_account_id = %s
                AND tax_period_id = %s
                AND charge_type_code = %s
            """

            totals = self.db.fetch_one(totals_query, (tax_account_id, tax_period_id, charge_type_code))
            debit_amount = totals[0] if totals else Decimal('0.00')
            credit_amount = totals[1] if totals else Decimal('0.00')

            # For Phase D simplicity: opening_balance = 0 (no carry-forward)
            opening_balance = Decimal('0.00')
            closing_balance = opening_balance + debit_amount - credit_amount

            # Determine if this is current period
            is_current = 1  # Simplified for Phase D

            # Insert account balance
            self.db.execute_query("""
                INSERT INTO accounting.account_balance
                (tax_account_id, tax_period_id, charge_type_code,
                 opening_balance, debit_amount, credit_amount, closing_balance,
                 is_current, last_updated_date, created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                tax_account_id, tax_period_id, charge_type_code,
                opening_balance, debit_amount, credit_amount, closing_balance,
                is_current, datetime.now(), DEFAULT_USER, datetime.now()
            ))
            self.generated_counts['account_balances'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['account_balances']} account balances")

    def _get_current_balance(self, tax_account_id: int, tax_period_id: int, charge_type_code: str) -> Decimal:
        """
        Get current running balance for an account/period/charge type.
        Used to calculate balance_after_transaction.
        """
        result = self.db.fetch_one("""
            SELECT IFNULL(MAX(balance_after_transaction), 0)
            FROM accounting.tax_transaction
            WHERE tax_account_id = %s
            AND tax_period_id = %s
            AND charge_type_code = %s
        """, (tax_account_id, tax_period_id, charge_type_code))

        return result[0] if result else Decimal('0.00')

    def _get_fiscal_year_id(self, fiscal_year: int) -> int:
        """Get fiscal_year_id for a given year."""
        result = self.db.fetch_one(
            "SELECT fiscal_year_id FROM accounting.fiscal_year WHERE fiscal_year = %s",
            (fiscal_year,)
        )
        if not result:
            raise ValueError(f"Fiscal year {fiscal_year} not found")
        return result[0]

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("ACCOUNTING GENERATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Fiscal Years:         {self.generated_counts['fiscal_years']}")
        logger.info(f"Transactions:         {self.generated_counts['transactions']}")
        logger.info(f"Account Balances:     {self.generated_counts['account_balances']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for accounting generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = AccountingGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Accounting generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
