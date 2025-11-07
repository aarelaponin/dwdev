"""
Payment & Refund Generator for TA-RDM (Phase D).

Generates payments, payment allocations, and refunds for test data.
Builds on Phase C assessments to create realistic payment scenarios.
"""

import logging
import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)


class PaymentGenerator:
    """
    Generates payment and refund data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize payment generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'payments': 0,
            'payment_allocations': 0,
            'refunds': 0,
            'banks': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all payment and refund data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Payment & Refund Generation (Phase D)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug in payment_refund FK constraints
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            self.db.commit()

            # Step 1: Generate bank master data
            self._generate_banks()

            # Step 2: Query existing assessments
            assessments = self._query_assessments()
            logger.info(f"Found {len(assessments)} assessments to generate payments for")

            # Step 3: Generate payments and allocations
            self._generate_payments_and_allocations(assessments)

            # Step 4: Generate VAT export refunds
            self._generate_refunds()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            self.db.commit()

            # Step 5: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Payment & Refund Generation Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Payment generation failed: {e}", exc_info=True)
            # Try to re-enable FK checks even on error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
            except:
                pass
            raise

    def _generate_banks(self):
        """Generate bank master data."""
        logger.info("Generating bank master data...")

        # Check if banks already exist
        existing = self.db.fetch_one("SELECT COUNT(*) FROM payment_refund.bank")
        if existing and existing[0] > 0:
            logger.info(f"  Banks already exist ({existing[0]} records), skipping generation")
            return

        banks = [
            {
                'bank_code': 'CB001',
                'bank_name': 'Central Commercial Bank',
                'swift_code': 'CCBKLKLX',
                'payment_interface_type': 'API',
                'is_active': True,
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            },
            {
                'bank_code': 'PB002',
                'bank_name': 'Peoples Bank',
                'swift_code': 'PEPLKLKX',
                'payment_interface_type': 'FILE_IMPORT',
                'is_active': True,
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            },
            {
                'bank_code': 'NB003',
                'bank_name': 'National Bank',
                'swift_code': 'NBLNKLKX',
                'payment_interface_type': 'FILE_IMPORT',
                'is_active': True,
                'created_by': DEFAULT_USER,
                'created_date': datetime.now()
            }
        ]

        for bank in banks:
            self.db.execute_query("""
                INSERT INTO payment_refund.bank
                (bank_code, bank_name, swift_code, payment_interface_type, is_active, created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                bank['bank_code'], bank['bank_name'], bank['swift_code'],
                bank['payment_interface_type'], bank['is_active'], bank['created_by'],
                bank['created_date']
            ))
            self.generated_counts['banks'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['banks']} banks")

    def _query_assessments(self) -> List[Dict]:
        """Query existing assessments from Phase C."""
        query = """
            SELECT
                a.assessment_id,
                a.tax_account_id,
                a.tax_period_id,
                a.assessment_date,
                a.payment_due_date,
                a.assessed_tax_amount,
                a.penalty_amount,
                a.interest_amount,
                a.net_assessment_amount,
                a.assessment_type_code,
                ta.party_id,
                tp.period_end_date,
                tt.tax_type_code
            FROM filing_assessment.assessment a
            INNER JOIN registration.tax_account ta ON a.tax_account_id = ta.tax_account_id
            INNER JOIN tax_framework.tax_period tp ON a.tax_period_id = tp.tax_period_id
            INNER JOIN tax_framework.tax_type tt ON tp.tax_type_id = tt.tax_type_id
            WHERE a.is_current_version = 1
            AND a.net_assessment_amount > 0
            ORDER BY a.assessment_date
        """

        rows = self.db.fetch_all(query)
        assessments = []

        for row in rows:
            assessments.append({
                'assessment_id': row[0],
                'tax_account_id': row[1],
                'tax_period_id': row[2],
                'assessment_date': row[3],
                'payment_due_date': row[4],
                'assessed_tax_amount': row[5],
                'penalty_amount': row[6],
                'interest_amount': row[7],
                'net_assessment_amount': row[8],
                'assessment_type': row[9],
                'party_id': row[10],
                'period_end_date': row[11],
                'tax_type_code': row[12]
            })

        return assessments

    def _generate_payments_and_allocations(self, assessments: List[Dict]):
        """
        Generate payments and allocations for assessments.

        Payment Scenarios:
        - 70% Compliant: Pay in full, on time
        - 20% Late: Pay late in 1-2 installments with additional penalties
        - 10% Non-payers: No payment (will go to collections)
        """
        logger.info("Generating payments and allocations...")

        payment_id_counter = 1
        allocation_id_counter = 1

        # Get bank IDs
        bank_rows = self.db.fetch_all("SELECT bank_id FROM payment_refund.bank LIMIT 3")
        bank_ids = [row[0] for row in bank_rows]

        for idx, assessment in enumerate(assessments):
            # Determine payment behavior
            scenario = random.random()

            if scenario < 0.70:  # 70% compliant
                self._generate_compliant_payment(
                    assessment, payment_id_counter, allocation_id_counter, bank_ids
                )
                payment_id_counter += 1
                allocation_id_counter += self._count_charge_types(assessment)

            elif scenario < 0.90:  # 20% late payers
                num_installments = random.randint(1, 2)
                self._generate_late_payments(
                    assessment, payment_id_counter, allocation_id_counter,
                    bank_ids, num_installments
                )
                payment_id_counter += num_installments
                allocation_id_counter += num_installments * self._count_charge_types(assessment)

            # else: 10% non-payers - no payment generated

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['payments']} payments")
        logger.info(f"  Generated {self.generated_counts['payment_allocations']} allocations")

    def _count_charge_types(self, assessment: Dict) -> int:
        """Count how many charge types have non-zero amounts."""
        count = 0
        if assessment['assessed_tax_amount'] > 0:
            count += 1
        if assessment['penalty_amount'] > 0:
            count += 1
        if assessment['interest_amount'] > 0:
            count += 1
        return max(count, 1)  # At least 1

    def _generate_compliant_payment(self, assessment: Dict, payment_id: int,
                                     allocation_id: int, bank_ids: List[int]):
        """Generate a compliant on-time full payment."""
        payment_date = assessment['payment_due_date'] - timedelta(days=random.randint(0, 5))
        received_date = payment_date + timedelta(days=random.randint(0, 1))

        payment_ref = f"PAY-{assessment['tax_type_code']}-{payment_id}"
        receipt_number = f"RCP-{payment_id:06d}"

        # Insert payment
        self.db.execute_query("""
            INSERT INTO payment_refund.payment
            (payment_reference_number, tax_account_id, party_id, payment_type_code,
             payment_location_code, payment_date, received_date, payment_amount,
             currency_code, amount_in_domestic_currency, bank_id,
             payment_status_code, is_allocated, allocated_date, is_suspended,
             is_reconciled, receipt_number, receipt_issued_date,
             created_by, created_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            payment_ref, assessment['tax_account_id'], assessment['party_id'],
            random.choice(['BANK_TRANSFER', 'E_PORTAL', 'DIRECT_DEBIT']),
            random.choice(['BANK_BRANCH', 'E_PORTAL']),
            payment_date, received_date, assessment['net_assessment_amount'],
            'LKR', assessment['net_assessment_amount'], random.choice(bank_ids),
            'ALLOCATED', True, datetime.now(), False, True,
            receipt_number, datetime.now(), DEFAULT_USER, datetime.now()
        ))

        self.generated_counts['payments'] += 1

        # Allocate payment to assessment components
        self._allocate_payment_to_assessment(
            payment_id, assessment, allocation_id, payment_date
        )

    def _generate_late_payments(self, assessment: Dict, payment_id: int,
                                allocation_id: int, bank_ids: List[int],
                                num_installments: int):
        """Generate late partial payments."""
        days_late = random.randint(15, 60)
        total_owed = assessment['net_assessment_amount']

        # Split payment into installments
        for i in range(num_installments):
            if i == num_installments - 1:
                # Last installment pays remaining
                installment_amount = total_owed
            else:
                # Partial payment
                installment_amount = self.round_decimal(total_owed * Decimal('0.4'))
                total_owed -= installment_amount

            payment_date = assessment['payment_due_date'] + timedelta(days=days_late + (i * 15))
            received_date = payment_date + timedelta(days=random.randint(0, 2))

            payment_ref = f"PAY-{assessment['tax_type_code']}-{payment_id + i}"
            receipt_number = f"RCP-{(payment_id + i):06d}"

            # Insert payment
            self.db.execute_query("""
                INSERT INTO payment_refund.payment
                (payment_reference_number, tax_account_id, party_id, payment_type_code,
                 payment_location_code, payment_date, received_date, payment_amount,
                 currency_code, amount_in_domestic_currency, bank_id,
                 payment_status_code, is_allocated, allocated_date, is_suspended,
                 is_reconciled, receipt_number, receipt_issued_date,
                 created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payment_ref, assessment['tax_account_id'], assessment['party_id'],
                random.choice(['BANK_TRANSFER', 'CASH', 'CHECK']),
                random.choice(['BANK_BRANCH', 'TAX_OFFICE']),
                payment_date, received_date, installment_amount,
                'LKR', installment_amount, random.choice(bank_ids),
                'ALLOCATED', True, datetime.now(), False, True,
                receipt_number, datetime.now(), DEFAULT_USER, datetime.now()
            ))

            self.generated_counts['payments'] += 1

            # Allocate this installment
            self._allocate_payment_to_assessment(
                payment_id + i, assessment, allocation_id + (i * 3), payment_date
            )

    def _allocate_payment_to_assessment(self, payment_id: int, assessment: Dict,
                                        allocation_id: int, payment_date: date):
        """
        Allocate payment to assessment components (principal, penalty, interest).
        Follows allocation priority: Interest → Penalty → Principal
        """
        sequence = 1

        # Allocate to interest first (if any)
        if assessment['interest_amount'] > 0:
            self.db.execute_query("""
                INSERT INTO payment_refund.payment_allocation
                (payment_id, assessment_id, charge_type_code, allocated_amount,
                 allocation_sequence, allocation_date, is_automatic, created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payment_id, assessment['assessment_id'], 'INTEREST',
                assessment['interest_amount'], sequence, payment_date,
                True, DEFAULT_USER, datetime.now()
            ))
            self.generated_counts['payment_allocations'] += 1
            sequence += 1

        # Allocate to penalty (if any)
        if assessment['penalty_amount'] > 0:
            self.db.execute_query("""
                INSERT INTO payment_refund.payment_allocation
                (payment_id, assessment_id, charge_type_code, allocated_amount,
                 allocation_sequence, allocation_date, is_automatic, created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payment_id, assessment['assessment_id'], 'PENALTY',
                assessment['penalty_amount'], sequence, payment_date,
                True, DEFAULT_USER, datetime.now()
            ))
            self.generated_counts['payment_allocations'] += 1
            sequence += 1

        # Allocate to principal tax
        if assessment['assessed_tax_amount'] > 0:
            self.db.execute_query("""
                INSERT INTO payment_refund.payment_allocation
                (payment_id, assessment_id, charge_type_code, allocated_amount,
                 allocation_sequence, allocation_date, is_automatic, created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payment_id, assessment['assessment_id'], 'PRINCIPAL_TAX',
                assessment['assessed_tax_amount'], sequence, payment_date,
                True, DEFAULT_USER, datetime.now()
            ))
            self.generated_counts['payment_allocations'] += 1

    def _generate_refunds(self):
        """Generate VAT export refunds (simple scenario)."""
        logger.info("Generating refunds...")

        # For Phase D, we'll generate a few simple refunds
        # In real system, these would come from VAT returns with export sales
        # For now, skip refunds as they require more complex setup

        logger.info("  Refunds deferred to future phase (requires VAT-specific data)")

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("PAYMENT GENERATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Banks:               {self.generated_counts['banks']}")
        logger.info(f"Payments:            {self.generated_counts['payments']}")
        logger.info(f"Payment Allocations: {self.generated_counts['payment_allocations']}")
        logger.info(f"Refunds:             {self.generated_counts['refunds']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for payment generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = PaymentGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Payment generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
