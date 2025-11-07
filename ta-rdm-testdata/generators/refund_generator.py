"""
Refund Generator for TA-RDM (Phase F).

Generates refund records for overpayments identified in the payment system.
Builds on Phase D payments to identify and process overpayment refunds.
"""

import logging
import random
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)


class RefundGenerator:
    """
    Generates refund data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize refund generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'refunds': 0,
            'refund_lines': 0,
            'refund_vouchers': 0,
            'refund_approvals': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all refund data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Refund Generation (Phase F)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            self.db.commit()

            # Step 1: Query overpayments
            overpayments = self._query_overpayments()
            logger.info(f"Found {len(overpayments)} overpayment(s) for refund processing")

            if len(overpayments) == 0:
                logger.info("No overpayments found - skipping refund generation")
                # Re-enable FK checks
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
                return

            # Step 2: Generate refund records
            self._generate_refunds(overpayments)

            # Step 3: Generate refund lines (breakdown by charge type)
            self._generate_refund_lines()

            # Step 4: Generate refund vouchers for approved refunds
            self._generate_refund_vouchers()

            # Step 5: Generate refund approvals for those requiring approval
            self._generate_refund_approvals()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            self.db.commit()

            # Step 6: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Refund Generation Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Refund generation failed: {e}", exc_info=True)
            # Try to re-enable FK checks even on error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
            except:
                pass
            raise

    def _query_overpayments(self) -> List[Dict]:
        """
        Query assessments where payments exceeded assessment amount.
        """
        query = """
            SELECT
                a.assessment_id,
                a.tax_account_id,
                ta.party_id,
                a.tax_period_id,
                ta.tax_type_code,
                a.assessment_date,
                a.payment_due_date,
                a.net_assessment_amount,
                IFNULL(SUM(pa.allocated_amount), 0) as total_paid,
                IFNULL(SUM(pa.allocated_amount), 0) - a.net_assessment_amount as overpayment_amount
            FROM filing_assessment.assessment a
            INNER JOIN registration.tax_account ta ON a.tax_account_id = ta.tax_account_id
            LEFT JOIN payment_refund.payment_allocation pa ON a.assessment_id = pa.assessment_id
            WHERE a.is_current_version = 1
            GROUP BY a.assessment_id, a.tax_account_id, ta.party_id, a.tax_period_id,
                     ta.tax_type_code, a.assessment_date, a.payment_due_date, a.net_assessment_amount
            HAVING total_paid > a.net_assessment_amount
            ORDER BY overpayment_amount DESC
        """

        rows = self.db.fetch_all(query)
        overpayments = []

        for row in rows:
            overpayments.append({
                'assessment_id': row[0],
                'tax_account_id': row[1],
                'party_id': row[2],
                'tax_period_id': row[3],
                'tax_type_code': row[4],
                'assessment_date': row[5],
                'payment_due_date': row[6],
                'net_assessment_amount': row[7],
                'total_paid': row[8],
                'overpayment_amount': row[9]
            })

        return overpayments

    def _generate_refunds(self, overpayments: List[Dict]):
        """
        Generate refund records for overpayments.
        """
        logger.info("Generating refunds...")

        # Check if refunds already exist and truncate
        existing = self.db.fetch_one("SELECT COUNT(*) FROM payment_refund.refund")
        if existing and existing[0] > 0:
            logger.info(f"  Deleting {existing[0]} existing refund(s)")
            self.db.execute_query("DELETE FROM payment_refund.refund_approval")
            self.db.execute_query("DELETE FROM payment_refund.refund_line")
            self.db.execute_query("DELETE FROM payment_refund.refund_voucher")
            self.db.execute_query("DELETE FROM payment_refund.refund")
            self.db.commit()

        refund_id_counter = 1
        current_year = datetime.now().year

        for overpayment in overpayments:
            # Refund characteristics
            refund_number = f"RFD-{current_year}-{refund_id_counter:06d}"
            refund_type = 'OVERPAYMENT'

            # Calculate refund amounts
            gross_refund_amount = overpayment['overpayment_amount']

            # Phase F simplification: No offsetting
            offset_amount = Decimal('0.00')
            net_refund_amount = gross_refund_amount - offset_amount

            # Interest: Simple calculation (0.5% of gross for delays > 30 days)
            interest_amount = Decimal('0.00')  # Phase F: simplify to 0

            total_refund_amount = net_refund_amount + interest_amount

            # Claim date: shortly after payment
            claim_date = overpayment['payment_due_date'] + timedelta(days=random.randint(10, 30))

            # Refund status progression with different dates
            status_roll = random.random()
            if status_roll < 0.15:
                refund_status = 'CLAIMED'
                review_assigned_to = None
                review_start_date = None
                review_completed_date = None
                approval_required = True
                approved_by = None
                approval_date = None
                refund_voucher_id = None
                treasury_instruction_date = None
                refund_date = None
                bank_confirmation_received = False
                bank_confirmation_date = None
            elif status_roll < 0.30:
                refund_status = 'UNDER_REVIEW'
                review_assigned_to = 'review_officer_1'
                review_start_date = claim_date + timedelta(days=random.randint(3, 7))
                review_completed_date = None
                approval_required = True
                approved_by = None
                approval_date = None
                refund_voucher_id = None
                treasury_instruction_date = None
                refund_date = None
                bank_confirmation_received = False
                bank_confirmation_date = None
            elif status_roll < 0.50:
                refund_status = 'PENDING_APPROVAL'
                review_assigned_to = 'review_officer_1'
                review_start_date = claim_date + timedelta(days=random.randint(3, 7))
                review_completed_date = review_start_date + timedelta(days=random.randint(5, 10))
                approval_required = True
                approved_by = None
                approval_date = None
                refund_voucher_id = None
                treasury_instruction_date = None
                refund_date = None
                bank_confirmation_received = False
                bank_confirmation_date = None
            elif status_roll < 0.75:
                refund_status = 'APPROVED'
                review_assigned_to = 'review_officer_1'
                review_start_date = claim_date + timedelta(days=random.randint(3, 7))
                review_completed_date = review_start_date + timedelta(days=random.randint(5, 10))
                approval_required = True
                approved_by = 'approval_officer_1'
                approval_date = review_completed_date + timedelta(days=random.randint(1, 5))
                refund_voucher_id = None
                treasury_instruction_date = None
                refund_date = None
                bank_confirmation_received = False
                bank_confirmation_date = None
            else:
                refund_status = 'DISBURSED'
                review_assigned_to = 'review_officer_1'
                review_start_date = claim_date + timedelta(days=random.randint(3, 7))
                review_completed_date = review_start_date + timedelta(days=random.randint(5, 10))
                approval_required = True
                approved_by = 'approval_officer_1'
                approval_date = review_completed_date + timedelta(days=random.randint(1, 5))
                refund_voucher_id = None  # Will be set later when voucher created
                treasury_instruction_date = approval_date + timedelta(days=random.randint(1, 3))
                refund_date = treasury_instruction_date + timedelta(days=random.randint(2, 5))
                bank_confirmation_received = True
                bank_confirmation_date = refund_date + timedelta(days=random.randint(1, 2))

            # Earliest refund date (30 days after claim per regulations)
            earliest_refund_date = claim_date + timedelta(days=30)

            # Bank account (simplified IBAN format)
            refund_bank_account = f"GB{random.randint(10,99)}BANK{random.randint(1000000000,9999999999):010d}"

            self.db.execute_query("""
                INSERT INTO payment_refund.refund
                (refund_number, tax_account_id, party_id, tax_period_id, refund_type_code,
                 refund_status_code, claim_date, gross_refund_amount, offset_amount,
                 net_refund_amount, interest_amount, total_refund_amount, earliest_refund_date,
                 review_assigned_to, review_start_date, review_completed_date,
                 approval_required, approved_by, approval_date, rejection_reason,
                 refund_voucher_id, treasury_instruction_date, refund_date,
                 bank_confirmation_received, bank_confirmation_date, refund_bank_account,
                 refund_notes, created_by, created_date, modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                refund_number, overpayment['tax_account_id'], overpayment['party_id'],
                overpayment['tax_period_id'], refund_type, refund_status, claim_date,
                gross_refund_amount, offset_amount, net_refund_amount, interest_amount,
                total_refund_amount, earliest_refund_date, review_assigned_to,
                review_start_date, review_completed_date, approval_required, approved_by,
                approval_date, None, refund_voucher_id, treasury_instruction_date,
                refund_date, bank_confirmation_received, bank_confirmation_date,
                refund_bank_account, f"Overpayment refund for assessment {overpayment['assessment_id']}",
                DEFAULT_USER, datetime.now(), DEFAULT_USER, datetime.now()
            ))

            self.generated_counts['refunds'] += 1
            refund_id_counter += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['refunds']} refund(s)")

    def _generate_refund_lines(self):
        """
        Generate refund line items (breakdown by charge type).
        Phase F: Simple - one line per refund for PRINCIPAL_TAX.
        """
        logger.info("Generating refund lines...")

        # Query all refunds
        refunds = self.db.fetch_all("""
            SELECT refund_id, tax_period_id, gross_refund_amount
            FROM payment_refund.refund
        """)

        for refund in refunds:
            refund_id = refund[0]
            tax_period_id = refund[1]
            amount = refund[2]

            # Phase F simplification: Single line for PRINCIPAL_TAX
            self.db.execute_query("""
                INSERT INTO payment_refund.refund_line
                (refund_id, tax_period_id, charge_type_code, line_amount,
                 original_assessment_id, created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                refund_id, tax_period_id, 'PRINCIPAL_TAX', amount,
                None, DEFAULT_USER, datetime.now()
            ))

            self.generated_counts['refund_lines'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['refund_lines']} refund line(s)")

    def _generate_refund_vouchers(self):
        """
        Generate refund vouchers for disbursed refunds.
        """
        logger.info("Generating refund vouchers...")

        # Query disbursed refunds
        disbursed_refunds = self.db.fetch_all("""
            SELECT refund_id, refund_number, total_refund_amount,
                   approval_date, treasury_instruction_date, refund_date
            FROM payment_refund.refund
            WHERE refund_status_code = 'DISBURSED'
        """)

        if not disbursed_refunds:
            logger.info("  No disbursed refunds found - skipping voucher generation")
            return

        # Get a bank_id (assume bank exists from DDL)
        bank = self.db.fetch_one("SELECT bank_id FROM payment_refund.bank LIMIT 1")
        if not bank:
            logger.warning("  No banks found - cannot generate vouchers")
            return

        bank_id = bank[0]
        voucher_counter = 1
        current_year = datetime.now().year

        for refund in disbursed_refunds:
            refund_id = refund[0]
            total_amount = refund[2]
            approval_date = refund[3]
            sent_date = refund[4]
            execution_date = refund[5]

            voucher_number = f"RFV-{current_year}-{voucher_counter:06d}"
            voucher_date = approval_date

            self.db.execute_query("""
                INSERT INTO payment_refund.refund_voucher
                (voucher_number, voucher_date, total_voucher_amount, bank_id,
                 voucher_status_code, sent_to_treasury_date, execution_date,
                 created_by, created_date, modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                voucher_number, voucher_date, total_amount, bank_id,
                'EXECUTED', sent_date, execution_date,
                DEFAULT_USER, datetime.now(), DEFAULT_USER, datetime.now()
            ))

            # Get the voucher_id and update refund
            voucher_id = self.db.get_last_insert_id()
            self.db.execute_query("""
                UPDATE payment_refund.refund
                SET refund_voucher_id = %s, modified_date = %s
                WHERE refund_id = %s
            """, (voucher_id, datetime.now(), refund_id))

            self.generated_counts['refund_vouchers'] += 1
            voucher_counter += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['refund_vouchers']} refund voucher(s)")

    def _generate_refund_approvals(self):
        """
        Generate refund approval records for refunds requiring approval.
        """
        logger.info("Generating refund approvals...")

        # Query refunds that have been approved or beyond
        approved_refunds = self.db.fetch_all("""
            SELECT refund_id, approved_by, approval_date, total_refund_amount
            FROM payment_refund.refund
            WHERE approval_required = TRUE
            AND approved_by IS NOT NULL
        """)

        if not approved_refunds:
            logger.info("  No approved refunds found - skipping approval generation")
            return

        for refund in approved_refunds:
            refund_id = refund[0]
            approved_by = refund[1]
            approval_date = refund[2]
            amount = refund[3]

            # Approval decision
            approval_level = 1  # First level approval
            approver_role = 'MANAGER'  # Default role
            approval_decision = 'APPROVED'
            approval_comments = 'Refund approved - overpayment verified'

            self.db.execute_query("""
                INSERT INTO payment_refund.refund_approval
                (refund_id, approval_level, approver_user_id, approver_role,
                 approval_date, approval_decision, approval_comments,
                 created_by, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                refund_id, approval_level, approved_by, approver_role,
                approval_date, approval_decision, approval_comments,
                DEFAULT_USER, datetime.now()
            ))

            self.generated_counts['refund_approvals'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['refund_approvals']} refund approval(s)")

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("REFUND GENERATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Refunds:             {self.generated_counts['refunds']}")
        logger.info(f"Refund Lines:        {self.generated_counts['refund_lines']}")
        logger.info(f"Refund Vouchers:     {self.generated_counts['refund_vouchers']}")
        logger.info(f"Refund Approvals:    {self.generated_counts['refund_approvals']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for refund generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = RefundGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Refund generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
