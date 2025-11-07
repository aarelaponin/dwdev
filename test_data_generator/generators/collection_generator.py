"""
Collection & Enforcement Generator for TA-RDM (Phase E).

Generates collection cases and enforcement actions for unpaid assessments.
Builds on Phase C assessments and Phase D payments to identify delinquent accounts.
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)

# Collection tables use BIGINT for created_by (user ID), not VARCHAR
DEFAULT_USER_ID = 1  # System user ID


class CollectionGenerator:
    """
    Generates collection and enforcement data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize collection generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'collection_cases': 0,
            'enforcement_actions': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all collection data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Collection & Enforcement Generation (Phase E)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            self.db.commit()

            # Step 1: Query unpaid assessments
            unpaid_assessments = self._query_unpaid_assessments()
            logger.info(f"Found {len(unpaid_assessments)} unpaid assessment(s) for collection")

            # Phase E simplification: If no unpaid found (all paid in Phase D),
            # create synthetic cases from older assessments for demo purposes
            if len(unpaid_assessments) == 0:
                logger.info("No actual unpaid assessments found - creating synthetic cases for demo")
                unpaid_assessments = self._create_synthetic_unpaid_cases()

            # Step 2: Generate collection cases
            self._generate_collection_cases(unpaid_assessments)

            # Step 3: Generate enforcement actions for each case
            self._generate_enforcement_actions()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            self.db.commit()

            # Step 4: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Collection & Enforcement Generation Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Collection generation failed: {e}", exc_info=True)
            # Try to re-enable FK checks even on error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
            except:
                pass
            raise

    def _query_unpaid_assessments(self) -> List[Dict]:
        """
        Query assessments that have not been fully paid (Phase D non-payers).
        """
        query = """
            SELECT
                a.assessment_id,
                a.tax_account_id,
                ta.party_id,
                a.assessment_date,
                a.payment_due_date,
                a.net_assessment_amount,
                IFNULL(SUM(pa.allocated_amount), 0) as amount_paid,
                (a.net_assessment_amount - IFNULL(SUM(pa.allocated_amount), 0)) as amount_outstanding,
                DATEDIFF(CURDATE(), a.payment_due_date) as days_overdue
            FROM filing_assessment.assessment a
            INNER JOIN registration.tax_account ta ON a.tax_account_id = ta.tax_account_id
            LEFT JOIN payment_refund.payment_allocation pa ON a.assessment_id = pa.assessment_id
            WHERE a.is_current_version = 1
            AND a.net_assessment_amount > 0
            GROUP BY a.assessment_id,
                     a.tax_account_id,
                     ta.party_id,
                     a.assessment_date,
                     a.payment_due_date,
                     a.net_assessment_amount
            HAVING amount_outstanding > 0
            ORDER BY days_overdue DESC
        """

        rows = self.db.fetch_all(query)
        assessments = []

        for row in rows:
            assessments.append({
                'assessment_id': row[0],
                'tax_account_id': row[1],
                'party_id': row[2],
                'assessment_date': row[3],
                'payment_due_date': row[4],
                'net_assessment_amount': row[5],
                'amount_paid': row[6],
                'amount_outstanding': row[7],
                'days_overdue': row[8]
            })

        return assessments

    def _create_synthetic_unpaid_cases(self) -> List[Dict]:
        """
        Create synthetic unpaid cases from older assessments for demo purposes.
        Selects ~10% of assessments and treats them as partially unpaid.
        """
        query = """
            SELECT
                a.assessment_id,
                a.tax_account_id,
                ta.party_id,
                a.assessment_date,
                a.payment_due_date,
                a.net_assessment_amount
            FROM filing_assessment.assessment a
            INNER JOIN registration.tax_account ta ON a.tax_account_id = ta.tax_account_id
            WHERE a.is_current_version = 1
            AND a.net_assessment_amount > 0
            ORDER BY a.assessment_date
            LIMIT 7
        """

        rows = self.db.fetch_all(query)
        assessments = []

        for row in rows:
            # Create synthetic unpaid scenario
            net_amount = row[5]
            # Assume 60% was paid, 40% outstanding
            amount_paid = self.round_decimal(net_amount * Decimal('0.6'))
            amount_outstanding = net_amount - amount_paid

            # Calculate days overdue
            from datetime import date
            payment_due_date = row[4]
            days_overdue = (date.today() - payment_due_date).days

            assessments.append({
                'assessment_id': row[0],
                'tax_account_id': row[1],
                'party_id': row[2],
                'assessment_date': row[3],
                'payment_due_date': payment_due_date,
                'net_assessment_amount': net_amount,
                'amount_paid': amount_paid,
                'amount_outstanding': amount_outstanding,
                'days_overdue': max(days_overdue, 30)  # At least 30 days overdue
            })

        return assessments

    def _generate_collection_cases(self, unpaid_assessments: List[Dict]):
        """
        Generate collection cases for unpaid assessments.
        """
        logger.info("Generating collection cases...")

        # Check if cases already exist and truncate
        existing = self.db.fetch_one("SELECT COUNT(*) FROM compliance_control.collection_case")
        if existing and existing[0] > 0:
            logger.info(f"  Deleting {existing[0]} existing collection case(s)")
            self.db.execute_query("DELETE FROM compliance_control.enforcement_action")
            self.db.execute_query("DELETE FROM compliance_control.collection_case")
            self.db.commit()

        case_id_counter = 1

        for assessment in unpaid_assessments:
            # Determine case characteristics based on debt age and amount
            days_overdue = assessment['days_overdue']
            debt_amount = assessment['amount_outstanding']

            # Determine priority
            if debt_amount > 50000 or days_overdue > 180:
                priority = 'URGENT'
            elif debt_amount > 20000 or days_overdue > 90:
                priority = 'HIGH'
            elif days_overdue > 30:
                priority = 'MEDIUM'
            else:
                priority = 'LOW'

            # Determine case type
            if debt_amount > 100000:
                case_type = 'HIGH_VALUE'
            else:
                case_type = 'STANDARD_COLLECTION'

            # Determine enforcement level based on age
            if days_overdue < 30:
                enforcement_level = 'NOTICE'
            elif days_overdue < 60:
                enforcement_level = 'DEMAND'
            elif days_overdue < 120:
                enforcement_level = 'GARNISHMENT'
            else:
                enforcement_level = 'LEGAL_ACTION'

            # Case status (most are active collection)
            case_status = 'ACTIVE_COLLECTION'

            # Collectability score (random but influenced by days overdue)
            if days_overdue < 60:
                collectability_score = self.round_decimal(Decimal(str(random.uniform(70, 95))))
            elif days_overdue < 180:
                collectability_score = self.round_decimal(Decimal(str(random.uniform(40, 70))))
            else:
                collectability_score = self.round_decimal(Decimal(str(random.uniform(10, 40))))

            # Case opened date (shortly after due date)
            case_opened_date = assessment['payment_due_date'] + timedelta(days=random.randint(15, 30))

            # Contact attempts
            taxpayer_contact_attempted = True
            taxpayer_contact_successful = random.choice([True, False])
            last_contact_date = case_opened_date + timedelta(days=random.randint(5, 20)) if taxpayer_contact_attempted else None

            # Next action due
            next_action_due_date = datetime.now().date() + timedelta(days=random.randint(5, 15))

            # Insert collection case
            case_number = f"COLL-{case_id_counter:06d}"

            self.db.execute_query("""
                INSERT INTO compliance_control.collection_case
                (case_number, party_id, tax_account_id, case_type_code, case_status_code,
                 priority_code, case_opened_date, original_debt_amount, current_debt_amount,
                 amount_collected, debt_age_days, collectability_score, enforcement_level_code,
                 has_payment_agreement, taxpayer_contact_attempted, taxpayer_contact_successful,
                 last_contact_date, next_action_due_date,
                 created_by, created_date, modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                case_number, assessment['party_id'], assessment['tax_account_id'],
                case_type, case_status, priority, case_opened_date,
                assessment['amount_outstanding'], assessment['amount_outstanding'],
                assessment['amount_paid'], days_overdue, collectability_score,
                enforcement_level, False, taxpayer_contact_attempted,
                taxpayer_contact_successful, last_contact_date, next_action_due_date,
                DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
            ))

            self.generated_counts['collection_cases'] += 1
            case_id_counter += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['collection_cases']} collection case(s)")

    def _generate_enforcement_actions(self):
        """
        Generate enforcement actions for each collection case.
        Each case gets 1-4 actions depending on enforcement level.
        """
        logger.info("Generating enforcement actions...")

        # Query collection cases
        cases_query = """
            SELECT
                collection_case_id,
                case_number,
                party_id,
                tax_account_id,
                enforcement_level_code,
                current_debt_amount,
                case_opened_date
            FROM compliance_control.collection_case
            ORDER BY case_opened_date
        """

        cases = self.db.fetch_all(cases_query)

        for case in cases:
            collection_case_id = case[0]
            enforcement_level = case[4]
            debt_amount = case[5]
            case_opened_date = case[6]

            # Determine number of actions based on enforcement level
            if enforcement_level == 'NOTICE':
                action_types = ['REMINDER_NOTICE']
            elif enforcement_level == 'DEMAND':
                action_types = ['REMINDER_NOTICE', 'DEMAND_NOTICE']
            elif enforcement_level == 'GARNISHMENT':
                action_types = ['REMINDER_NOTICE', 'DEMAND_NOTICE', 'PHONE_CONTACT', 'BANK_GARNISHMENT']
            else:  # LEGAL_ACTION
                action_types = ['REMINDER_NOTICE', 'DEMAND_NOTICE', 'PHONE_CONTACT',
                                'FIELD_VISIT', 'LEGAL_FILING']

            # Generate actions in sequence
            action_date = case_opened_date
            for idx, action_type in enumerate(action_types):
                # Action date increments
                if idx > 0:
                    action_date = action_date + timedelta(days=random.randint(10, 20))

                # Action status
                if idx < len(action_types) - 1:
                    action_status = 'COMPLETED'
                else:
                    action_status = random.choice(['COMPLETED', 'IN_PROGRESS'])

                # Outcome (max 20 chars for VARCHAR(20))
                if action_status == 'COMPLETED':
                    action_outcome = random.choice([
                        'SUCCESSFUL', 'PARTIAL', 'UNSUCCESSFUL',
                        'RESPONSIVE', 'UNRESPONSIVE'
                    ])
                else:
                    action_outcome = None

                # Amount collected (some actions result in payment)
                if action_outcome == 'SUCCESSFUL':
                    amount_collected = self.round_decimal(debt_amount * Decimal(random.uniform(0.3, 0.8)))
                elif action_outcome == 'PARTIAL':
                    amount_collected = self.round_decimal(debt_amount * Decimal(random.uniform(0.1, 0.3)))
                else:
                    amount_collected = Decimal('0.00')

                # Next action recommendation
                if idx < len(action_types) - 1:
                    next_action_recommended = action_types[idx + 1]
                else:
                    next_action_recommended = None

                self.db.execute_query("""
                    INSERT INTO compliance_control.enforcement_action
                    (collection_case_id, action_type_code, action_date, action_status_code,
                     performed_by, target_amount, amount_collected, action_outcome_code,
                     next_action_recommended, next_action_due_date,
                     created_by, created_date, modified_by, modified_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    collection_case_id, action_type, action_date, action_status,
                    DEFAULT_USER_ID, debt_amount, amount_collected, action_outcome,
                    next_action_recommended,
                    action_date + timedelta(days=15) if next_action_recommended else None,
                    DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
                ))

                self.generated_counts['enforcement_actions'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['enforcement_actions']} enforcement action(s)")

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("COLLECTION & ENFORCEMENT GENERATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Collection Cases:        {self.generated_counts['collection_cases']}")
        logger.info(f"Enforcement Actions:     {self.generated_counts['enforcement_actions']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for collection generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = CollectionGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Collection generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
