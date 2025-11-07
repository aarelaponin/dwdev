"""
Objection & Appeal Generator for TA-RDM (Phase H).

Generates objection cases and appeal cases for disputed tax assessments and audit findings.
Builds on existing audit and assessment data to create realistic dispute scenarios.
"""

import logging
import random
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)

# Objection tables use BIGINT for user IDs
DEFAULT_USER_ID = 1


class ObjectionGenerator:
    """
    Generates objection and appeal data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize objection generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'objection_cases': 0,
            'appeal_cases': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all objection and appeal data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Objection & Appeal Generation (Phase H)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            self.db.commit()

            # Step 1: Query objection candidates (audits with findings where taxpayer disagreed)
            objection_candidates = self._query_objection_candidates()
            logger.info(f"Found {len(objection_candidates)} objection candidate(s)")

            if len(objection_candidates) == 0:
                logger.info("No objection candidates found - skipping objection generation")
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
                return

            # Step 2: Generate objection cases
            self._generate_objection_cases(objection_candidates)

            # Step 3: Generate appeal cases for rejected objections
            self._generate_appeal_cases()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            self.db.commit()

            # Step 4: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Objection & Appeal Generation Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Objection generation failed: {e}", exc_info=True)
            # Try to re-enable FK checks even on error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
            except:
                pass
            raise

    def _query_objection_candidates(self) -> List[Dict]:
        """
        Query audit cases or assessments suitable for objections.
        Focus on audits with findings where taxpayer_agreed = FALSE.
        """
        query = """
            SELECT
                ac.audit_case_id,
                ac.party_id,
                ac.tax_type_code,
                ac.total_amount_assessed,
                ac.adjustment_amount,
                ac.penalty_amount,
                ac.interest_amount,
                ac.actual_completion_date,
                ac.taxpayer_agreed
            FROM compliance_control.audit_case ac
            WHERE ac.case_status_code = 'FINALIZED'
            AND ac.total_amount_assessed IS NOT NULL
            AND ac.total_amount_assessed > 0
            AND (ac.taxpayer_agreed = FALSE OR ac.taxpayer_agreed IS NULL)
            ORDER BY ac.total_amount_assessed DESC
        """

        rows = self.db.fetch_all(query)
        candidates = []

        for row in rows:
            candidates.append({
                'audit_case_id': row[0],
                'party_id': row[1],
                'tax_type_code': row[2],
                'total_amount_assessed': row[3],
                'adjustment_amount': row[4],
                'penalty_amount': row[5],
                'interest_amount': row[6],
                'completion_date': row[7],
                'taxpayer_agreed': row[8]
            })

        return candidates

    def _generate_objection_cases(self, candidates: List[Dict]):
        """
        Generate objection cases for disputed audit findings.
        """
        logger.info("Generating objection cases...")

        # Check if objections already exist
        existing = self.db.fetch_one("SELECT COUNT(*) FROM compliance_control.objection_case")
        if existing and existing[0] > 0:
            logger.info(f"  Deleting {existing[0]} existing objection case(s)")
            self.db.execute_query("DELETE FROM compliance_control.appeal_case")
            self.db.execute_query("DELETE FROM compliance_control.objection_case")
            self.db.commit()

        case_counter = 1
        current_year = 2024

        for candidate in candidates:
            case_number = f"OBJ-{current_year}-{case_counter:04d}"

            # Objection type
            objection_type = 'AUDIT_OBJECTION'
            objection_subject_type = 'AUDIT_CASE'

            # Filing date: shortly after audit completion
            completion_date = candidate['completion_date']
            filing_date = completion_date + timedelta(days=random.randint(10, 30))

            # Amount disputed (typically taxpayer disputes most of it)
            amount_disputed = candidate['total_amount_assessed']
            amount_at_stake_tax = candidate['adjustment_amount']
            amount_at_stake_penalty = candidate['penalty_amount']
            amount_at_stake_interest = candidate['interest_amount']

            # Case status and timeline
            status_roll = random.random()
            if status_roll < 0.20:
                case_status = 'UNDER_REVIEW'
                assignment_date = filing_date + timedelta(days=random.randint(3, 7))
                decision_date = None
                decision_outcome = None
                amount_adjusted = None
                appeal_filed = None
                closure_date = None
            elif status_roll < 0.40:
                case_status = 'HEARING_SCHEDULED'
                assignment_date = filing_date + timedelta(days=random.randint(3, 7))
                hearing_scheduled_date = filing_date + timedelta(days=random.randint(30, 60))
                decision_date = None
                decision_outcome = None
                amount_adjusted = None
                appeal_filed = None
                closure_date = None
            elif status_roll < 0.60:
                case_status = 'DECISION_ISSUED'
                assignment_date = filing_date + timedelta(days=random.randint(3, 7))
                hearing_scheduled_date = filing_date + timedelta(days=random.randint(30, 60))
                hearing_held_date = hearing_scheduled_date
                decision_date = hearing_held_date + timedelta(days=random.randint(10, 30))
                # Decision outcome
                decision_outcome = random.choice(['PARTIALLY_ACCEPTED', 'REJECTED'])
                if decision_outcome == 'PARTIALLY_ACCEPTED':
                    # Grant 30-50% relief
                    amount_adjusted = self.round_decimal(amount_disputed * Decimal(random.uniform(0.3, 0.5)))
                    appeal_filed = False
                else:
                    amount_adjusted = Decimal('0.00')
                    appeal_filed = random.choice([True, False])
                closure_date = decision_date + timedelta(days=random.randint(5, 15))
            else:
                case_status = 'CLOSED'
                assignment_date = filing_date + timedelta(days=random.randint(3, 7))
                hearing_scheduled_date = filing_date + timedelta(days=random.randint(30, 60))
                hearing_held_date = hearing_scheduled_date
                decision_date = hearing_held_date + timedelta(days=random.randint(10, 30))
                decision_outcome = random.choice(['PARTIALLY_ACCEPTED', 'REJECTED', 'SETTLED'])
                if decision_outcome == 'PARTIALLY_ACCEPTED':
                    amount_adjusted = self.round_decimal(amount_disputed * Decimal(random.uniform(0.3, 0.5)))
                    appeal_filed = False
                elif decision_outcome == 'SETTLED':
                    amount_adjusted = self.round_decimal(amount_disputed * Decimal(random.uniform(0.4, 0.7)))
                    appeal_filed = False
                else:
                    amount_adjusted = Decimal('0.00')
                    appeal_filed = random.choice([True, False])
                closure_date = decision_date + timedelta(days=random.randint(5, 15))

            # Priority based on amount
            if amount_disputed > 50000:
                priority = 'HIGH'
            elif amount_disputed > 20000:
                priority = 'MEDIUM'
            else:
                priority = 'LOW'

            # Taxpayer requested hearing
            taxpayer_requested_hearing = random.choice([True, False])

            self.db.execute_query("""
                INSERT INTO compliance_control.objection_case
                (case_number, party_id, objection_type_code, objection_subject_type,
                 assessment_id, audit_case_id, tax_type_code, tax_period_id,
                 filing_date, filing_method_code, case_status_code, priority_code,
                 assigned_officer_id, assignment_date, amount_disputed,
                 amount_at_stake_tax, amount_at_stake_penalty, amount_at_stake_interest,
                 objection_grounds, taxpayer_requested_hearing, hearing_scheduled_date,
                 hearing_held_date, legal_representative, decision_date,
                 decision_outcome_code, decision_summary, amount_adjusted,
                 appeal_filed, appeal_deadline_date, suspension_of_collection,
                 closure_date, created_by, created_date, modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                case_number, candidate['party_id'], objection_type, objection_subject_type,
                None, candidate['audit_case_id'], candidate['tax_type_code'],
                None,  # tax_period_id: NULL for audit-based objections
                filing_date, 'ONLINE_PORTAL',
                case_status, priority, DEFAULT_USER_ID, assignment_date if case_status != 'UNDER_REVIEW' else None,
                amount_disputed, amount_at_stake_tax, amount_at_stake_penalty,
                amount_at_stake_interest, "Objection to audit findings and assessment",
                taxpayer_requested_hearing,
                hearing_scheduled_date if case_status in ('HEARING_SCHEDULED', 'DECISION_ISSUED', 'CLOSED') else None,
                hearing_held_date if case_status in ('DECISION_ISSUED', 'CLOSED') else None,
                'Legal Representative Name' if random.random() > 0.5 else None,
                decision_date if case_status in ('DECISION_ISSUED', 'CLOSED') else None,
                decision_outcome if case_status in ('DECISION_ISSUED', 'CLOSED') else None,
                'Summary of decision' if decision_outcome else None,
                amount_adjusted if case_status in ('DECISION_ISSUED', 'CLOSED') else None,
                appeal_filed if case_status in ('DECISION_ISSUED', 'CLOSED') else None,
                decision_date + timedelta(days=30) if decision_date else None,
                random.choice([True, False]), closure_date,
                DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
            ))

            self.generated_counts['objection_cases'] += 1
            case_counter += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['objection_cases']} objection case(s)")

    def _generate_appeal_cases(self):
        """
        Generate appeal cases for objections where appeal_filed = TRUE.
        """
        logger.info("Generating appeal cases...")

        # Query objections that were appealed
        appeals_query = """
            SELECT
                objection_case_id,
                case_number,
                party_id,
                amount_disputed,
                decision_date,
                closure_date
            FROM compliance_control.objection_case
            WHERE appeal_filed = TRUE
            AND decision_date IS NOT NULL
        """

        objections = self.db.fetch_all(appeals_query)

        if not objections:
            logger.info("  No objections were appealed - skipping appeal generation")
            return

        appeal_counter = 1
        current_year = 2024

        for objection in objections:
            objection_case_id = objection[0]
            party_id = objection[2]
            amount_at_stake = objection[3]
            decision_date = objection[4]

            appeal_number = f"APP-{current_year}-{appeal_counter:04d}"

            # Filing date: shortly after objection decision
            filing_date = decision_date + timedelta(days=random.randint(15, 30))

            # Appeal level (Phase H: simplify to Appeals Board)
            appeal_level = 'APPEALS_BOARD'

            # Appeal status
            status_roll = random.random()
            if status_roll < 0.30:
                appeal_status = 'FILED'
                hearing_date = None
                decision_date_appeal = None
                decision_outcome = None
                amount_adjusted = None
            elif status_roll < 0.60:
                appeal_status = 'HEARING_SCHEDULED'
                hearing_date = filing_date + timedelta(days=random.randint(60, 120))
                decision_date_appeal = None
                decision_outcome = None
                amount_adjusted = None
            else:
                appeal_status = 'DECISION_ISSUED'
                hearing_date = filing_date + timedelta(days=random.randint(60, 120))
                decision_date_appeal = hearing_date + timedelta(days=random.randint(30, 60))
                decision_outcome = random.choice(['APPEAL_UPHELD', 'APPEAL_DISMISSED', 'REMANDED'])
                if decision_outcome == 'APPEAL_UPHELD':
                    # Taxpayer wins additional relief
                    amount_adjusted = self.round_decimal(amount_at_stake * Decimal(random.uniform(0.2, 0.4)))
                else:
                    amount_adjusted = Decimal('0.00')

            self.db.execute_query("""
                INSERT INTO compliance_control.appeal_case
                (appeal_number, objection_case_id, party_id, appeal_level_code,
                 appeal_status_code, filing_date, amount_at_stake, appeal_grounds,
                 hearing_date, decision_date, decision_outcome_code, decision_summary,
                 amount_adjusted, created_by, created_date, modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                appeal_number, objection_case_id, party_id, appeal_level,
                appeal_status, filing_date, amount_at_stake,
                "Grounds for appeal to higher level",
                hearing_date, decision_date_appeal, decision_outcome,
                'Summary of appeal decision' if decision_outcome else None,
                amount_adjusted, DEFAULT_USER_ID, datetime.now(),
                DEFAULT_USER_ID, datetime.now()
            ))

            self.generated_counts['appeal_cases'] += 1
            appeal_counter += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['appeal_cases']} appeal case(s)")

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("OBJECTION & APPEAL GENERATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Objection Cases:         {self.generated_counts['objection_cases']}")
        logger.info(f"Appeal Cases:            {self.generated_counts['appeal_cases']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for objection generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = ObjectionGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Objection generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
