"""
Audit Generator for TA-RDM (Phase G).

Generates audit plans, audit cases, and audit findings for tax compliance audits.
Builds on existing taxpayer and filing data to create realistic audit scenarios.
"""

import logging
import random
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER


logger = logging.getLogger(__name__)

# Audit tables use BIGINT for user IDs
DEFAULT_USER_ID = 1


class AuditGenerator:
    """
    Generates audit data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize audit generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'audit_plans': 0,
            'audit_cases': 0,
            'audit_findings': 0,
            'audit_case_officers': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Generate all audit data in dependency order.
        """
        logger.info("=" * 80)
        logger.info("Starting Audit Generation (Phase G)")
        logger.info("=" * 80)

        try:
            # Temporarily disable FK checks due to DDL bug
            logger.info("Temporarily disabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            self.db.commit()

            # Step 1: Generate audit plan
            audit_plan_id = self._generate_audit_plan()

            # Step 2: Query taxpayers for audit selection
            audit_candidates = self._query_audit_candidates()
            logger.info(f"Found {len(audit_candidates)} audit candidate(s)")

            if len(audit_candidates) == 0:
                logger.info("No audit candidates found - skipping audit generation")
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
                return

            # Step 3: Generate audit cases
            self._generate_audit_cases(audit_plan_id, audit_candidates)

            # Step 4: Generate audit findings for completed cases
            self._generate_audit_findings()

            # Step 5: Assign officers to cases
            self._generate_audit_case_officers()

            # Re-enable FK checks
            logger.info("Re-enabling FK checks...")
            self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            self.db.commit()

            # Step 6: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Audit Generation Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Audit generation failed: {e}", exc_info=True)
            # Try to re-enable FK checks even on error
            try:
                self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
                self.db.commit()
            except:
                pass
            raise

    def _generate_audit_plan(self) -> int:
        """
        Generate audit plan for 2024.
        Returns the audit_plan_id.
        """
        logger.info("Generating audit plan...")

        # Check if plan already exists
        existing = self.db.fetch_one("SELECT COUNT(*) FROM compliance_control.audit_plan")
        if existing and existing[0] > 0:
            logger.info(f"  Deleting {existing[0]} existing audit plan(s)")
            self.db.execute_query("DELETE FROM compliance_control.audit_finding")
            self.db.execute_query("DELETE FROM compliance_control.audit_case_officer")
            self.db.execute_query("DELETE FROM compliance_control.audit_case")
            self.db.execute_query("DELETE FROM compliance_control.audit_plan")
            self.db.commit()

        # Create 2024 audit plan
        plan_code = "AP-2024-001"
        plan_name = "2024 Annual Compliance Audit Program"
        plan_type = "ANNUAL"
        plan_year = 2024
        plan_status = "ACTIVE"
        target_audit_count = 15
        target_revenue_impact = Decimal('500000.00')
        allocated_resources = 5

        self.db.execute_query("""
            INSERT INTO compliance_control.audit_plan
            (plan_code, plan_name, plan_type_code, plan_period_year, plan_period_quarter,
             plan_status_code, target_audit_count, target_revenue_impact,
             allocated_resources_count, approved_by, approval_date, plan_description,
             created_by, created_date, modified_by, modified_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            plan_code, plan_name, plan_type, plan_year, None,
            plan_status, target_audit_count, target_revenue_impact,
            allocated_resources, DEFAULT_USER_ID,
            date(2024, 1, 15), "Annual risk-based audit program targeting high-risk taxpayers",
            DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
        ))

        audit_plan_id = self.db.get_last_insert_id()
        self.generated_counts['audit_plans'] += 1
        self.db.commit()

        logger.info(f"  Generated audit plan: {plan_code}")
        return audit_plan_id

    def _query_audit_candidates(self) -> List[Dict]:
        """
        Query taxpayers suitable for audit.
        Select based on filing history and assessment amounts.
        """
        query = """
            SELECT DISTINCT
                ta.tax_account_id,
                ta.party_id,
                ta.tax_type_code,
                COUNT(DISTINCT tr.tax_return_id) as return_count,
                SUM(a.net_assessment_amount) as total_assessments
            FROM registration.tax_account ta
            INNER JOIN filing_assessment.tax_return tr ON ta.tax_account_id = tr.tax_account_id
            INNER JOIN filing_assessment.assessment a ON tr.tax_return_id = a.tax_return_id
            WHERE ta.account_status_code = 'ACTIVE'
            AND a.is_current_version = 1
            GROUP BY ta.tax_account_id, ta.party_id, ta.tax_type_code
            HAVING return_count >= 2
            ORDER BY total_assessments DESC
            LIMIT 12
        """

        rows = self.db.fetch_all(query)
        candidates = []

        for idx, row in enumerate(rows):
            # Calculate simple risk score based on total assessments (higher = higher risk)
            # Top candidates get higher risk scores
            risk_score = Decimal('85.00') - (idx * Decimal('5.00'))
            risk_score = max(risk_score, Decimal('40.00'))

            candidates.append({
                'tax_account_id': row[0],
                'party_id': row[1],
                'tax_type_code': row[2],
                'return_count': row[3],
                'total_assessments': row[4],
                'risk_score': risk_score
            })

        return candidates

    def _generate_audit_cases(self, audit_plan_id: int, candidates: List[Dict]):
        """
        Generate audit cases for selected candidates.
        """
        logger.info("Generating audit cases...")

        case_counter = 1
        current_year = 2024

        for candidate in candidates:
            case_number = f"AUD-{current_year}-{case_counter:04d}"

            # Audit characteristics based on risk
            risk_score = float(candidate['risk_score'])

            if risk_score >= 70:
                audit_type = 'COMPREHENSIVE'
                audit_scope = 'FULL_AUDIT'
                priority = 'HIGH'
                selection_method = 'RISK_BASED'
            elif risk_score >= 50:
                audit_type = 'DESK_AUDIT'
                audit_scope = 'PARTIAL_AUDIT'
                priority = 'MEDIUM'
                selection_method = 'RISK_BASED'
            else:
                audit_type = 'DESK_AUDIT'
                audit_scope = 'SINGLE_ISSUE'
                priority = 'LOW'
                selection_method = 'RANDOM'

            # Audit periods (audit last 2 years)
            audit_period_from = date(2023, 1, 1)
            audit_period_to = date(2024, 12, 31)

            # Case timeline with different statuses
            assignment_date = date(2024, 2, 1) + timedelta(days=random.randint(0, 30))
            notification_date = assignment_date + timedelta(days=random.randint(5, 15))
            planned_start_date = notification_date + timedelta(days=random.randint(7, 20))

            # Status progression
            status_roll = random.random()
            if status_roll < 0.15:
                case_status = 'ASSIGNED'
                actual_start_date = None
                planned_completion_date = planned_start_date + timedelta(days=random.randint(30, 90))
                actual_completion_date = None
                adjustment_amount = None
                penalty_amount = None
                interest_amount = None
                total_amount_assessed = None
                taxpayer_agreed = None
                objection_filed = None
            elif status_roll < 0.30:
                case_status = 'IN_PROGRESS'
                actual_start_date = planned_start_date
                planned_completion_date = planned_start_date + timedelta(days=random.randint(30, 90))
                actual_completion_date = None
                adjustment_amount = None
                penalty_amount = None
                interest_amount = None
                total_amount_assessed = None
                taxpayer_agreed = None
                objection_filed = None
            elif status_roll < 0.50:
                case_status = 'FINDINGS_REVIEW'
                actual_start_date = planned_start_date
                planned_completion_date = planned_start_date + timedelta(days=random.randint(30, 90))
                actual_completion_date = None
                # Estimate adjustments
                adjustment_amount = self.round_decimal(
                    candidate['total_assessments'] * Decimal(random.uniform(0.05, 0.20))
                )
                penalty_amount = self.round_decimal(adjustment_amount * Decimal('0.15'))
                interest_amount = self.round_decimal(adjustment_amount * Decimal('0.08'))
                total_amount_assessed = adjustment_amount + penalty_amount + interest_amount
                taxpayer_agreed = None
                objection_filed = None
            else:
                case_status = 'FINALIZED'
                actual_start_date = planned_start_date
                planned_completion_date = planned_start_date + timedelta(days=random.randint(30, 90))
                actual_completion_date = planned_start_date + timedelta(days=random.randint(20, 80))
                # Final adjustments
                adjustment_amount = self.round_decimal(
                    candidate['total_assessments'] * Decimal(random.uniform(0.05, 0.20))
                )
                penalty_amount = self.round_decimal(adjustment_amount * Decimal('0.15'))
                interest_amount = self.round_decimal(adjustment_amount * Decimal('0.08'))
                total_amount_assessed = adjustment_amount + penalty_amount + interest_amount
                taxpayer_agreed = random.choice([True, False])
                objection_filed = not taxpayer_agreed if random.random() > 0.3 else False

            # Estimated hours
            if audit_type == 'COMPREHENSIVE':
                estimated_hours = Decimal(random.uniform(80, 200))
                actual_hours = Decimal(random.uniform(75, 210)) if actual_completion_date else None
            elif audit_type == 'FIELD_AUDIT':
                estimated_hours = Decimal(random.uniform(50, 120))
                actual_hours = Decimal(random.uniform(45, 130)) if actual_completion_date else None
            else:
                estimated_hours = Decimal(random.uniform(20, 60))
                actual_hours = Decimal(random.uniform(18, 65)) if actual_completion_date else None

            if actual_hours:
                actual_hours = self.round_decimal(actual_hours, 1)
            estimated_hours = self.round_decimal(estimated_hours, 1)

            self.db.execute_query("""
                INSERT INTO compliance_control.audit_case
                (case_number, audit_plan_id, party_id, tax_account_id, tax_type_code,
                 audit_type_code, audit_scope_code, selection_method_code, risk_score,
                 audit_period_from, audit_period_to, case_status_code, priority_code,
                 assigned_officer_id, assignment_date, notification_date,
                 planned_start_date, actual_start_date, planned_completion_date,
                 actual_completion_date, audit_location_code, estimated_hours,
                 actual_hours, adjustment_amount, penalty_amount, interest_amount,
                 total_amount_assessed, taxpayer_agreed, objection_filed,
                 case_summary, closure_reason_code,
                 created_by, created_date, modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                case_number, audit_plan_id, candidate['party_id'], candidate['tax_account_id'],
                candidate['tax_type_code'], audit_type, audit_scope, selection_method,
                candidate['risk_score'], audit_period_from, audit_period_to, case_status,
                priority, DEFAULT_USER_ID, assignment_date, notification_date,
                planned_start_date, actual_start_date, planned_completion_date,
                actual_completion_date, 'OFFICE_DESK', estimated_hours, actual_hours,
                adjustment_amount, penalty_amount, interest_amount, total_amount_assessed,
                taxpayer_agreed, objection_filed, f"Audit case for {candidate['tax_type_code']}",
                None, DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
            ))

            self.generated_counts['audit_cases'] += 1
            case_counter += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['audit_cases']} audit case(s)")

    def _generate_audit_findings(self):
        """
        Generate audit findings for cases with adjustments (FINDINGS_REVIEW or FINALIZED status).
        """
        logger.info("Generating audit findings...")

        # Query cases with adjustments
        cases_query = """
            SELECT
                audit_case_id,
                case_number,
                party_id,
                adjustment_amount,
                penalty_amount,
                interest_amount
            FROM compliance_control.audit_case
            WHERE case_status_code IN ('FINDINGS_REVIEW', 'FINALIZED')
            AND adjustment_amount IS NOT NULL
        """

        cases = self.db.fetch_all(cases_query)

        for case in cases:
            audit_case_id = case[0]
            adjustment_amount = case[3]

            # Generate 1-3 findings per case
            num_findings = random.randint(1, 3)

            for i in range(num_findings):
                finding_number = f"F{i+1:02d}"

                # Finding categories
                finding_category = random.choice([
                    'UNREPORTED_INCOME',
                    'OVERSTATED_EXPENSE',
                    'CALCULATION_ERROR',
                    'MISCLASSIFICATION'
                ])

                severity = random.choice(['MODERATE', 'MAJOR', 'CRITICAL'])

                # Split adjustment across findings
                if i == 0:
                    tax_adjustment = self.round_decimal(adjustment_amount * Decimal('0.5'))
                elif i == 1:
                    tax_adjustment = self.round_decimal(adjustment_amount * Decimal('0.3'))
                else:
                    tax_adjustment = self.round_decimal(adjustment_amount * Decimal('0.2'))

                penalty_applicable = True
                penalty_rate = Decimal('15.00')
                penalty_amount = self.round_decimal(tax_adjustment * penalty_rate / 100)
                interest_amount = self.round_decimal(tax_adjustment * Decimal('0.08'))

                taxpayer_accepted = random.choice([True, False])

                self.db.execute_query("""
                    INSERT INTO compliance_control.audit_finding
                    (audit_case_id, finding_number, tax_period_id, finding_category_code,
                     severity_code, finding_description, legal_basis, taxpayer_position,
                     auditor_position, tax_base_adjustment, tax_adjustment,
                     penalty_applicable, penalty_rate, penalty_amount, interest_amount,
                     taxpayer_response, taxpayer_accepted, adjustment_posted, assessment_id,
                     created_by, created_date, modified_by, modified_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    audit_case_id, finding_number, None, finding_category, severity,
                    f"Finding: {finding_category}",
                    "Relevant tax code section",
                    "Taxpayer treatment of issue",
                    "Correct treatment per auditor",
                    None, tax_adjustment, penalty_applicable, penalty_rate,
                    penalty_amount, interest_amount, "Taxpayer response to finding",
                    taxpayer_accepted, False, None,
                    DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
                ))

                self.generated_counts['audit_findings'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['audit_findings']} audit finding(s)")

    def _generate_audit_case_officers(self):
        """
        Assign lead auditor to each audit case.
        Phase G simplification: Single officer per case.
        """
        logger.info("Generating audit case officer assignments...")

        # Query all audit cases
        cases = self.db.fetch_all("SELECT audit_case_id, assignment_date FROM compliance_control.audit_case")

        for case in cases:
            audit_case_id = case[0]
            assignment_date = case[1]

            # Assign lead auditor
            self.db.execute_query("""
                INSERT INTO compliance_control.audit_case_officer
                (audit_case_id, officer_id, role_code, assignment_start_date,
                 assignment_end_date, hours_spent, created_by, created_date,
                 modified_by, modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                audit_case_id, DEFAULT_USER_ID, 'LEAD_AUDITOR', assignment_date,
                None, None, DEFAULT_USER_ID, datetime.now(), DEFAULT_USER_ID, datetime.now()
            ))

            self.generated_counts['audit_case_officers'] += 1

        self.db.commit()
        logger.info(f"  Generated {self.generated_counts['audit_case_officers']} audit case officer assignment(s)")

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("AUDIT GENERATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Audit Plans:             {self.generated_counts['audit_plans']}")
        logger.info(f"Audit Cases:             {self.generated_counts['audit_cases']}")
        logger.info(f"Audit Findings:          {self.generated_counts['audit_findings']}")
        logger.info(f"Case Officer Assignments:{self.generated_counts['audit_case_officers']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for audit generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = AuditGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Audit generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
