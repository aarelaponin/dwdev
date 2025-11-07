#!/usr/bin/env python3
"""
Risk Assessment Generator for TA-RDM (Phase I).

Updates taxpayer risk profiles with calculated risk scores and metrics
derived from transactional data (filings, payments, audits, objections, collections).
"""

import logging
import random
from datetime import datetime, date
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_HALF_UP

from utils.db_utils import DatabaseConnection
from config.tax_config import DEFAULT_USER

logger = logging.getLogger(__name__)

DEFAULT_USER_ID = 1


class RiskAssessmentGenerator:
    """
    Generates and updates risk assessment data for TA-RDM database.
    """

    def __init__(self, db: DatabaseConnection, seed: int = 42):
        """
        Initialize risk assessment generator.

        Args:
            db: Database connection
            seed: Random seed for reproducibility
        """
        self.db = db
        self.seed = seed
        random.seed(seed)
        self.generated_counts = {
            'risk_profiles_updated': 0
        }

    @staticmethod
    def round_decimal(value: Decimal, places: int = 2) -> Decimal:
        """Round a Decimal to specified number of decimal places."""
        quantizer = Decimal(10) ** -places
        return value.quantize(quantizer, rounding=ROUND_HALF_UP)

    def generate_all(self):
        """
        Update all risk assessment data based on transactional data.
        """
        logger.info("=" * 80)
        logger.info("Starting Risk Assessment Update (Phase I)")
        logger.info("=" * 80)

        try:
            # Step 1: Get existing risk profiles
            risk_profiles = self._query_risk_profiles()
            logger.info(f"Found {len(risk_profiles)} risk profile(s)")

            if len(risk_profiles) == 0:
                logger.info("No risk profiles found - skipping risk assessment update")
                return

            # Step 2: Compute additional risk metrics from transactional data
            self._update_risk_metrics(risk_profiles)

            # Step 3: Summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("✓ Risk Assessment Update Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Risk assessment update failed: {e}", exc_info=True)
            raise

    def _query_risk_profiles(self) -> List[Dict]:
        """
        Query existing risk profiles.
        """
        query = """
            SELECT
                trp.taxpayer_risk_profile_id,
                trp.party_id,
                trp.overall_risk_score,
                trp.risk_rating_code,
                trp.filing_risk_score,
                trp.payment_risk_score,
                trp.accuracy_risk_score,
                trp.industry_risk_score,
                trp.complexity_risk_score,
                trp.late_filing_count_12m,
                trp.non_filing_count_12m,
                trp.late_payment_count_12m,
                trp.current_arrears_amount,
                trp.objection_count_history,
                trp.third_party_discrepancy_count
            FROM compliance_control.taxpayer_risk_profile trp
            ORDER BY trp.overall_risk_score DESC
        """

        rows = self.db.fetch_all(query)
        profiles = []

        for row in rows:
            profiles.append({
                'risk_profile_id': row[0],
                'party_id': row[1],
                'overall_risk_score': row[2],
                'risk_rating_code': row[3],
                'filing_risk_score': row[4],
                'payment_risk_score': row[5],
                'accuracy_risk_score': row[6],
                'industry_risk_score': row[7],
                'complexity_risk_score': row[8],
                'late_filing_count_12m': row[9],
                'non_filing_count_12m': row[10],
                'late_payment_count_12m': row[11],
                'current_arrears_amount': row[12],
                'objection_count_history': row[13],
                'third_party_discrepancy_count': row[14]
            })

        return profiles

    def _update_risk_metrics(self, profiles: List[Dict]):
        """
        Update risk profiles with calculated metrics from transactional data.
        """
        logger.info("Updating risk metrics from transactional data...")

        for profile in profiles:
            party_id = profile['party_id']

            # Count audit assessments
            audit_count_query = """
                SELECT COUNT(*)
                FROM compliance_control.audit_case
                WHERE party_id = %s
                AND total_amount_assessed > 0
            """
            audit_count = self.db.fetch_one(audit_count_query, (party_id,))
            audit_assessment_count = audit_count[0] if audit_count else 0

            # Count objections
            objection_count_query = """
                SELECT COUNT(*)
                FROM compliance_control.objection_case
                WHERE party_id = %s
            """
            objection_count = self.db.fetch_one(objection_count_query, (party_id,))
            objection_total_count = objection_count[0] if objection_count else 0

            # Calculate audit history risk score (based on audit adjustments)
            audit_history_query = """
                SELECT
                    IFNULL(SUM(total_amount_assessed), 0) as total_assessed,
                    IFNULL(AVG(total_amount_assessed), 0) as avg_assessed
                FROM compliance_control.audit_case
                WHERE party_id = %s
                AND case_status_code = 'FINALIZED'
            """
            audit_history = self.db.fetch_one(audit_history_query, (party_id,))
            total_assessed = audit_history[0] if audit_history else Decimal('0')

            # Audit history risk: higher assessments = higher risk
            if total_assessed > 50000:
                audit_history_risk_score = Decimal('75.00') + Decimal(random.uniform(0, 15))
            elif total_assessed > 10000:
                audit_history_risk_score = Decimal('50.00') + Decimal(random.uniform(0, 20))
            elif total_assessed > 0:
                audit_history_risk_score = Decimal('30.00') + Decimal(random.uniform(0, 15))
            else:
                audit_history_risk_score = Decimal('10.00') + Decimal(random.uniform(0, 10))

            # Reporting risk score (accuracy_risk_score from existing data)
            reporting_risk_score = profile['accuracy_risk_score'] if profile['accuracy_risk_score'] else Decimal('20.00')

            # Third party risk score (based on discrepancy count)
            third_party_count = profile['third_party_discrepancy_count'] if profile['third_party_discrepancy_count'] else 0
            if third_party_count > 5:
                third_party_risk_score = Decimal('70.00') + Decimal(random.uniform(0, 20))
            elif third_party_count > 2:
                third_party_risk_score = Decimal('40.00') + Decimal(random.uniform(0, 20))
            elif third_party_count > 0:
                third_party_risk_score = Decimal('20.00') + Decimal(random.uniform(0, 15))
            else:
                third_party_risk_score = Decimal('5.00') + Decimal(random.uniform(0, 10))

            # Determine if audit candidate (high risk + certain conditions)
            is_audit_candidate = (
                profile['overall_risk_score'] > 60 or
                profile['current_arrears_amount'] > 50000 or
                audit_assessment_count > 0
            )

            # Update the risk profile with calculated values
            self.db.execute_query("""
                UPDATE compliance_control.taxpayer_risk_profile
                SET
                    audit_adjustment_history = %s,
                    objection_count_history = %s,
                    profile_last_updated = %s,
                    modified_by = %s,
                    modified_date = %s
                WHERE party_id = %s
            """, (
                total_assessed,
                objection_total_count,
                datetime.now(),
                DEFAULT_USER_ID,
                datetime.now(),
                party_id
            ))

            logger.info(f"  Updated risk profile for party {party_id}: "
                       f"audits={audit_assessment_count}, objections={objection_total_count}, "
                       f"audit_candidate={is_audit_candidate}")

            self.generated_counts['risk_profiles_updated'] += 1

        self.db.commit()
        logger.info(f"  Updated {self.generated_counts['risk_profiles_updated']} risk profile(s)")

    def _print_summary(self):
        """Print generation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("RISK ASSESSMENT UPDATE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Risk Profiles Updated:   {self.generated_counts['risk_profiles_updated']}")
        logger.info("=" * 80 + "\n")


def main():
    """Main entry point for risk assessment generator."""
    from utils.db_utils import get_db_connection

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:
        with get_db_connection() as db:
            generator = RiskAssessmentGenerator(db, seed=42)
            generator.generate_all()
            return 0
    except Exception as e:
        logger.error(f"Risk assessment update failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
