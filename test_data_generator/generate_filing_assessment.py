#!/usr/bin/env python3
"""
Generate filing & assessment test data for Phase C.

This script generates tax returns, assessments, penalties, and interest
for registered taxpayers across all tax periods.
"""

import logging
import sys

from utils.db_utils import get_db_connection
from generators.filing_assessment_generator import FilingAssessmentGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    try:
        logger.info("=" * 80)
        logger.info("PHASE C: FILING & ASSESSMENT GENERATION")
        logger.info("=" * 80)

        with get_db_connection() as db:
            # Initialize generator
            generator = FilingAssessmentGenerator(db)

            # Generate all filing and assessment data
            generator.generate_all()

            # Commit the transaction
            db.commit()

            logger.info("=" * 80)
            logger.info("SUCCESS: Filing & Assessment data generated")
            logger.info("=" * 80)

            # Show summary statistics
            logger.info("\nDatabase Summary:")
            logger.info("-" * 80)

            # Tax returns by tax type
            returns_by_type = db.fetch_all("""
                SELECT
                    ta.tax_type_code,
                    COUNT(*) as return_count,
                    SUM(CASE WHEN tr.is_filing_late = 1 THEN 1 ELSE 0 END) as late_count,
                    SUM(CASE WHEN tr.is_flagged_for_review = 1 THEN 1 ELSE 0 END) as flagged_count
                FROM filing_assessment.tax_return tr
                INNER JOIN registration.tax_account ta ON tr.tax_account_id = ta.tax_account_id
                GROUP BY ta.tax_type_code
                ORDER BY ta.tax_type_code
            """)

            logger.info("Tax Returns by Type:")
            for code, total, late, flagged in returns_by_type:
                logger.info(f"  {code:<6} Total: {total:>3}  Late: {late:>3}  Flagged: {flagged:>3}")

            # Assessment summary
            assessment_summary = db.fetch_one("""
                SELECT
                    COUNT(*) as total_assessments,
                    SUM(assessed_tax_amount) as total_tax,
                    SUM(penalty_amount) as total_penalties,
                    SUM(interest_amount) as total_interest,
                    SUM(net_assessment_amount) as total_net,
                    SUM(amount_paid) as total_paid,
                    SUM(balance_outstanding) as total_outstanding
                FROM filing_assessment.assessment
            """)

            logger.info("\nAssessment Summary:")
            logger.info(f"  Total Assessments: {assessment_summary[0]}")
            logger.info(f"  Total Tax: ${assessment_summary[1]:,.2f}")
            logger.info(f"  Total Penalties: ${assessment_summary[2]:,.2f}")
            logger.info(f"  Total Interest: ${assessment_summary[3]:,.2f}")
            logger.info(f"  Net Assessment: ${assessment_summary[4]:,.2f}")
            logger.info(f"  Amount Paid: ${assessment_summary[5]:,.2f}")
            logger.info(f"  Balance Outstanding: ${assessment_summary[6]:,.2f}")

        return 0

    except Exception as e:
        logger.error(f"Error generating filing/assessment data: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
