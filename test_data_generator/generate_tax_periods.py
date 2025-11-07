#!/usr/bin/env python3
"""
Generate tax period test data for Phase C.

This script generates tax periods for all tax types based on their
filing frequency (monthly, quarterly, annual) for years 2023-2025.
"""

import logging
import sys

from utils.db_utils import get_db_connection
from generators.tax_framework_generator import TaxFrameworkGenerator

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
        logger.info("PHASE C: TAX PERIOD GENERATION")
        logger.info("=" * 80)

        with get_db_connection() as db:
            # Initialize generator
            generator = TaxFrameworkGenerator(db)

            # Generate tax periods for 2023-2025
            total_periods = generator.generate_tax_periods(
                start_year=2023,
                end_year=2025
            )

            # Commit the transaction
            db.commit()

            logger.info("=" * 80)
            logger.info(f"SUCCESS: Generated {total_periods} tax periods")
            logger.info("=" * 80)

            # Show breakdown by tax type
            periods_by_type = db.fetch_all("""
                SELECT
                    tt.tax_type_code,
                    tt.tax_type_name,
                    tt.default_filing_frequency,
                    COUNT(*) as period_count,
                    MIN(tp.period_start_date) as first_period,
                    MAX(tp.period_end_date) as last_period
                FROM tax_framework.tax_period tp
                INNER JOIN tax_framework.tax_type tt ON tp.tax_type_id = tt.tax_type_id
                GROUP BY tt.tax_type_code, tt.tax_type_name, tt.default_filing_frequency
                ORDER BY tt.tax_type_code
            """)

            logger.info("\nPeriod Breakdown by Tax Type:")
            logger.info("-" * 80)
            for row in periods_by_type:
                code, name, freq, count, first, last = row
                logger.info(f"  {code:<6} {name:<30} {freq:<10} "
                          f"Periods: {count:>3}  ({first} to {last})")

        return 0

    except Exception as e:
        logger.error(f"Error generating tax periods: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
