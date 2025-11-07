#!/usr/bin/env python3
"""
Generate tax form test data for Phase C.

This script generates minimal tax forms required for filing/assessment generation.
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
        logger.info("PHASE C: TAX FORM GENERATION")
        logger.info("=" * 80)

        with get_db_connection() as db:
            # Initialize generator
            generator = TaxFrameworkGenerator(db)

            # Generate tax forms
            generator.generate_tax_forms()

            # Commit the transaction
            db.commit()

            logger.info("=" * 80)
            logger.info("SUCCESS: Tax forms generated")
            logger.info("=" * 80)

            # Show summary
            forms = db.fetch_all("""
                SELECT tf.form_code, tf.form_name, tt.tax_type_code
                FROM tax_framework.tax_form tf
                INNER JOIN tax_framework.tax_type tt ON tf.tax_type_id = tt.tax_type_id
                ORDER BY tf.tax_form_id
            """)

            logger.info("\nGenerated Tax Forms:")
            logger.info("-" * 80)
            for code, name, tax_type in forms:
                logger.info(f"  {code:<20} {name:<40} ({tax_type})")

        return 0

    except Exception as e:
        logger.error(f"Error generating tax forms: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
