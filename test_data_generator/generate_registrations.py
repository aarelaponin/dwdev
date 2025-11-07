#!/usr/bin/env python3
"""
Generate tax account registrations for Phase B.

Usage:
    python generate_registrations.py
"""

import sys
import logging
from utils.db_utils import get_db_connection
from generators.tax_framework_generator import TaxFrameworkGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point for tax account registration generation."""
    try:
        logger.info("Starting Tax Account Registration Generation (Phase B)")

        with get_db_connection() as db:
            generator = TaxFrameworkGenerator(db, seed=42)
            generator.generate_tax_account_registrations()
            db.commit()

        logger.info("Tax account registration generation completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Tax account registration generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
