#!/usr/bin/env python3
"""
Generate tax framework data for Phase B.

Usage:
    python generate_tax_framework.py
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
    """Main entry point for tax framework data generation."""
    try:
        logger.info("Starting Tax Framework Data Generation (Phase B)")

        with get_db_connection() as db:
            generator = TaxFrameworkGenerator(db, seed=42)
            generator.generate_all(include_registrations=True)
            db.commit()

        logger.info("Tax framework data generation completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Tax framework data generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
