"""
Business Rule Validator

Validates business logic rules such as SCD Type 2 consistency,
date ranges, and calculated fields.
"""

import logging
from typing import List

from .base_validator import BaseValidator, ValidationResult


logger = logging.getLogger(__name__)


class BusinessValidator(BaseValidator):
    """Validates business rules in L3 tables."""

    def validate(self, table_filter: str = None) -> List[ValidationResult]:
        """
        Run business rule validation checks.

        Args:
            table_filter: Optional table name to validate

        Returns:
            List of validation results
        """
        logger.info("Running Business Rule Validation...")

        # SCD Type 2 validation
        self._validate_scd_type2_consistency()

        # Date range validation
        self._validate_registration_dates()

        return self.results

    def _validate_scd_type2_consistency(self):
        """Validate SCD Type 2 consistency rules for dim_party."""

        # Rule 1: is_current=1 must have valid_to=NULL
        query1 = """
            SELECT COUNT(*) as violation_count
            FROM ta_dw.dim_party
            WHERE is_current = 1 AND valid_to IS NOT NULL
        """

        try:
            result = self.clickhouse_client.query(query1)
            violation_count = result.result_rows[0][0] if result.result_rows else 0

            status = 'PASS' if violation_count == 0 else 'FAIL'

            self.add_result(
                check_name='SCD Type 2: is_current=1 → valid_to IS NULL',
                status=status,
                details={
                    'violation_count': violation_count,
                    'rule': 'Current records must have NULL valid_to'
                }
            )

            logger.info(f"  SCD Type 2 rule 1: {violation_count} violations [{status}]")

        except Exception as e:
            logger.error(f"Error validating SCD Type 2 rule 1: {e}")

        # Rule 2: is_current=0 must have valid_to NOT NULL
        query2 = """
            SELECT COUNT(*) as violation_count
            FROM ta_dw.dim_party
            WHERE is_current = 0 AND valid_to IS NULL
        """

        try:
            result = self.clickhouse_client.query(query2)
            violation_count = result.result_rows[0][0] if result.result_rows else 0

            status = 'PASS' if violation_count == 0 else 'FAIL'

            self.add_result(
                check_name='SCD Type 2: is_current=0 → valid_to IS NOT NULL',
                status=status,
                details={
                    'violation_count': violation_count,
                    'rule': 'Historical records must have valid_to date'
                }
            )

            logger.info(f"  SCD Type 2 rule 2: {violation_count} violations [{status}]")

        except Exception as e:
            logger.error(f"Error validating SCD Type 2 rule 2: {e}")

    def _validate_registration_dates(self):
        """Validate registration dates are within dim_time range."""

        query = """
            SELECT COUNT(*) as invalid_count
            FROM ta_dw.fact_registration f
            WHERE f.dim_date_key NOT IN (
                SELECT date_key FROM ta_dw.dim_time
            )
        """

        try:
            result = self.clickhouse_client.query(query)
            invalid_count = result.result_rows[0][0] if result.result_rows else 0

            status = 'PASS' if invalid_count == 0 else 'FAIL'

            self.add_result(
                check_name='Business Rule: Registration Dates in dim_time Range',
                status=status,
                details={
                    'invalid_date_count': invalid_count,
                    'rule': 'All registration dates must exist in dim_time'
                }
            )

            logger.info(f"  Registration date range: {invalid_count} invalid [{status}]")

        except Exception as e:
            logger.error(f"Error validating registration dates: {e}")
