"""
Data Quality Validator

Validates data quality rules including NULL checks, data types, and value ranges.
"""

import logging
from typing import List

from .base_validator import BaseValidator, ValidationResult
from .mapping_config import ETL_MAPPINGS


logger = logging.getLogger(__name__)


class QualityValidator(BaseValidator):
    """Validates data quality in L3 tables."""

    def validate(self, table_filter: str = None) -> List[ValidationResult]:
        """
        Run data quality validation checks.

        Args:
            table_filter: Optional table name to validate

        Returns:
            List of validation results
        """
        logger.info("Running Data Quality Validation...")

        # Check mandatory fields for NULLs
        self._validate_mandatory_fields(table_filter)

        # Check specific data quality rules
        self._validate_party_tin_format()
        self._validate_risk_scores()

        return self.results

    def _validate_mandatory_fields(self, table_filter: str = None):
        """Check for NULL or empty values in mandatory fields."""

        for table_name, mapping in ETL_MAPPINGS.items():
            # Apply table filter
            if table_filter and table_name != table_filter:
                continue

            mandatory_fields = mapping.get('mandatory_fields', [])

            for field in mandatory_fields:
                query = f"""
                    SELECT COUNT(*) as null_count
                    FROM {mapping['l3_target']}
                    WHERE {field} IS NULL
                       OR CAST({field} AS String) = ''
                """

                try:
                    result = self.clickhouse_client.query(query)
                    null_count = result.result_rows[0][0] if result.result_rows else 0

                    status = 'PASS' if null_count == 0 else 'FAIL'

                    self.add_result(
                        check_name=f'Mandatory Field: {table_name}.{field}',
                        status=status,
                        details={
                            'table': table_name,
                            'field': field,
                            'null_or_empty_count': null_count
                        }
                    )

                    if null_count > 0:
                        logger.warning(f"  {table_name}.{field}: {null_count} NULL/empty values")

                except Exception as e:
                    logger.error(f"Error checking mandatory field {table_name}.{field}: {e}")

    def _validate_party_tin_format(self):
        """Validate TIN format for parties."""

        query = """
            SELECT COUNT(*) as invalid_count
            FROM ta_dw.dim_party
            WHERE is_current = 1
              AND (
                  tin IS NULL
                  OR length(tin) NOT IN (9, 10, 12)  -- Valid TIN lengths
                  OR tin = ''
              )
        """

        try:
            result = self.clickhouse_client.query(query)
            invalid_count = result.result_rows[0][0] if result.result_rows else 0

            status = 'PASS' if invalid_count == 0 else 'FAIL'

            self.add_result(
                check_name='Data Quality: Party TIN Format',
                status=status,
                details={
                    'invalid_tin_count': invalid_count,
                    'rule': 'TIN must be 9, 10, or 12 characters'
                }
            )

            logger.info(f"  Party TIN format: {invalid_count} invalid [{status}]")

        except Exception as e:
            logger.error(f"Error validating TIN format: {e}")

    def _validate_risk_scores(self):
        """Validate risk scores are in valid range (0-100)."""

        query = """
            SELECT COUNT(*) as invalid_count
            FROM ta_dw.dim_party
            WHERE is_current = 1
              AND (
                  risk_score < 0
                  OR risk_score > 100
              )
        """

        try:
            result = self.clickhouse_client.query(query)
            invalid_count = result.result_rows[0][0] if result.result_rows else 0

            status = 'PASS' if invalid_count == 0 else 'FAIL'

            self.add_result(
                check_name='Data Quality: Risk Score Range',
                status=status,
                details={
                    'invalid_score_count': invalid_count,
                    'rule': 'Risk score must be between 0 and 100'
                }
            )

            logger.info(f"  Risk score range: {invalid_count} invalid [{status}]")

        except Exception as e:
            logger.error(f"Error validating risk scores: {e}")
