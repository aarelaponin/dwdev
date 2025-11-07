"""
Referential Integrity Validator

Validates foreign key relationships and data consistency between tables.
"""

import logging
from typing import List

from .base_validator import BaseValidator, ValidationResult
from .mapping_config import ETL_MAPPINGS


logger = logging.getLogger(__name__)


class IntegrityValidator(BaseValidator):
    """Validates referential integrity across L3 tables."""

    def validate(self, table_filter: str = None) -> List[ValidationResult]:
        """
        Run referential integrity validation checks.

        Args:
            table_filter: Optional table name to validate

        Returns:
            List of validation results
        """
        logger.info("Running Referential Integrity Validation...")

        # Check dimension foreign keys in facts
        self._validate_dimension_foreign_keys(table_filter)

        # Check natural key uniqueness
        self._validate_natural_key_uniqueness(table_filter)

        return self.results

    def _validate_dimension_foreign_keys(self, table_filter: str = None):
        """Verify all fact dimension keys exist in dimensions."""

        for fact_name, mapping in ETL_MAPPINGS.items():
            # Only check fact tables
            if not fact_name.startswith('fact_'):
                continue

            # Apply table filter
            if table_filter and fact_name != table_filter:
                continue

            dim_fks = mapping.get('dimension_fks', {})

            for fk_column, dim_table in dim_fks.items():
                dim_mapping = ETL_MAPPINGS.get(dim_table)
                if not dim_mapping:
                    continue

                # Check for orphaned fact records
                query = f"""
                    SELECT COUNT(*) as orphan_count
                    FROM {mapping['l3_target']} f
                    WHERE f.{fk_column} NOT IN (
                        SELECT {dim_mapping['surrogate_key']}
                        FROM {dim_mapping['l3_target']}
                    )
                    AND f.{fk_column} != 0  -- Exclude placeholder keys
                """

                try:
                    result = self.clickhouse_client.query(query)
                    orphan_count = result.result_rows[0][0] if result.result_rows else 0

                    status = 'PASS' if orphan_count == 0 else 'FAIL'

                    self.add_result(
                        check_name=f'FK Integrity: {fact_name}.{fk_column} → {dim_table}',
                        status=status,
                        details={
                            'fact_table': fact_name,
                            'dimension_table': dim_table,
                            'fk_column': fk_column,
                            'orphan_records': orphan_count
                        }
                    )

                    logger.info(f"  FK {fact_name}.{fk_column} → {dim_table}: {orphan_count} orphans [{status}]")

                except Exception as e:
                    logger.error(f"Error checking FK {fk_column}: {e}")
                    self.add_result(
                        check_name=f'FK Integrity: {fact_name}.{fk_column} → {dim_table}',
                        status='FAIL',
                        details={'error': str(e)}
                    )

    def _validate_natural_key_uniqueness(self, table_filter: str = None):
        """Check for duplicate natural keys in current dimensions."""

        for table_name, mapping in ETL_MAPPINGS.items():
            # Only check dimensions
            if not table_name.startswith('dim_'):
                continue

            # Apply table filter
            if table_filter and table_name != table_filter:
                continue

            natural_key = mapping.get('natural_key')
            if not natural_key:
                continue

            # For SCD Type 2, check uniqueness of natural key where is_current=1
            # For regular dimensions, check overall uniqueness
            if 'is_current' in mapping.get('mandatory_fields', []) or table_name == 'dim_party':
                # SCD Type 2
                query = f"""
                    SELECT {natural_key}, COUNT(*) as dup_count
                    FROM {mapping['l3_target']}
                    WHERE is_current = 1
                    GROUP BY {natural_key}
                    HAVING COUNT(*) > 1
                """
            else:
                # Regular dimension
                query = f"""
                    SELECT {natural_key}, COUNT(*) as dup_count
                    FROM {mapping['l3_target']}
                    GROUP BY {natural_key}
                    HAVING COUNT(*) > 1
                """

            try:
                result = self.clickhouse_client.query(query)
                duplicate_count = len(result.result_rows)

                status = 'PASS' if duplicate_count == 0 else 'FAIL'

                self.add_result(
                    check_name=f'Natural Key Uniqueness: {table_name}.{natural_key}',
                    status=status,
                    details={
                        'table': table_name,
                        'natural_key': natural_key,
                        'duplicate_keys': duplicate_count
                    }
                )

                logger.info(f"  {table_name}.{natural_key}: {duplicate_count} duplicates [{status}]")

            except Exception as e:
                logger.error(f"Error checking natural key uniqueness for {table_name}: {e}")
