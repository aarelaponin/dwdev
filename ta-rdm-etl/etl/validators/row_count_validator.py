"""
Row Count Validator

Validates row counts between L2 MySQL source tables and L3 ClickHouse target tables.
This is the primary validation to ensure no data loss during ETL.
"""

import logging
from typing import Dict, List, Any

from .base_validator import BaseValidator, ValidationResult
from .mapping_config import ETL_MAPPINGS


logger = logging.getLogger(__name__)


class RowCountValidator(BaseValidator):
    """Validates row counts between L2 input and L3 output."""

    def validate(self, table_filter: str = None) -> List[ValidationResult]:
        """
        Run row count validation checks.

        Args:
            table_filter: Optional table name to validate (validates all if None)

        Returns:
            List of validation results
        """
        logger.info("Running Row Count Validation...")

        for table_name, mapping in ETL_MAPPINGS.items():
            # Apply table filter if specified
            if table_filter and table_name != table_filter:
                continue

            # Get counts
            l2_count = self._get_l2_source_count(table_name, mapping)
            l3_count = self._get_l3_target_count(table_name, mapping)

            # Calculate expected count
            if 'expected_count' in mapping:
                # For generated dimensions like dim_time
                expected_l3_count = mapping['expected_count']
            else:
                # For L2-sourced tables
                expected_l3_count = int(l2_count * mapping.get('expected_ratio', 1.0))

            # Determine status
            if l3_count == expected_l3_count:
                status = 'PASS'
            elif abs(l3_count - expected_l3_count) <= 1:
                # Allow 1 row difference as warning (for rounding, filtering edge cases)
                status = 'WARNING'
            else:
                status = 'FAIL'

            # Calculate difference and ratio
            difference = l3_count - expected_l3_count
            ratio = l3_count / l2_count if l2_count > 0 else 0

            # Add result
            self.add_result(
                check_name=f'Row Count: {table_name}',
                status=status,
                details={
                    'table': table_name,
                    'l2_source_count': l2_count,
                    'l3_target_count': l3_count,
                    'expected_count': expected_l3_count,
                    'difference': difference,
                    'ratio': round(ratio, 4),
                    'description': mapping.get('description', '')
                }
            )

            logger.info(f"  {table_name}: L2={l2_count} → L3={l3_count} (Expected={expected_l3_count}) [{status}]")

        return self.results

    def _get_l2_source_count(self, table_name: str, mapping: Dict[str, Any]) -> int:
        """
        Get row count from L2 MySQL source tables.

        Args:
            table_name: Name of the L3 table
            mapping: Mapping configuration

        Returns:
            Row count from L2 source
        """
        # Handle generated dimensions (no L2 source)
        if not mapping.get('l2_sources'):
            return mapping.get('expected_count', 0)

        # Get main source table
        main_source = [s for s in mapping['l2_sources'] if s['join_type'] == 'main'][0]

        # Build query
        query = f"""
            SELECT COUNT(*)
            FROM {main_source['schema']}.{main_source['table']}
        """

        # Apply filters
        filters = mapping.get('filters', {})
        if filters:
            where_clauses = []
            for col, val in filters.items():
                if isinstance(val, bool):
                    where_clauses.append(f"{col} = {1 if val else 0}")
                elif isinstance(val, str):
                    where_clauses.append(f"{col} = '{val}'")
                else:
                    where_clauses.append(f"{col} = {val}")

            query += " WHERE " + " AND ".join(where_clauses)

        # Execute query
        try:
            result = self.mysql_conn.fetch_one(query)
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting L2 count for {table_name}: {e}")
            return 0

    def _get_l3_target_count(self, table_name: str, mapping: Dict[str, Any]) -> int:
        """
        Get row count from L3 ClickHouse target table.

        Args:
            table_name: Name of the L3 table
            mapping: Mapping configuration

        Returns:
            Row count from L3 target
        """
        l3_table = mapping['l3_target']

        query = f"SELECT COUNT(*) FROM {l3_table}"

        try:
            result = self.clickhouse_client.query(query)
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            logger.error(f"Error getting L3 count for {table_name}: {e}")
            return 0
