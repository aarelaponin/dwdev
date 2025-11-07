"""
Mapping Transformer for TA-RDM Source Ingestion.

Applies column mappings and transformations based on configuration database.
"""

import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, date

from transformers.base_transformer import BaseTransformer
from metadata.catalog import MetadataCatalog

logger = logging.getLogger(__name__)


class MappingTransformer(BaseTransformer):
    """
    Metadata-driven mapping transformer.

    Applies column mappings, transformations, and lookups
    based on configuration from the metadata catalog.
    """

    def __init__(self, mapping_id: int, catalog: MetadataCatalog):
        """
        Initialize mapping transformer.

        Args:
            mapping_id: Table mapping ID
            catalog: Metadata catalog instance
        """
        super().__init__()
        self.mapping_id = mapping_id
        self.catalog = catalog

        # Load mapping configuration
        self.table_mapping = catalog.get_table_mapping(mapping_id)
        if not self.table_mapping:
            raise ValueError(f"Mapping ID {mapping_id} not found")

        self.column_mappings = catalog.get_column_mappings(mapping_id)
        self.lookup_mappings = catalog.get_lookup_mappings(mapping_id)

        # Build lookup cache
        self._build_lookup_cache()

        logger.info(
            f"Mapping transformer initialized: {self.table_mapping['mapping_code']} "
            f"({len(self.column_mappings)} columns)"
        )

    def _build_lookup_cache(self):
        """Build lookup value cache for performance."""
        self.lookup_cache = {}

        for lookup in self.lookup_mappings:
            if lookup['column_mapping_id'] not in self.lookup_cache:
                self.lookup_cache[lookup['column_mapping_id']] = {}

            self.lookup_cache[lookup['column_mapping_id']][lookup['source_value']] = {
                'target_value': lookup['target_value'],
                'fallback': lookup['fallback_value']
            }

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform a batch of rows.

        Args:
            rows: List of source rows

        Returns:
            List[Dict]: Transformed rows
        """
        transformed_rows = []

        for row in rows:
            try:
                transformed_row = self.transform_row(row)
                transformed_rows.append(transformed_row)
            except Exception as e:
                row_id = self._get_row_id(row)
                self.log_error(row_id, str(e), {'row': row})
                # Continue processing other rows

        return transformed_rows

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single row.

        Args:
            row: Source row

        Returns:
            Dict: Transformed row with target column names
        """
        transformed = {}

        for col_mapping in self.column_mappings:
            source_col = col_mapping['source_column']
            target_col = col_mapping['target_column']
            transformation_type = col_mapping['transformation_type']

            # Get source value
            source_value = row.get(source_col)

            # Apply transformation
            if transformation_type == 'DIRECT':
                target_value = source_value

            elif transformation_type == 'EXPRESSION':
                target_value = self._apply_expression(
                    source_value,
                    col_mapping['transformation_sql'],
                    row
                )

            elif transformation_type == 'LOOKUP':
                target_value = self._apply_lookup(
                    source_value,
                    col_mapping['column_mapping_id']
                )

            elif transformation_type == 'FUNCTION':
                target_value = self._apply_function(
                    source_value,
                    col_mapping['transformation_sql']
                )

            else:
                logger.warning(
                    f"Unknown transformation type: {transformation_type}, "
                    f"using DIRECT"
                )
                target_value = source_value

            # Apply default value if needed
            if target_value is None and col_mapping['default_value']:
                target_value = col_mapping['default_value']

            # Cast to target type
            if target_value is not None and col_mapping['target_data_type']:
                target_value = self.cast_to_type(
                    target_value,
                    col_mapping['target_data_type']
                )

            transformed[target_col] = target_value

        return transformed

    def _apply_expression(self, value: Any, sql_expression: Optional[str],
                         row: Dict[str, Any]) -> Any:
        """
        Apply SQL-like expression transformation.

        Args:
            value: Source value
            sql_expression: SQL expression with {source_column} placeholder
            row: Full source row for complex expressions

        Returns:
            Transformed value
        """
        if not sql_expression:
            return value

        try:
            # Simple string transformations
            if value is None:
                return None

            # Handle common SQL functions
            expr = sql_expression.upper()

            if 'UPPER' in expr:
                return str(value).upper() if value else None

            elif 'LOWER' in expr:
                return str(value).lower() if value else None

            elif 'TRIM' in expr:
                return str(value).strip() if value else None

            elif 'LTRIM' in expr:
                return str(value).lstrip() if value else None

            elif 'RTRIM' in expr:
                return str(value).rstrip() if value else None

            elif 'COALESCE' in expr:
                # Extract values from COALESCE(col1, col2, 'default')
                match = re.findall(r'COALESCE\((.*?)\)', expr)
                if match:
                    values = [v.strip().strip("'") for v in match[0].split(',')]
                    for val in values:
                        # Check if it's a column reference
                        if val in row and row[val] is not None:
                            return row[val]
                        # Or a literal value
                        elif val.startswith("'"):
                            return val.strip("'")
                    return None

            elif 'CAST' in expr and 'DATE' in expr:
                # CAST({source_column} AS DATE)
                if isinstance(value, (date, datetime)):
                    return value.date() if isinstance(value, datetime) else value
                elif isinstance(value, str):
                    return datetime.strptime(value, '%Y-%m-%d').date()

            # If no specific function matched, return original value
            # In production, could use sqlglot or similar for full SQL parsing
            return value

        except Exception as e:
            logger.warning(f"Expression transformation failed: {e}")
            return value

    def _apply_lookup(self, value: Any, column_mapping_id: int) -> Any:
        """
        Apply lookup transformation.

        Args:
            value: Source value to lookup
            column_mapping_id: Column mapping ID

        Returns:
            Target value from lookup or fallback
        """
        if value is None:
            return None

        # Get lookup from cache
        lookups = self.lookup_cache.get(column_mapping_id, {})

        # Convert value to string for lookup
        lookup_key = str(value)

        if lookup_key in lookups:
            return lookups[lookup_key]['target_value']
        else:
            # Return fallback if defined
            fallback = next(
                (l['fallback'] for l in lookups.values() if l['fallback']),
                None
            )
            if fallback:
                logger.debug(f"Using fallback value for {lookup_key}: {fallback}")
            return fallback

    def _apply_function(self, value: Any, function_name: Optional[str]) -> Any:
        """
        Apply custom function transformation.

        Args:
            value: Source value
            function_name: Function name

        Returns:
            Transformed value
        """
        # This is a placeholder for custom function transformations
        # In production, could register custom Python functions
        logger.warning(f"Custom function not implemented: {function_name}")
        return value

    def _get_row_id(self, row: Dict[str, Any]) -> str:
        """
        Get row identifier for error logging.

        Args:
            row: Source row

        Returns:
            str: Row identifier
        """
        # Try to get key columns from mapping
        import json
        try:
            key_columns = json.loads(self.table_mapping.get('source_key_columns', '[]'))
            if key_columns:
                key_values = [str(row.get(col, '')) for col in key_columns]
                return ":".join(key_values)
        except:
            pass

        # Fallback: use first few columns
        values = list(row.values())[:3]
        return ":".join([str(v) for v in values])

    def get_target_columns(self) -> List[str]:
        """
        Get list of target column names.

        Returns:
            List[str]: Target column names
        """
        return [cm['target_column'] for cm in self.column_mappings]

    def get_key_columns(self) -> List[str]:
        """
        Get list of key (primary/unique) column names.

        Returns:
            List[str]: Key column names
        """
        return [
            cm['target_column']
            for cm in self.column_mappings
            if cm['is_key_column']
        ]
