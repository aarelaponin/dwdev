"""
Base Transformer for TA-RDM Source Ingestion.

Provides abstract base class for all data transformers.
Transformers apply mappings and business rules to extracted data.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """
    Abstract base class for data transformers.

    All transformation logic should inherit from this class
    and implement the abstract methods.
    """

    def __init__(self):
        """Initialize base transformer."""
        self.stats = {
            'rows_transformed': 0,
            'transformation_start': None,
            'transformation_end': None,
            'errors': []
        }

    @abstractmethod
    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform a batch of rows.

        Args:
            rows: List of source rows (as dictionaries)

        Returns:
            List[Dict]: Transformed rows
        """
        pass

    @abstractmethod
    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single row.

        Args:
            row: Source row (as dictionary)

        Returns:
            Dict: Transformed row
        """
        pass

    def start_transformation(self):
        """Record transformation start time."""
        self.stats['transformation_start'] = datetime.now()
        self.stats['rows_transformed'] = 0
        self.stats['errors'] = []
        logger.info("Transformation started")

    def end_transformation(self):
        """Record transformation end time."""
        self.stats['transformation_end'] = datetime.now()
        duration = (self.stats['transformation_end'] -
                   self.stats['transformation_start']).total_seconds()

        logger.info(
            f"Transformation completed: {self.stats['rows_transformed']:,} rows, "
            f"{len(self.stats['errors'])} errors, "
            f"{duration:.2f} seconds"
        )

    def log_transformation_progress(self, batch_number: int, batch_size: int):
        """
        Log transformation progress.

        Args:
            batch_number: Current batch number
            batch_size: Number of rows in batch
        """
        self.stats['rows_transformed'] += batch_size
        logger.debug(
            f"Transformed batch {batch_number}: "
            f"{self.stats['rows_transformed']:,} rows total"
        )

    def log_error(self, row_id: Any, error_message: str, context: Optional[Dict] = None):
        """
        Log a transformation error.

        Args:
            row_id: Row identifier
            error_message: Error message
            context: Additional context information
        """
        error = {
            'row_id': row_id,
            'error_message': error_message,
            'context': context,
            'timestamp': datetime.now()
        }
        self.stats['errors'].append(error)
        logger.error(f"Transformation error for row {row_id}: {error_message}")

    def get_transformation_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics.

        Returns:
            Dict: Transformation statistics
        """
        stats = self.stats.copy()

        if stats['transformation_start'] and stats['transformation_end']:
            stats['duration_seconds'] = (
                stats['transformation_end'] - stats['transformation_start']
            ).total_seconds()

            if stats['duration_seconds'] > 0:
                stats['rows_per_second'] = (
                    stats['rows_transformed'] / stats['duration_seconds']
                )

        stats['error_count'] = len(stats['errors'])

        return stats

    def reset_stats(self):
        """Reset transformation statistics."""
        self.stats = {
            'rows_transformed': 0,
            'transformation_start': None,
            'transformation_end': None,
            'errors': []
        }

    def apply_default_value(self, value: Any, default: Any) -> Any:
        """
        Apply default value if value is None.

        Args:
            value: Current value
            default: Default value

        Returns:
            Value or default
        """
        return default if value is None else value

    def cast_to_type(self, value: Any, target_type: str) -> Any:
        """
        Cast value to target type.

        Args:
            value: Value to cast
            target_type: Target type (INT, VARCHAR, DATE, etc.)

        Returns:
            Casted value
        """
        if value is None:
            return None

        try:
            target_type = target_type.upper()

            if target_type in ('INT', 'INTEGER', 'BIGINT'):
                return int(value)
            elif target_type in ('FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'):
                return float(value)
            elif target_type in ('VARCHAR', 'CHAR', 'TEXT', 'STRING'):
                return str(value)
            elif target_type in ('DATE', 'DATETIME', 'TIMESTAMP'):
                # Already handled by database-specific logic
                return value
            elif target_type == 'BOOLEAN':
                if isinstance(value, str):
                    return value.upper() in ('TRUE', '1', 'Y', 'YES')
                return bool(value)
            else:
                return value

        except Exception as e:
            logger.warning(f"Type casting failed: {e}")
            return value
