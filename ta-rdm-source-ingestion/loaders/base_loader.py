"""
Base Loader for TA-RDM Source Ingestion.

Provides abstract base class for all data loaders.
Loaders are responsible for writing data to target tables.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """
    Abstract base class for data loaders.

    All target-specific loaders should inherit from this class
    and implement the abstract methods.
    """

    def __init__(self, db_connection: DatabaseConnection,
                 batch_size: int = 10000):
        """
        Initialize base loader.

        Args:
            db_connection: Database connection to target
            batch_size: Number of rows to load per batch
        """
        self.db = db_connection
        self.batch_size = batch_size
        self.stats = {
            'rows_loaded': 0,
            'batches_processed': 0,
            'load_start': None,
            'load_end': None,
            'errors': []
        }

    @abstractmethod
    def load(self, rows: List[Dict[str, Any]], table_name: str,
            schema: Optional[str] = None) -> int:
        """
        Load data to target table.

        Args:
            rows: List of rows to load
            table_name: Target table name
            schema: Schema name

        Returns:
            int: Number of rows loaded
        """
        pass

    @abstractmethod
    def truncate_table(self, table_name: str, schema: Optional[str] = None):
        """
        Truncate target table.

        Args:
            table_name: Table name
            schema: Schema name
        """
        pass

    def start_load(self):
        """Record load start time."""
        self.stats['load_start'] = datetime.now()
        self.stats['rows_loaded'] = 0
        self.stats['batches_processed'] = 0
        self.stats['errors'] = []
        logger.info("Load started")

    def end_load(self):
        """Record load end time."""
        self.stats['load_end'] = datetime.now()
        duration = (self.stats['load_end'] -
                   self.stats['load_start']).total_seconds()

        logger.info(
            f"Load completed: {self.stats['rows_loaded']:,} rows, "
            f"{self.stats['batches_processed']} batches, "
            f"{duration:.2f} seconds"
        )

    def log_batch_progress(self, batch_number: int, batch_size: int):
        """
        Log batch load progress.

        Args:
            batch_number: Current batch number
            batch_size: Number of rows in batch
        """
        self.stats['batches_processed'] += 1
        self.stats['rows_loaded'] += batch_size
        logger.info(
            f"Loaded batch {batch_number}: "
            f"{self.stats['rows_loaded']:,} rows total"
        )

    def log_error(self, error_message: str, context: Optional[Dict] = None):
        """
        Log a load error.

        Args:
            error_message: Error message
            context: Additional context information
        """
        error = {
            'error_message': error_message,
            'context': context,
            'timestamp': datetime.now()
        }
        self.stats['errors'].append(error)
        logger.error(f"Load error: {error_message}")

    def get_load_stats(self) -> Dict[str, Any]:
        """
        Get load statistics.

        Returns:
            Dict: Load statistics
        """
        stats = self.stats.copy()

        if stats['load_start'] and stats['load_end']:
            stats['duration_seconds'] = (
                stats['load_end'] - stats['load_start']
            ).total_seconds()

            if stats['duration_seconds'] > 0:
                stats['rows_per_second'] = (
                    stats['rows_loaded'] / stats['duration_seconds']
                )

        stats['error_count'] = len(stats['errors'])

        return stats

    def reset_stats(self):
        """Reset load statistics."""
        self.stats = {
            'rows_loaded': 0,
            'batches_processed': 0,
            'load_start': None,
            'load_end': None,
            'errors': []
        }

    def build_insert_query(self, table_name: str, schema: Optional[str],
                          columns: List[str]) -> str:
        """
        Build INSERT query.

        Args:
            table_name: Table name
            schema: Schema name
            columns: Column names

        Returns:
            str: INSERT query with placeholders
        """
        if schema:
            full_table = f"`{schema}`.`{table_name}`"
        else:
            full_table = f"`{table_name}`"

        column_list = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        return f"INSERT INTO {full_table} ({column_list}) VALUES ({placeholders})"

    def build_values_from_rows(self, rows: List[Dict[str, Any]],
                              columns: List[str]) -> List[tuple]:
        """
        Build values tuples from row dictionaries.

        Args:
            rows: List of row dictionaries
            columns: Column names (in order)

        Returns:
            List of value tuples
        """
        values = []
        for row in rows:
            value_tuple = tuple(row.get(col) for col in columns)
            values.append(value_tuple)
        return values

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if target table exists.

        Args:
            table_name: Table name
            schema: Schema name

        Returns:
            bool: True if table exists
        """
        return self.db.table_exists(schema or 'public', table_name)

    def get_table_row_count(self, table_name: str,
                           schema: Optional[str] = None) -> int:
        """
        Get row count for target table.

        Args:
            table_name: Table name
            schema: Schema name

        Returns:
            int: Number of rows
        """
        return self.db.get_table_row_count(schema or 'public', table_name)
