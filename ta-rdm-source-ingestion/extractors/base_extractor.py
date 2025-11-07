"""
Base Extractor for TA-RDM Source Ingestion.

Provides abstract base class for all data extractors.
Extractors are responsible for pulling data from source systems.
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Generator
from datetime import datetime

from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for data extractors.

    All source-specific extractors should inherit from this class
    and implement the abstract methods.
    """

    def __init__(self, db_connection: DatabaseConnection,
                 batch_size: int = 10000):
        """
        Initialize base extractor.

        Args:
            db_connection: Database connection to source
            batch_size: Number of rows to fetch per batch
        """
        self.db = db_connection
        self.batch_size = batch_size
        self.stats = {
            'rows_extracted': 0,
            'batches_processed': 0,
            'extraction_start': None,
            'extraction_end': None
        }

    @abstractmethod
    def extract(self, table_name: str, schema: Optional[str] = None,
                columns: Optional[List[str]] = None,
                filter_condition: Optional[str] = None,
                incremental_column: Optional[str] = None,
                incremental_value: Optional[Any] = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Extract data from source table.

        Args:
            table_name: Source table name
            schema: Schema/database name
            columns: List of columns to extract (None = all columns)
            filter_condition: WHERE clause filter
            incremental_column: Column for incremental extraction
            incremental_value: Last extracted value for incremental loads

        Yields:
            List[Dict]: Batches of rows as dictionaries
        """
        pass

    @abstractmethod
    def get_table_row_count(self, table_name: str,
                           schema: Optional[str] = None,
                           filter_condition: Optional[str] = None) -> int:
        """
        Get row count for a table.

        Args:
            table_name: Table name
            schema: Schema name
            filter_condition: WHERE clause filter

        Returns:
            int: Number of rows
        """
        pass

    @abstractmethod
    def get_max_value(self, table_name: str, column_name: str,
                     schema: Optional[str] = None) -> Optional[Any]:
        """
        Get maximum value from a column (for incremental loads).

        Args:
            table_name: Table name
            column_name: Column name
            schema: Schema name

        Returns:
            Maximum value or None
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate database connection.

        Returns:
            bool: True if connection is valid
        """
        pass

    def build_select_query(self, table_name: str,
                          schema: Optional[str] = None,
                          columns: Optional[List[str]] = None,
                          filter_condition: Optional[str] = None,
                          incremental_column: Optional[str] = None,
                          incremental_value: Optional[Any] = None,
                          order_by: Optional[str] = None,
                          limit: Optional[int] = None) -> str:
        """
        Build SELECT query for extraction.

        Args:
            table_name: Table name
            schema: Schema name
            columns: List of columns
            filter_condition: WHERE clause
            incremental_column: Column for incremental extraction
            incremental_value: Last extracted value
            order_by: ORDER BY clause
            limit: LIMIT clause

        Returns:
            str: SELECT query
        """
        # This is overridden in specific extractors for SQL dialect differences
        pass

    def start_extraction(self):
        """Record extraction start time."""
        self.stats['extraction_start'] = datetime.now()
        self.stats['rows_extracted'] = 0
        self.stats['batches_processed'] = 0
        logger.info("Extraction started")

    def end_extraction(self):
        """Record extraction end time."""
        self.stats['extraction_end'] = datetime.now()
        duration = (self.stats['extraction_end'] -
                   self.stats['extraction_start']).total_seconds()

        logger.info(
            f"Extraction completed: {self.stats['rows_extracted']:,} rows, "
            f"{self.stats['batches_processed']} batches, "
            f"{duration:.2f} seconds"
        )

    def log_batch_progress(self, batch_number: int, batch_size: int,
                          total_rows: Optional[int] = None):
        """
        Log batch extraction progress.

        Args:
            batch_number: Current batch number
            batch_size: Number of rows in batch
            total_rows: Total expected rows (if known)
        """
        self.stats['batches_processed'] += 1
        self.stats['rows_extracted'] += batch_size

        if total_rows:
            percent = (self.stats['rows_extracted'] / total_rows * 100)
            logger.info(
                f"Extracted batch {batch_number}: "
                f"{self.stats['rows_extracted']:,}/{total_rows:,} rows "
                f"({percent:.1f}%)"
            )
        else:
            logger.info(
                f"Extracted batch {batch_number}: "
                f"{self.stats['rows_extracted']:,} rows total"
            )

    def get_extraction_stats(self) -> Dict[str, Any]:
        """
        Get extraction statistics.

        Returns:
            Dict: Extraction statistics
        """
        stats = self.stats.copy()

        if stats['extraction_start'] and stats['extraction_end']:
            stats['duration_seconds'] = (
                stats['extraction_end'] - stats['extraction_start']
            ).total_seconds()

            if stats['duration_seconds'] > 0:
                stats['rows_per_second'] = (
                    stats['rows_extracted'] / stats['duration_seconds']
                )

        return stats

    def reset_stats(self):
        """Reset extraction statistics."""
        self.stats = {
            'rows_extracted': 0,
            'batches_processed': 0,
            'extraction_start': None,
            'extraction_end': None
        }
