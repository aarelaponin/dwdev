"""
SQL Server Extractor for TA-RDM Source Ingestion.

Implements data extraction from SQL Server databases (e.g., RAMIS).
"""

import logging
from typing import Optional, List, Dict, Any, Generator

from extractors.base_extractor import BaseExtractor
from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class SQLServerExtractor(BaseExtractor):
    """
    SQL Server data extractor.

    Extracts data from SQL Server databases with support for:
    - Full table extraction
    - Incremental extraction based on timestamp/ID columns
    - Filtered extraction with WHERE clauses
    - Column selection
    - Batch processing
    """

    def __init__(self, db_connection: DatabaseConnection,
                 batch_size: int = 10000):
        """
        Initialize SQL Server extractor.

        Args:
            db_connection: SQL Server database connection
            batch_size: Number of rows to fetch per batch
        """
        super().__init__(db_connection, batch_size)
        logger.info(f"SQL Server extractor initialized (batch size: {batch_size})")

    def extract(self, table_name: str, schema: Optional[str] = None,
                columns: Optional[List[str]] = None,
                filter_condition: Optional[str] = None,
                incremental_column: Optional[str] = None,
                incremental_value: Optional[Any] = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Extract data from SQL Server table.

        Args:
            table_name: Source table name
            schema: Schema/database name (default: dbo)
            columns: List of columns to extract (None = all columns)
            filter_condition: WHERE clause filter (without WHERE keyword)
            incremental_column: Column for incremental extraction
            incremental_value: Last extracted value for incremental loads

        Yields:
            List[Dict]: Batches of rows as dictionaries
        """
        self.start_extraction()

        # Build query
        query = self.build_select_query(
            table_name=table_name,
            schema=schema,
            columns=columns,
            filter_condition=filter_condition,
            incremental_column=incremental_column,
            incremental_value=incremental_value
        )

        logger.info(f"Extracting from {schema or 'dbo'}.{table_name}")
        logger.debug(f"Query: {query}")

        # Get total row count for progress tracking
        total_rows = self.get_table_row_count(
            table_name,
            schema,
            filter_condition
        )
        logger.info(f"Total rows to extract: {total_rows:,}")

        # Execute query and yield batches
        batch_number = 0
        try:
            for batch in self.db.fetch_batch(query, batch_size=self.batch_size, dictionary=True):
                batch_number += 1
                batch_size = len(batch)

                if batch_size == 0:
                    break

                self.log_batch_progress(batch_number, batch_size, total_rows)

                yield batch

        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            raise

        finally:
            self.end_extraction()

    def get_table_row_count(self, table_name: str,
                           schema: Optional[str] = None,
                           filter_condition: Optional[str] = None) -> int:
        """
        Get row count for a SQL Server table.

        Args:
            table_name: Table name
            schema: Schema name (default: dbo)
            filter_condition: WHERE clause filter

        Returns:
            int: Number of rows
        """
        schema = schema or 'dbo'
        query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"

        if filter_condition:
            query += f" WHERE {filter_condition}"

        result = self.db.fetch_one(query)
        return result[0] if result else 0

    def get_max_value(self, table_name: str, column_name: str,
                     schema: Optional[str] = None) -> Optional[Any]:
        """
        Get maximum value from a column (for incremental loads).

        Args:
            table_name: Table name
            column_name: Column name
            schema: Schema name (default: dbo)

        Returns:
            Maximum value or None
        """
        schema = schema or 'dbo'
        query = f"SELECT MAX([{column_name}]) FROM [{schema}].[{table_name}]"

        result = self.db.fetch_one(query)
        return result[0] if result and result[0] else None

    def validate_connection(self) -> bool:
        """
        Validate SQL Server connection.

        Returns:
            bool: True if connection is valid
        """
        try:
            result = self.db.fetch_one("SELECT 1")
            return result is not None
        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False

    def build_select_query(self, table_name: str,
                          schema: Optional[str] = None,
                          columns: Optional[List[str]] = None,
                          filter_condition: Optional[str] = None,
                          incremental_column: Optional[str] = None,
                          incremental_value: Optional[Any] = None,
                          order_by: Optional[str] = None,
                          limit: Optional[int] = None) -> str:
        """
        Build SELECT query for SQL Server.

        Args:
            table_name: Table name
            schema: Schema name (default: dbo)
            columns: List of columns (None = *)
            filter_condition: WHERE clause
            incremental_column: Column for incremental extraction
            incremental_value: Last extracted value
            order_by: ORDER BY clause
            limit: TOP clause (SQL Server uses TOP, not LIMIT)

        Returns:
            str: SELECT query
        """
        schema = schema or 'dbo'

        # Build column list
        if columns:
            column_list = ", ".join([f"[{col}]" for col in columns])
        else:
            column_list = "*"

        # Build TOP clause
        top_clause = f"TOP {limit} " if limit else ""

        # Start query
        query = f"SELECT {top_clause}{column_list} FROM [{schema}].[{table_name}]"

        # Build WHERE clause
        where_conditions = []

        if filter_condition:
            where_conditions.append(f"({filter_condition})")

        if incremental_column and incremental_value is not None:
            # Handle different data types
            if isinstance(incremental_value, str):
                where_conditions.append(f"[{incremental_column}] > '{incremental_value}'")
            else:
                where_conditions.append(f"[{incremental_column}] > {incremental_value}")

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        # Add ORDER BY
        if order_by:
            query += f" ORDER BY {order_by}"
        elif incremental_column:
            query += f" ORDER BY [{incremental_column}]"

        return query

    def get_table_columns(self, table_name: str,
                         schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get column information for a table.

        Args:
            table_name: Table name
            schema: Schema name (default: dbo)

        Returns:
            List[Dict]: Column information (name, type, nullable, etc.)
        """
        schema = schema or 'dbo'

        query = """
            SELECT
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                CHARACTER_MAXIMUM_LENGTH as max_length,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as default_value
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """

        return self.db.fetch_all(query, (schema, table_name), dictionary=True)

    def get_table_primary_keys(self, table_name: str,
                              schema: Optional[str] = None) -> List[str]:
        """
        Get primary key columns for a table.

        Args:
            table_name: Table name
            schema: Schema name (default: dbo)

        Returns:
            List[str]: Primary key column names
        """
        schema = schema or 'dbo'

        query = """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND tc.TABLE_NAME = c.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = ?
              AND tc.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """

        rows = self.db.fetch_all(query, (schema, table_name))
        return [row[0] for row in rows]

    def test_table_access(self, table_name: str,
                         schema: Optional[str] = None) -> bool:
        """
        Test if table exists and is accessible.

        Args:
            table_name: Table name
            schema: Schema name (default: dbo)

        Returns:
            bool: True if table is accessible
        """
        schema = schema or 'dbo'

        try:
            query = f"SELECT TOP 1 * FROM [{schema}].[{table_name}]"
            self.db.fetch_one(query)
            return True
        except Exception as e:
            logger.error(f"Table access test failed: {e}")
            return False
