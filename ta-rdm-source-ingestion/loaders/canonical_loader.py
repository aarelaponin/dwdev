"""
Canonical Loader for TA-RDM Source Ingestion.

Implements UPSERT loading to canonical (Layer 2) tables.
"""

import logging
from typing import List, Dict, Any, Optional

from loaders.base_loader import BaseLoader
from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class CanonicalLoader(BaseLoader):
    """
    Canonical table loader.

    Loads data to canonical tables (Layer 2) using UPSERT (INSERT...ON DUPLICATE KEY UPDATE)
    or separate INSERT/UPDATE operations based on key columns.
    """

    def __init__(self, db_connection: DatabaseConnection,
                 batch_size: int = 10000,
                 merge_strategy: str = 'UPSERT'):
        """
        Initialize canonical loader.

        Args:
            db_connection: MySQL database connection
            batch_size: Number of rows to load per batch
            merge_strategy: UPSERT, INSERT, UPDATE
        """
        super().__init__(db_connection, batch_size)
        self.merge_strategy = merge_strategy
        logger.info(
            f"Canonical loader initialized (batch size: {batch_size}, "
            f"strategy: {merge_strategy})"
        )

    def load(self, rows: List[Dict[str, Any]], table_name: str,
            schema: Optional[str] = None,
            key_columns: Optional[List[str]] = None) -> int:
        """
        Load data to canonical table.

        Args:
            rows: List of rows to load
            table_name: Target table name
            schema: Schema name
            key_columns: Key columns for UPSERT (primary/unique keys)

        Returns:
            int: Number of rows loaded
        """
        if not rows:
            logger.warning("No rows to load")
            return 0

        self.start_load()

        columns = list(rows[0].keys())
        logger.info(
            f"Loading {len(rows):,} rows to {schema}.{table_name} "
            f"(strategy: {self.merge_strategy})"
        )

        # Choose loading strategy
        if self.merge_strategy == 'UPSERT':
            total_loaded = self._load_upsert(
                rows, table_name, schema, columns, key_columns
            )
        elif self.merge_strategy == 'INSERT':
            total_loaded = self._load_insert(
                rows, table_name, schema, columns
            )
        elif self.merge_strategy == 'UPDATE':
            total_loaded = self._load_update(
                rows, table_name, schema, columns, key_columns
            )
        else:
            raise ValueError(f"Unknown merge strategy: {self.merge_strategy}")

        self.end_load()
        return total_loaded

    def _load_upsert(self, rows: List[Dict[str, Any]], table_name: str,
                    schema: Optional[str], columns: List[str],
                    key_columns: Optional[List[str]]) -> int:
        """
        Load using INSERT...ON DUPLICATE KEY UPDATE.

        Args:
            rows: Rows to load
            table_name: Table name
            schema: Schema name
            columns: All columns
            key_columns: Key columns

        Returns:
            int: Rows affected
        """
        if not key_columns:
            logger.warning("No key columns specified, using INSERT only")
            return self._load_insert(rows, table_name, schema, columns)

        # Build UPSERT query
        upsert_query = self._build_upsert_query(
            table_name, schema, columns, key_columns
        )
        logger.debug(f"UPSERT query: {upsert_query}")

        total_loaded = 0
        batch_number = 0

        try:
            for i in range(0, len(rows), self.batch_size):
                batch_number += 1
                batch = rows[i:i + self.batch_size]

                values = self.build_values_from_rows(batch, columns)
                rows_affected = self.db.execute_many(upsert_query, values)
                total_loaded += rows_affected

                self.log_batch_progress(batch_number, len(batch))

            self.db.commit()
            logger.info(f"Successfully loaded {total_loaded:,} rows to canonical")

        except Exception as e:
            self.log_error(str(e), {'table': table_name, 'batch': batch_number})
            logger.error(f"Error loading to canonical: {e}")
            self.db.rollback()
            raise

        return total_loaded

    def _load_insert(self, rows: List[Dict[str, Any]], table_name: str,
                    schema: Optional[str], columns: List[str]) -> int:
        """
        Load using INSERT IGNORE (skip duplicates).

        Args:
            rows: Rows to load
            table_name: Table name
            schema: Schema name
            columns: All columns

        Returns:
            int: Rows inserted
        """
        # Build INSERT IGNORE query
        insert_query = self._build_insert_ignore_query(table_name, schema, columns)
        logger.debug(f"INSERT query: {insert_query}")

        total_loaded = 0
        batch_number = 0

        try:
            for i in range(0, len(rows), self.batch_size):
                batch_number += 1
                batch = rows[i:i + self.batch_size]

                values = self.build_values_from_rows(batch, columns)
                rows_affected = self.db.execute_many(insert_query, values)
                total_loaded += rows_affected

                self.log_batch_progress(batch_number, len(batch))

            self.db.commit()
            logger.info(f"Successfully inserted {total_loaded:,} rows")

        except Exception as e:
            self.log_error(str(e), {'table': table_name, 'batch': batch_number})
            self.db.rollback()
            raise

        return total_loaded

    def _load_update(self, rows: List[Dict[str, Any]], table_name: str,
                    schema: Optional[str], columns: List[str],
                    key_columns: Optional[List[str]]) -> int:
        """
        Load using UPDATE only (requires existing rows).

        Args:
            rows: Rows to load
            table_name: Table name
            schema: Schema name
            columns: All columns
            key_columns: Key columns for WHERE clause

        Returns:
            int: Rows updated
        """
        if not key_columns:
            raise ValueError("Key columns required for UPDATE strategy")

        # Non-key columns to update
        update_columns = [col for col in columns if col not in key_columns]

        # Build UPDATE query
        update_query = self._build_update_query(
            table_name, schema, update_columns, key_columns
        )
        logger.debug(f"UPDATE query: {update_query}")

        total_updated = 0
        batch_number = 0

        try:
            # Update row by row (or small batches)
            for i, row in enumerate(rows):
                if i % self.batch_size == 0 and i > 0:
                    batch_number += 1
                    self.log_batch_progress(batch_number, min(self.batch_size, len(rows) - i))

                # Build value tuple: update_columns + key_columns
                values = tuple([row.get(col) for col in update_columns + key_columns])

                rows_affected = self.db.execute_query(update_query, values)
                total_updated += rows_affected

            self.db.commit()
            logger.info(f"Successfully updated {total_updated:,} rows")

        except Exception as e:
            self.log_error(str(e), {'table': table_name})
            self.db.rollback()
            raise

        return total_updated

    def _build_upsert_query(self, table_name: str, schema: Optional[str],
                           columns: List[str], key_columns: List[str]) -> str:
        """
        Build INSERT...ON DUPLICATE KEY UPDATE query.

        Args:
            table_name: Table name
            schema: Schema name
            columns: All columns
            key_columns: Key columns

        Returns:
            str: UPSERT query
        """
        if schema:
            full_table = f"`{schema}`.`{table_name}`"
        else:
            full_table = f"`{table_name}`"

        column_list = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        # Build UPDATE clause for non-key columns
        update_columns = [col for col in columns if col not in key_columns]
        update_clause = ", ".join([
            f"`{col}` = VALUES(`{col}`)" for col in update_columns
        ])

        return (
            f"INSERT INTO {full_table} ({column_list}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

    def _build_insert_ignore_query(self, table_name: str, schema: Optional[str],
                                   columns: List[str]) -> str:
        """
        Build INSERT IGNORE query.

        Args:
            table_name: Table name
            schema: Schema name
            columns: All columns

        Returns:
            str: INSERT IGNORE query
        """
        if schema:
            full_table = f"`{schema}`.`{table_name}`"
        else:
            full_table = f"`{table_name}`"

        column_list = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        return f"INSERT IGNORE INTO {full_table} ({column_list}) VALUES ({placeholders})"

    def _build_update_query(self, table_name: str, schema: Optional[str],
                           update_columns: List[str], key_columns: List[str]) -> str:
        """
        Build UPDATE query.

        Args:
            table_name: Table name
            schema: Schema name
            update_columns: Columns to update
            key_columns: Key columns for WHERE clause

        Returns:
            str: UPDATE query
        """
        if schema:
            full_table = f"`{schema}`.`{table_name}`"
        else:
            full_table = f"`{table_name}`"

        set_clause = ", ".join([f"`{col}` = %s" for col in update_columns])
        where_clause = " AND ".join([f"`{col}` = %s" for col in key_columns])

        return f"UPDATE {full_table} SET {set_clause} WHERE {where_clause}"

    def truncate_table(self, table_name: str, schema: Optional[str] = None):
        """
        Truncate canonical table (use with caution!).

        Args:
            table_name: Table name
            schema: Schema name
        """
        logger.warning(
            f"Truncating canonical table {schema}.{table_name} - "
            f"this will delete all data!"
        )

        try:
            self.db.truncate_table(schema, table_name)
            self.db.commit()
            logger.info(f"Truncated table {schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to truncate table: {e}")
            raise
