"""
Staging Loader for TA-RDM Source Ingestion.

Implements bulk loading to staging (Layer 1) tables.
"""

import logging
from typing import List, Dict, Any, Optional

from loaders.base_loader import BaseLoader
from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class StagingLoader(BaseLoader):
    """
    Staging table loader.

    Loads data to staging tables (Layer 1) using bulk INSERT operations.
    Staging tables are typically truncated before each load.
    """

    def __init__(self, db_connection: DatabaseConnection,
                 batch_size: int = 10000,
                 truncate_before_load: bool = True):
        """
        Initialize staging loader.

        Args:
            db_connection: MySQL database connection
            batch_size: Number of rows to load per batch
            truncate_before_load: Truncate staging table before loading
        """
        super().__init__(db_connection, batch_size)
        self.truncate_before_load = truncate_before_load
        logger.info(
            f"Staging loader initialized (batch size: {batch_size}, "
            f"truncate: {truncate_before_load})"
        )

    def load(self, rows: List[Dict[str, Any]], table_name: str,
            schema: Optional[str] = 'staging') -> int:
        """
        Load data to staging table.

        Args:
            rows: List of rows to load
            table_name: Target table name
            schema: Schema name (default: staging)

        Returns:
            int: Number of rows loaded
        """
        if not rows:
            logger.warning("No rows to load")
            return 0

        self.start_load()

        # Truncate staging table if requested
        if self.truncate_before_load:
            logger.info(f"Truncating staging table {schema}.{table_name}")
            self.truncate_table(table_name, schema)

        # Get column names from first row
        columns = list(rows[0].keys())
        logger.info(f"Loading {len(rows):,} rows to {schema}.{table_name}")
        logger.debug(f"Columns: {columns}")

        # Build INSERT query
        insert_query = self.build_insert_query(table_name, schema, columns)
        logger.debug(f"Insert query: {insert_query}")

        # Load in batches
        total_loaded = 0
        batch_number = 0

        try:
            for i in range(0, len(rows), self.batch_size):
                batch_number += 1
                batch = rows[i:i + self.batch_size]

                # Convert rows to value tuples
                values = self.build_values_from_rows(batch, columns)

                # Execute batch insert
                rows_affected = self.db.execute_many(insert_query, values)
                total_loaded += rows_affected

                self.log_batch_progress(batch_number, len(batch))

            # Commit transaction
            self.db.commit()
            logger.info(f"Successfully loaded {total_loaded:,} rows to staging")

        except Exception as e:
            self.log_error(str(e), {'table': table_name, 'batch': batch_number})
            logger.error(f"Error loading to staging: {e}")
            self.db.rollback()
            raise

        finally:
            self.end_load()

        return total_loaded

    def load_batch(self, batch: List[Dict[str, Any]], table_name: str,
                  schema: Optional[str] = 'staging') -> int:
        """
        Load a single batch to staging table (incremental mode).

        Args:
            batch: Batch of rows to load
            table_name: Target table name
            schema: Schema name

        Returns:
            int: Number of rows loaded
        """
        if not batch:
            return 0

        columns = list(batch[0].keys())
        insert_query = self.build_insert_query(table_name, schema, columns)
        values = self.build_values_from_rows(batch, columns)

        try:
            rows_affected = self.db.execute_many(insert_query, values)
            self.stats['rows_loaded'] += rows_affected
            self.stats['batches_processed'] += 1
            return rows_affected

        except Exception as e:
            self.log_error(str(e), {'table': table_name})
            raise

    def truncate_table(self, table_name: str, schema: Optional[str] = 'staging'):
        """
        Truncate staging table.

        Args:
            table_name: Table name
            schema: Schema name
        """
        schema = schema or 'staging'

        try:
            self.db.truncate_table(schema, table_name)
            logger.info(f"Truncated table {schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to truncate table: {e}")
            raise

    def create_staging_table_like(self, staging_table: str,
                                  source_table: str,
                                  staging_schema: str = 'staging',
                                  source_schema: Optional[str] = None):
        """
        Create staging table with same structure as source table.

        Args:
            staging_table: Staging table name
            source_table: Source table name
            staging_schema: Staging schema
            source_schema: Source schema
        """
        query = f"""
            CREATE TABLE IF NOT EXISTS `{staging_schema}`.`{staging_table}`
            LIKE `{source_schema}`.`{source_table}`
        """

        try:
            self.db.execute_query(query)
            self.db.commit()
            logger.info(
                f"Created staging table {staging_schema}.{staging_table} "
                f"like {source_schema}.{source_table}"
            )
        except Exception as e:
            logger.error(f"Failed to create staging table: {e}")
            raise

    def get_staging_row_count(self, table_name: str,
                             schema: str = 'staging') -> int:
        """
        Get row count for staging table.

        Args:
            table_name: Table name
            schema: Schema name

        Returns:
            int: Number of rows
        """
        return self.get_table_row_count(table_name, schema)

    def clear_old_staging_data(self, retention_days: int = 7,
                              staging_schema: str = 'staging'):
        """
        Clear old staging data based on retention policy.

        Args:
            retention_days: Number of days to retain
            staging_schema: Staging schema name
        """
        # This would query staging_tables metadata and delete old data
        # Implementation depends on tracking load timestamps
        logger.info(f"Clearing staging data older than {retention_days} days")
        # TODO: Implement based on staging_tables.last_refresh_date
