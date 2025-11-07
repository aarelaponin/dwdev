"""
ClickHouse utilities for TA-RDM Test Data Generator.

Provides connection management and common ClickHouse operations
for Layer 3 data warehouse (ta_dw).
"""

import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import clickhouse_connect
from clickhouse_connect.driver import Client

from config.clickhouse_config import CLICKHOUSE_CONFIG, get_connection_string


logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """
    Manages ClickHouse database connections and operations.
    """

    def __init__(self):
        """Initialize ClickHouse connection manager."""
        self.client: Optional[Client] = None

    def connect(self) -> Client:
        """
        Establish connection to ClickHouse database.

        Returns:
            Client: Active ClickHouse client

        Raises:
            Exception: If connection fails
        """
        try:
            logger.info(f"Connecting to ClickHouse: {get_connection_string()}")

            self.client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG['host'],
                port=CLICKHOUSE_CONFIG['port'],
                database=CLICKHOUSE_CONFIG['database'],
                username=CLICKHOUSE_CONFIG['username'],
                password=CLICKHOUSE_CONFIG['password'],
                secure=CLICKHOUSE_CONFIG['secure'],
                compress=CLICKHOUSE_CONFIG['compress']
            )

            # Test connection
            version = self.client.server_version
            logger.info(f"Successfully connected to ClickHouse Server version {version}")

            return self.client

        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {e}")
            raise

    def disconnect(self):
        """Close ClickHouse connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("ClickHouse connection closed")

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a single query (INSERT, UPDATE, DELETE).

        Args:
            query: SQL query
            parameters: Query parameters (optional)

        Returns:
            int: Number of affected rows (for ClickHouse, this may not be accurate)

        Raises:
            Exception: If query execution fails
        """
        if not self.client:
            self.connect()

        try:
            result = self.client.command(query, parameters=parameters)
            logger.debug(f"Query executed: {query[:100]}...")
            return 1  # ClickHouse doesn't return rowcount like MySQL

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Parameters: {parameters}")
            raise

    def insert(self, table: str, data: List[List[Any]], column_names: Optional[List[str]] = None):
        """
        Insert data into a table.

        Args:
            table: Table name
            data: List of rows (each row is a list of values)
            column_names: Optional list of column names

        Raises:
            Exception: If insert fails
        """
        if not self.client:
            self.connect()

        try:
            self.client.insert(table, data, column_names=column_names)
            logger.info(f"Inserted {len(data)} rows into {table}")

        except Exception as e:
            logger.error(f"Insert failed: {e}")
            logger.error(f"Table: {table}")
            logger.error(f"Rows: {len(data)}")
            raise

    def query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[tuple]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL SELECT query
            parameters: Query parameters (optional)

        Returns:
            List[tuple]: Query results
        """
        if not self.client:
            self.connect()

        try:
            result = self.client.query(query, parameters=parameters)
            return result.result_rows

        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.error(f"Query: {query}")
            raise

    def query_df(self, query: str, parameters: Optional[Dict[str, Any]] = None):
        """
        Execute a SELECT query and return results as DataFrame.

        Args:
            query: SQL SELECT query
            parameters: Query parameters (optional)

        Returns:
            DataFrame: Query results as pandas DataFrame

        Note:
            Requires pandas to be installed
        """
        if not self.client:
            self.connect()

        try:
            return self.client.query_df(query, parameters=parameters)

        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.error(f"Query: {query}")
            raise

    def table_exists(self, table_name: str, database: Optional[str] = None) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Table name
            database: Database name (defaults to configured database)

        Returns:
            bool: True if table exists
        """
        db = database or CLICKHOUSE_CONFIG['database']

        query = """
            SELECT count()
            FROM system.tables
            WHERE database = {db:String} AND name = {table:String}
        """

        result = self.query(query, {'db': db, 'table': table_name})
        return result[0][0] > 0 if result else False

    def get_table_row_count(self, table_name: str, database: Optional[str] = None) -> int:
        """
        Get the number of rows in a table.

        Args:
            table_name: Table name
            database: Database name (defaults to configured database)

        Returns:
            int: Number of rows
        """
        db = database or CLICKHOUSE_CONFIG['database']
        query = f"SELECT count() FROM `{db}`.`{table_name}`"

        result = self.query(query)
        return result[0][0] if result else 0

    def get_table_schema(self, table_name: str, database: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get the schema (columns) of a table.

        Args:
            table_name: Table name
            database: Database name (defaults to configured database)

        Returns:
            List[Dict]: List of column definitions
        """
        db = database or CLICKHOUSE_CONFIG['database']

        query = f"DESCRIBE TABLE `{db}`.`{table_name}`"

        result = self.query(query)

        # Convert to list of dicts
        columns = []
        for row in result:
            columns.append({
                'name': row[0],
                'type': row[1],
                'default_type': row[2] if len(row) > 2 else '',
                'default_expression': row[3] if len(row) > 3 else '',
                'comment': row[4] if len(row) > 4 else ''
            })

        return columns

    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """
        List all tables in a database.

        Args:
            database: Database name (defaults to configured database)

        Returns:
            List[str]: List of table names
        """
        db = database or CLICKHOUSE_CONFIG['database']

        query = """
            SELECT name
            FROM system.tables
            WHERE database = {db:String}
            ORDER BY name
        """

        result = self.query(query, {'db': db})
        return [row[0] for row in result]

    def truncate_table(self, table_name: str, database: Optional[str] = None):
        """
        Truncate a table (delete all rows).

        Args:
            table_name: Table name
            database: Database name (defaults to configured database)
        """
        db = database or CLICKHOUSE_CONFIG['database']
        query = f"TRUNCATE TABLE `{db}`.`{table_name}`"

        self.execute_query(query)
        logger.info(f"Truncated table {db}.{table_name}")

    def get_max_value(self, table_name: str, column_name: str,
                     database: Optional[str] = None) -> Optional[int]:
        """
        Get the maximum value of a column (useful for generating surrogate keys).

        Args:
            table_name: Table name
            column_name: Column name
            database: Database name (defaults to configured database)

        Returns:
            Optional[int]: Maximum value or None if table is empty
        """
        db = database or CLICKHOUSE_CONFIG['database']
        query = f"SELECT max(`{column_name}`) FROM `{db}`.`{table_name}`"

        result = self.query(query)
        return result[0][0] if result and result[0][0] is not None else None


@contextmanager
def get_clickhouse_connection():
    """
    Context manager for ClickHouse connections.

    Usage:
        with get_clickhouse_connection() as ch:
            ch.execute_query(...)
    """
    ch = ClickHouseConnection()
    try:
        ch.connect()
        yield ch
    except Exception as e:
        logger.error(f"ClickHouse error: {e}")
        raise
    finally:
        ch.disconnect()
