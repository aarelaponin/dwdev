"""
Database utilities for TA-RDM Test Data Generator.

Provides connection management, transaction handling, and
common database operations.
"""

import logging
from typing import Optional, List, Tuple, Any, Dict
from contextlib import contextmanager
import mysql.connector
from mysql.connector import Error, MySQLConnection
from mysql.connector.cursor import MySQLCursor

from config.database_config import DATABASE_CONFIG, get_connection_string


logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Manages MySQL database connections and transactions.
    """

    def __init__(self):
        """Initialize database connection manager."""
        self.connection: Optional[MySQLConnection] = None
        self.cursor: Optional[MySQLCursor] = None

    def connect(self) -> MySQLConnection:
        """
        Establish connection to MySQL database.

        Returns:
            MySQLConnection: Active database connection

        Raises:
            Error: If connection fails
        """
        try:
            logger.info(f"Connecting to database: {get_connection_string()}")
            self.connection = mysql.connector.connect(**DATABASE_CONFIG)

            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                logger.info(f"Successfully connected to MySQL Server version {db_info}")
                return self.connection
            else:
                raise Error("Failed to establish database connection")

        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")

    def get_cursor(self) -> MySQLCursor:
        """
        Get a cursor for executing queries.

        Returns:
            MySQLCursor: Database cursor

        Raises:
            Error: If no connection exists
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()

        self.cursor = self.connection.cursor()
        return self.cursor

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a single query (INSERT, UPDATE, DELETE).

        Args:
            query: SQL query with placeholders
            params: Query parameters

        Returns:
            int: Number of affected rows

        Raises:
            Error: If query execution fails
        """
        cursor = self.get_cursor()

        try:
            cursor.execute(query, params or ())
            return cursor.rowcount

        except Error as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

        finally:
            cursor.close()

    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """
        Execute a query with multiple parameter sets (batch insert).

        Args:
            query: SQL query with placeholders
            params_list: List of parameter tuples

        Returns:
            int: Total number of affected rows

        Raises:
            Error: If query execution fails
        """
        cursor = self.get_cursor()

        try:
            cursor.executemany(query, params_list)
            return cursor.rowcount

        except Error as e:
            logger.error(f"Batch query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Batch size: {len(params_list)}")
            raise

        finally:
            cursor.close()

    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """
        Execute a SELECT query and fetch one result.

        Args:
            query: SQL SELECT query
            params: Query parameters

        Returns:
            Optional[Tuple]: First row or None
        """
        cursor = self.get_cursor()

        try:
            cursor.execute(query, params or ())
            return cursor.fetchone()

        except Error as e:
            logger.error(f"Fetch one failed: {e}")
            logger.error(f"Query: {query}")
            raise

        finally:
            cursor.close()

    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute a SELECT query and fetch all results.

        Args:
            query: SQL SELECT query
            params: Query parameters

        Returns:
            List[Tuple]: All result rows
        """
        cursor = self.get_cursor()

        try:
            cursor.execute(query, params or ())
            return cursor.fetchall()

        except Error as e:
            logger.error(f"Fetch all failed: {e}")
            logger.error(f"Query: {query}")
            raise

        finally:
            cursor.close()

    def get_last_insert_id(self) -> int:
        """
        Get the last auto-increment ID inserted.

        Returns:
            int: Last inserted ID
        """
        result = self.fetch_one("SELECT LAST_INSERT_ID()")
        return result[0] if result else 0

    def commit(self):
        """Commit current transaction."""
        if self.connection:
            self.connection.commit()
            logger.debug("Transaction committed")

    def rollback(self):
        """Rollback current transaction."""
        if self.connection:
            self.connection.rollback()
            logger.warning("Transaction rolled back")

    def table_exists(self, schema: str, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            schema: Schema name
            table_name: Table name

        Returns:
            bool: True if table exists
        """
        query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        result = self.fetch_one(query, (schema, table_name))
        return result[0] > 0 if result else False

    def get_table_row_count(self, schema: str, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            schema: Schema name
            table_name: Table name

        Returns:
            int: Number of rows
        """
        query = f"SELECT COUNT(*) FROM `{schema}`.`{table_name}`"
        result = self.fetch_one(query)
        return result[0] if result else 0

    def truncate_table(self, schema: str, table_name: str):
        """
        Truncate a table (delete all rows).

        Args:
            schema: Schema name
            table_name: Table name
        """
        query = f"TRUNCATE TABLE `{schema}`.`{table_name}`"
        self.execute_query(query)
        logger.info(f"Truncated table {schema}.{table_name}")

    def get_foreign_key_id(self, schema: str, table_name: str,
                          key_column: str, value_column: str,
                          value: Any) -> Optional[int]:
        """
        Get foreign key ID by looking up a value.

        Args:
            schema: Schema name
            table_name: Table name
            key_column: Primary key column name
            value_column: Column to search by
            value: Value to search for

        Returns:
            Optional[int]: Foreign key ID or None
        """
        query = f"""
            SELECT `{key_column}`
            FROM `{schema}`.`{table_name}`
            WHERE `{value_column}` = %s
            LIMIT 1
        """
        result = self.fetch_one(query, (value,))
        return result[0] if result else None


@contextmanager
def get_db_connection():
    """
    Context manager for database connections.

    Usage:
        with get_db_connection() as db:
            db.execute_query(...)
            db.commit()
    """
    db = DatabaseConnection()
    try:
        db.connect()
        yield db
    except Error as e:
        logger.error(f"Database error: {e}")
        db.rollback()
        raise
    finally:
        db.disconnect()


def validate_foreign_keys(db: DatabaseConnection,
                         foreign_keys: Dict[str, Tuple[str, str, Any]]) -> bool:
    """
    Validate that foreign key references exist.

    Args:
        db: Database connection
        foreign_keys: Dict mapping FK name to (schema, table, value) tuple

    Returns:
        bool: True if all foreign keys are valid

    Raises:
        ValueError: If any foreign key is invalid
    """
    for fk_name, (schema, table, value) in foreign_keys.items():
        # Extract key column (assume it's table_name + '_id')
        key_column = f"{table}_id"

        # Check if value exists
        result = db.fetch_one(
            f"SELECT 1 FROM `{schema}`.`{table}` WHERE `{key_column}` = %s",
            (value,)
        )

        if not result:
            raise ValueError(
                f"Invalid foreign key '{fk_name}': "
                f"{schema}.{table}.{key_column} = {value} does not exist"
            )

    logger.debug(f"Validated {len(foreign_keys)} foreign key references")
    return True
