"""
Database utilities for TA-RDM Source Ingestion.

Provides connection management for:
- MySQL (L2 Canonical, L1 Staging, Configuration Database)
- SQL Server (RAMIS Source)

Includes transaction handling, batch operations, and common database utilities.
"""

import logging
from typing import Optional, List, Tuple, Any, Dict
from contextlib import contextmanager
from enum import Enum

import mysql.connector
from mysql.connector import Error as MySQLError, MySQLConnection
from mysql.connector.cursor import MySQLCursor

try:
    import pymssql
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    pymssql = None


logger = logging.getLogger(__name__)


class DatabaseType(Enum):
    """Supported database types."""
    MYSQL = "mysql"
    SQL_SERVER = "sql_server"


class DatabaseConnection:
    """
    Base database connection manager.

    Supports both MySQL and SQL Server connections with
    transaction management and batch operations.
    """

    def __init__(self, db_type: DatabaseType, config: Dict[str, Any]):
        """
        Initialize database connection manager.

        Args:
            db_type: Type of database (MySQL or SQL Server)
            config: Database connection configuration
        """
        self.db_type = db_type
        self.config = config
        self.connection: Optional[Any] = None
        self.cursor: Optional[Any] = None

    def connect(self) -> Any:
        """
        Establish database connection.

        Returns:
            Connection object

        Raises:
            ConnectionError: If connection fails
        """
        try:
            if self.db_type == DatabaseType.MYSQL:
                return self._connect_mysql()
            elif self.db_type == DatabaseType.SQL_SERVER:
                return self._connect_sqlserver()
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

        except Exception as e:
            logger.error(f"Error connecting to {self.db_type.value}: {e}")
            raise ConnectionError(f"Failed to connect to database: {e}")

    def _connect_mysql(self) -> MySQLConnection:
        """Connect to MySQL database."""
        logger.info(f"Connecting to MySQL: {self.config.get('host')}:{self.config.get('port')}")

        self.connection = mysql.connector.connect(**self.config)

        if self.connection.is_connected():
            db_info = self.connection.get_server_info()
            logger.info(f"Connected to MySQL Server version {db_info}")
            return self.connection
        else:
            raise ConnectionError("Failed to establish MySQL connection")

    def _connect_sqlserver(self):
        """Connect to SQL Server database."""
        if not MSSQL_AVAILABLE:
            raise ImportError(
                "pymssql is not installed. Install with: pip install pymssql"
            )

        logger.info(
            f"Connecting to SQL Server: {self.config.get('host')}:"
            f"{self.config.get('port')}"
        )

        self.connection = pymssql.connect(
            server=self.config['host'],
            port=self.config.get('port', 1433),
            user=self.config['user'],
            password=self.config['password'],
            database=self.config.get('database', ''),
            timeout=self.config.get('timeout', 30),
            login_timeout=self.config.get('login_timeout', 10)
        )

        logger.info("Connected to SQL Server")
        return self.connection

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.connection:
            if self.db_type == DatabaseType.MYSQL:
                if self.connection.is_connected():
                    self.connection.close()
            else:
                self.connection.close()

            logger.info(f"{self.db_type.value} connection closed")
            self.connection = None

    def get_cursor(self, dictionary: bool = False) -> Any:
        """
        Get a cursor for executing queries.

        Args:
            dictionary: Return rows as dictionaries (MySQL only)

        Returns:
            Database cursor

        Raises:
            ConnectionError: If no connection exists
        """
        if not self.connection:
            self.connect()

        if self.db_type == DatabaseType.MYSQL:
            if not self.connection.is_connected():
                self.connect()
            self.cursor = self.connection.cursor(dictionary=dictionary)
        else:
            self.cursor = self.connection.cursor(as_dict=dictionary)

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
            Exception: If query execution fails
        """
        cursor = self.get_cursor()

        try:
            cursor.execute(query, params or ())
            return cursor.rowcount

        except Exception as e:
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
            Exception: If query execution fails
        """
        if not params_list:
            logger.warning("execute_many called with empty params_list")
            return 0

        cursor = self.get_cursor()

        try:
            cursor.executemany(query, params_list)
            rows_affected = cursor.rowcount
            logger.debug(
                f"Batch executed: {len(params_list)} statements, "
                f"{rows_affected} rows affected"
            )
            return rows_affected

        except Exception as e:
            logger.error(f"Batch query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Batch size: {len(params_list)}")
            raise

        finally:
            cursor.close()

    def fetch_one(self, query: str, params: Optional[Tuple] = None,
                  dictionary: bool = False) -> Optional[Any]:
        """
        Execute a SELECT query and fetch one result.

        Args:
            query: SQL SELECT query
            params: Query parameters
            dictionary: Return as dictionary

        Returns:
            First row or None
        """
        cursor = self.get_cursor(dictionary=dictionary)

        try:
            cursor.execute(query, params or ())
            return cursor.fetchone()

        except Exception as e:
            logger.error(f"Fetch one failed: {e}")
            logger.error(f"Query: {query}")
            raise

        finally:
            cursor.close()

    def fetch_all(self, query: str, params: Optional[Tuple] = None,
                  dictionary: bool = False) -> List[Any]:
        """
        Execute a SELECT query and fetch all results.

        Args:
            query: SQL SELECT query
            params: Query parameters
            dictionary: Return as dictionaries

        Returns:
            List of result rows
        """
        cursor = self.get_cursor(dictionary=dictionary)

        try:
            cursor.execute(query, params or ())
            return cursor.fetchall()

        except Exception as e:
            logger.error(f"Fetch all failed: {e}")
            logger.error(f"Query: {query}")
            raise

        finally:
            cursor.close()

    def fetch_batch(self, query: str, params: Optional[Tuple] = None,
                   batch_size: int = 10000, dictionary: bool = False):
        """
        Execute a SELECT query and yield results in batches.

        Args:
            query: SQL SELECT query
            params: Query parameters
            batch_size: Number of rows per batch
            dictionary: Return as dictionaries

        Yields:
            List of rows (batch)
        """
        cursor = self.get_cursor(dictionary=dictionary)

        try:
            cursor.execute(query, params or ())

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

        except Exception as e:
            logger.error(f"Fetch batch failed: {e}")
            logger.error(f"Query: {query}")
            raise

        finally:
            cursor.close()

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
            schema: Schema/database name
            table_name: Table name

        Returns:
            bool: True if table exists
        """
        if self.db_type == DatabaseType.MYSQL:
            query = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """
        else:  # SQL Server
            query = """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """

        result = self.fetch_one(query, (schema, table_name))
        return result[0] > 0 if result else False

    def get_table_row_count(self, schema: str, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            schema: Schema/database name
            table_name: Table name

        Returns:
            int: Number of rows
        """
        if self.db_type == DatabaseType.MYSQL:
            query = f"SELECT COUNT(*) FROM `{schema}`.`{table_name}`"
        else:  # SQL Server
            query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"

        result = self.fetch_one(query)
        return result[0] if result else 0

    def truncate_table(self, schema: str, table_name: str):
        """
        Truncate a table (delete all rows).

        Args:
            schema: Schema/database name
            table_name: Table name
        """
        if self.db_type == DatabaseType.MYSQL:
            query = f"TRUNCATE TABLE `{schema}`.`{table_name}`"
        else:  # SQL Server
            query = f"TRUNCATE TABLE [{schema}].[{table_name}]"

        self.execute_query(query)
        logger.info(f"Truncated table {schema}.{table_name}")

    def get_column_names(self, schema: str, table_name: str) -> List[str]:
        """
        Get column names for a table.

        Args:
            schema: Schema/database name
            table_name: Table name

        Returns:
            List[str]: Column names
        """
        if self.db_type == DatabaseType.MYSQL:
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
        else:  # SQL Server
            query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """

        rows = self.fetch_all(query, (schema, table_name))
        return [row[0] for row in rows]


@contextmanager
def get_db_connection(db_type: DatabaseType, config: Dict[str, Any]):
    """
    Context manager for database connections.

    Args:
        db_type: Type of database
        config: Database configuration

    Yields:
        DatabaseConnection: Database connection instance

    Usage:
        with get_db_connection(DatabaseType.MYSQL, config) as db:
            db.execute_query(...)
            db.commit()
    """
    db = DatabaseConnection(db_type, config)
    try:
        db.connect()
        yield db
    except Exception as e:
        logger.error(f"Database error: {e}")
        db.rollback()
        raise
    finally:
        db.disconnect()


def test_connection(db_type: DatabaseType, config: Dict[str, Any]) -> bool:
    """
    Test database connection.

    Args:
        db_type: Type of database
        config: Database configuration

    Returns:
        bool: True if connection successful
    """
    try:
        with get_db_connection(db_type, config) as db:
            # Test with simple query
            if db_type == DatabaseType.MYSQL:
                result = db.fetch_one("SELECT VERSION()")
                logger.info(f"MySQL version: {result[0]}")
            else:
                result = db.fetch_one("SELECT @@VERSION")
                logger.info(f"SQL Server version: {result[0]}")
            return True

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
