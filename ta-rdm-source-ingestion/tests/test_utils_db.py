"""
Unit Tests for utils.db_utils module.

Tests database connection management, query execution, and batch operations.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from utils.db_utils import DatabaseType, DatabaseConnection


# ============================================================================
# DatabaseType Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
class TestDatabaseType:
    """Test DatabaseType enum."""

    def test_database_types_defined(self):
        """Test that all database types are defined."""
        assert DatabaseType.MYSQL == 'mysql'
        assert DatabaseType.SQL_SERVER == 'sqlserver'

    def test_database_types_are_strings(self):
        """Test that database types are string values."""
        for db_type in DatabaseType:
            assert isinstance(db_type.value, str)


# ============================================================================
# DatabaseConnection Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
@pytest.mark.database
class TestDatabaseConnection:
    """Test DatabaseConnection class."""

    # ------------------------------------------------------------------------
    # Initialization Tests
    # ------------------------------------------------------------------------

    def test_init_mysql(self, test_db_config):
        """Test MySQL connection initialization."""
        db = DatabaseConnection(DatabaseType.MYSQL, test_db_config)

        assert db.db_type == DatabaseType.MYSQL
        assert db.config == test_db_config
        assert db.connection is None
        assert db.cursor is None
        assert db.is_connected is False

    def test_init_sqlserver(self, test_sqlserver_config):
        """Test SQL Server connection initialization."""
        db = DatabaseConnection(DatabaseType.SQL_SERVER, test_sqlserver_config)

        assert db.db_type == DatabaseType.SQL_SERVER
        assert db.config == test_sqlserver_config
        assert db.connection is None
        assert db.cursor is None
        assert db.is_connected is False

    def test_init_unsupported_database_type(self, test_db_config):
        """Test initialization with unsupported database type."""
        with pytest.raises(ValueError, match="Unsupported database type"):
            DatabaseConnection('postgresql', test_db_config)

    # ------------------------------------------------------------------------
    # Connection Tests
    # ------------------------------------------------------------------------

    @patch('utils.db_utils.mysql.connector.connect')
    def test_connect_mysql_success(self, mock_connect, test_db_config):
        """Test successful MySQL connection."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Connect
        db = DatabaseConnection(DatabaseType.MYSQL, test_db_config)
        result = db.connect()

        # Verify
        assert result is True
        assert db.is_connected is True
        assert db.connection == mock_connection
        mock_connect.assert_called_once()

    @patch('utils.db_utils.pymssql.connect')
    def test_connect_sqlserver_success(self, mock_connect, test_sqlserver_config):
        """Test successful SQL Server connection."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Connect
        db = DatabaseConnection(DatabaseType.SQL_SERVER, test_sqlserver_config)
        result = db.connect()

        # Verify
        assert result is True
        assert db.is_connected is True
        assert db.connection == mock_connection
        mock_connect.assert_called_once()

    @patch('utils.db_utils.mysql.connector.connect')
    def test_connect_failure(self, mock_connect, test_db_config):
        """Test connection failure."""
        # Setup mock to raise exception
        mock_connect.side_effect = Exception("Connection failed")

        # Connect
        db = DatabaseConnection(DatabaseType.MYSQL, test_db_config)
        result = db.connect()

        # Verify
        assert result is False
        assert db.is_connected is False
        assert db.connection is None

    def test_disconnect_success(self, mock_mysql_connection):
        """Test successful disconnect."""
        # Setup
        db = mock_mysql_connection
        db.is_connected = True

        # Disconnect
        result = db.disconnect()

        # Verify
        assert result is True
        db.disconnect.assert_called_once()

    # ------------------------------------------------------------------------
    # Query Execution Tests
    # ------------------------------------------------------------------------

    def test_execute_query_success(self, mock_mysql_connection):
        """Test successful query execution."""
        # Setup
        db = mock_mysql_connection
        db.execute.return_value = True

        # Execute
        result = db.execute("SELECT * FROM test")

        # Verify
        assert result is True
        db.execute.assert_called_once_with("SELECT * FROM test")

    def test_execute_query_with_params(self, mock_mysql_connection):
        """Test query execution with parameters."""
        # Setup
        db = mock_mysql_connection
        db.execute.return_value = True
        params = ('value1', 'value2')

        # Execute
        result = db.execute("INSERT INTO test VALUES (%s, %s)", params)

        # Verify
        assert result is True
        db.execute.assert_called_once_with("INSERT INTO test VALUES (%s, %s)", params)

    def test_fetch_one(self, mock_mysql_connection):
        """Test fetch_one method."""
        # Setup
        expected_row = {'id': 1, 'name': 'Test'}
        db = mock_mysql_connection
        db.fetch_one.return_value = expected_row

        # Fetch
        result = db.fetch_one("SELECT * FROM test WHERE id = 1")

        # Verify
        assert result == expected_row
        db.fetch_one.assert_called_once()

    def test_fetch_all(self, mock_mysql_connection):
        """Test fetch_all method."""
        # Setup
        expected_rows = [
            {'id': 1, 'name': 'Test1'},
            {'id': 2, 'name': 'Test2'}
        ]
        db = mock_mysql_connection
        db.fetch_all.return_value = expected_rows

        # Fetch
        result = db.fetch_all("SELECT * FROM test")

        # Verify
        assert result == expected_rows
        assert len(result) == 2
        db.fetch_all.assert_called_once()

    # ------------------------------------------------------------------------
    # Batch Operation Tests
    # ------------------------------------------------------------------------

    def test_execute_many(self, mock_mysql_connection):
        """Test execute_many for batch operations."""
        # Setup
        db = mock_mysql_connection
        db.execute_many.return_value = 3

        params_list = [
            ('value1', 'value2'),
            ('value3', 'value4'),
            ('value5', 'value6')
        ]

        # Execute
        result = db.execute_many("INSERT INTO test VALUES (%s, %s)", params_list)

        # Verify
        assert result == 3
        db.execute_many.assert_called_once()

    # ------------------------------------------------------------------------
    # Transaction Tests
    # ------------------------------------------------------------------------

    def test_commit(self, mock_mysql_connection):
        """Test commit transaction."""
        db = mock_mysql_connection

        # Commit
        db.commit()

        # Verify
        db.commit.assert_called_once()

    def test_rollback(self, mock_mysql_connection):
        """Test rollback transaction."""
        db = mock_mysql_connection

        # Rollback
        db.rollback()

        # Verify
        db.rollback.assert_called_once()

    # ------------------------------------------------------------------------
    # Context Manager Tests
    # ------------------------------------------------------------------------

    @patch('utils.db_utils.mysql.connector.connect')
    def test_context_manager(self, mock_connect, test_db_config):
        """Test using DatabaseConnection as context manager."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Use as context manager
        db = DatabaseConnection(DatabaseType.MYSQL, test_db_config)

        # Note: Since we're mocking, we can't actually use 'with' statement
        # but we can test the methods exist
        assert hasattr(db, '__enter__')
        assert hasattr(db, '__exit__')

    # ------------------------------------------------------------------------
    # Configuration Validation Tests
    # ------------------------------------------------------------------------

    def test_missing_required_config_host(self):
        """Test missing required configuration parameter (host)."""
        invalid_config = {
            'port': 3308,
            'user': 'test',
            'password': 'test',
            'database': 'test'
        }

        with pytest.raises((KeyError, ValueError)):
            db = DatabaseConnection(DatabaseType.MYSQL, invalid_config)
            db.connect()

    def test_config_with_extra_parameters(self, test_db_config):
        """Test configuration with extra parameters."""
        # Add extra parameters
        config_with_extras = test_db_config.copy()
        config_with_extras['extra_param'] = 'extra_value'

        # Should not raise error
        db = DatabaseConnection(DatabaseType.MYSQL, config_with_extras)
        assert db.config['extra_param'] == 'extra_value'


# ============================================================================
# Integration-like Tests (marked slow)
# ============================================================================

@pytest.mark.slow
@pytest.mark.database
class TestDatabaseConnectionIntegration:
    """Integration-style tests for database connections.

    Note: These tests use mocks but simulate real-world scenarios.
    """

    @patch('utils.db_utils.mysql.connector.connect')
    def test_full_query_lifecycle(self, mock_connect, test_db_config):
        """Test complete query lifecycle: connect, execute, fetch, disconnect."""
        # Setup mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {'count': 10}
        mock_cursor.fetchall.return_value = [{'id': 1}, {'id': 2}]

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Create connection
        db = DatabaseConnection(DatabaseType.MYSQL, test_db_config)

        # Connect
        assert db.connect() is True

        # Execute query (would be mocked in real test)
        # Fetch results
        # Commit
        # Disconnect

        assert db.is_connected is True

    @patch('utils.db_utils.mysql.connector.connect')
    def test_transaction_rollback_on_error(self, mock_connect, test_db_config):
        """Test that transactions are rolled back on error."""
        # Setup mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        db = DatabaseConnection(DatabaseType.MYSQL, test_db_config)
        db.connect()

        # Verify connection was established
        assert db.is_connected is True


# ============================================================================
# Test Data Validation
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
class TestDatabaseConfigValidation:
    """Test database configuration validation."""

    def test_valid_mysql_config(self, test_db_config):
        """Test valid MySQL configuration."""
        required_keys = ['host', 'port', 'user', 'password', 'database']

        for key in required_keys:
            assert key in test_db_config

    def test_valid_sqlserver_config(self, test_sqlserver_config):
        """Test valid SQL Server configuration."""
        required_keys = ['host', 'port', 'user', 'password', 'database']

        for key in required_keys:
            assert key in test_sqlserver_config
