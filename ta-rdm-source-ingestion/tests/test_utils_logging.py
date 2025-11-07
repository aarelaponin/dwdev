"""
Unit Tests for utils.logging_utils module.

Tests logging configuration, ETL logging, and colored output.
"""

import pytest
import logging
import os
from unittest.mock import Mock, patch, call
from utils.logging_utils import setup_logging, ETLLogger, format_duration


# ============================================================================
# setup_logging Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
class TestSetupLogging:
    """Test setup_logging function."""

    def test_setup_basic_logger(self):
        """Test basic logger setup."""
        logger = setup_logging(name='test_logger', log_level='INFO')

        assert logger is not None
        assert logger.name == 'test_logger'
        assert logger.level == logging.INFO

    def test_setup_logger_with_file(self, temp_log_file):
        """Test logger with file output."""
        logger = setup_logging(
            name='test_file_logger',
            log_level='DEBUG',
            log_file=temp_log_file
        )

        assert logger is not None
        logger.info("Test message")

        # Verify log file was created
        assert os.path.exists(temp_log_file)

    def test_setup_logger_different_levels(self):
        """Test logger with different log levels."""
        levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

        for level in levels:
            logger = setup_logging(name=f'test_{level}', log_level=level)
            assert logger.level == getattr(logging, level)

    def test_setup_logger_colored_output(self):
        """Test logger with colored output enabled."""
        logger = setup_logging(
            name='test_colored',
            log_level='INFO',
            use_colors=True
        )

        assert logger is not None
        # Colored output should work without errors
        logger.info("Test colored message")

    def test_setup_logger_no_colors(self):
        """Test logger with colored output disabled."""
        logger = setup_logging(
            name='test_no_colors',
            log_level='INFO',
            use_colors=False
        )

        assert logger is not None
        logger.info("Test plain message")


# ============================================================================
# ETLLogger Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
class TestETLLogger:
    """Test ETLLogger class."""

    def setup_method(self):
        """Setup test logger for each test."""
        self.etl_logger = ETLLogger(log_level='INFO')

    # ------------------------------------------------------------------------
    # Initialization Tests
    # ------------------------------------------------------------------------

    def test_init_default(self):
        """Test ETLLogger initialization with defaults."""
        logger = ETLLogger()

        assert logger is not None
        assert logger.logger is not None

    def test_init_with_log_level(self):
        """Test ETLLogger initialization with log level."""
        logger = ETLLogger(log_level='DEBUG')

        assert logger.logger.level == logging.DEBUG

    def test_init_with_colored_output(self):
        """Test ETLLogger with colored output."""
        logger = ETLLogger(use_colors=True)

        assert logger is not None

    # ------------------------------------------------------------------------
    # ETL Start/End Logging Tests
    # ------------------------------------------------------------------------

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_etl_start(self, mock_info):
        """Test log_etl_start method."""
        self.etl_logger.log_etl_start(
            process_name='TEST_PROCESS',
            source='source.table',
            target='target.table'
        )

        # Verify info was called
        assert mock_info.called

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_etl_end_success(self, mock_info):
        """Test log_etl_end with SUCCESS status."""
        self.etl_logger.log_etl_end(
            process_name='TEST_PROCESS',
            status='SUCCESS',
            rows_processed=1000,
            duration_seconds=45.5
        )

        # Verify info was called
        assert mock_info.called

    @patch('utils.logging_utils.logging.Logger.error')
    def test_log_etl_end_failure(self, mock_error):
        """Test log_etl_end with FAILED status."""
        self.etl_logger.log_etl_end(
            process_name='TEST_PROCESS',
            status='FAILED',
            rows_processed=0,
            duration_seconds=10.0
        )

        # Verify error was called
        assert mock_error.called

    # ------------------------------------------------------------------------
    # Batch Progress Logging Tests
    # ------------------------------------------------------------------------

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_batch_progress(self, mock_info):
        """Test log_batch_progress method."""
        self.etl_logger.log_batch_progress(
            current=5000,
            total=10000,
            batch_size=1000
        )

        # Verify info was called
        assert mock_info.called

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_batch_progress_complete(self, mock_info):
        """Test log_batch_progress when processing is complete."""
        self.etl_logger.log_batch_progress(
            current=10000,
            total=10000,
            batch_size=1000
        )

        # Verify info was called
        assert mock_info.called

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_batch_progress_first_batch(self, mock_info):
        """Test log_batch_progress for first batch."""
        self.etl_logger.log_batch_progress(
            current=1000,
            total=10000,
            batch_size=1000
        )

        # Verify info was called
        assert mock_info.called

    # ------------------------------------------------------------------------
    # Data Quality Logging Tests
    # ------------------------------------------------------------------------

    @patch('utils.logging_utils.logging.Logger.warning')
    def test_log_dq_issue(self, mock_warning):
        """Test log_dq_issue method."""
        self.etl_logger.log_dq_issue(
            rule_code='NOT_NULL',
            column_name='party_id',
            error_message='Column cannot be null',
            severity='ERROR'
        )

        # Verify warning was called
        assert mock_warning.called

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_dq_issue_info_severity(self, mock_info):
        """Test log_dq_issue with INFO severity."""
        self.etl_logger.log_dq_issue(
            rule_code='LENGTH',
            column_name='party_name',
            error_message='Name is short',
            severity='INFO'
        )

        # Verify info was called
        assert mock_info.called

    @patch('utils.logging_utils.logging.Logger.error')
    def test_log_dq_issue_critical_severity(self, mock_error):
        """Test log_dq_issue with CRITICAL severity."""
        self.etl_logger.log_dq_issue(
            rule_code='PATTERN',
            column_name='email',
            error_message='Invalid email format',
            severity='CRITICAL'
        )

        # Verify error was called
        assert mock_error.called

    # ------------------------------------------------------------------------
    # Extraction Logging Tests
    # ------------------------------------------------------------------------

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_extraction_complete(self, mock_info):
        """Test log_extraction_complete method."""
        self.etl_logger.log_extraction_complete(
            source='source.table',
            rows_extracted=10000,
            duration_seconds=30.5
        )

        # Verify info was called
        assert mock_info.called

    # ------------------------------------------------------------------------
    # Transformation Logging Tests
    # ------------------------------------------------------------------------

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_transformation_complete(self, mock_info):
        """Test log_transformation_complete method."""
        self.etl_logger.log_transformation_complete(
            rows_transformed=9950,
            rows_rejected=50,
            duration_seconds=15.2
        )

        # Verify info was called
        assert mock_info.called

    # ------------------------------------------------------------------------
    # Loading Logging Tests
    # ------------------------------------------------------------------------

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_load_complete(self, mock_info):
        """Test log_load_complete method."""
        self.etl_logger.log_load_complete(
            target='target.table',
            rows_loaded=9950,
            duration_seconds=25.8
        )

        # Verify info was called
        assert mock_info.called


# ============================================================================
# Helper Function Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
class TestHelperFunctions:
    """Test helper functions."""

    def test_format_duration_seconds(self):
        """Test format_duration with seconds."""
        result = format_duration(45.5)
        assert '45.5' in result or '45.50' in result
        assert 's' in result.lower()

    def test_format_duration_minutes(self):
        """Test format_duration with minutes."""
        result = format_duration(125.0)  # 2 minutes 5 seconds
        assert 'm' in result.lower() or 'min' in result.lower()

    def test_format_duration_hours(self):
        """Test format_duration with hours."""
        result = format_duration(3665.0)  # 1 hour 1 minute 5 seconds
        assert 'h' in result.lower() or 'hour' in result.lower()

    def test_format_duration_zero(self):
        """Test format_duration with zero."""
        result = format_duration(0.0)
        assert result is not None
        assert '0' in result


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.utils
class TestLoggingIntegration:
    """Integration tests for logging functionality."""

    def test_complete_etl_logging_workflow(self, temp_log_file):
        """Test complete ETL logging workflow."""
        # Setup logger
        logger = ETLLogger(log_level='INFO')

        # Log ETL start
        logger.log_etl_start(
            process_name='INTEGRATION_TEST',
            source='source.test',
            target='target.test'
        )

        # Log extraction
        logger.log_extraction_complete(
            source='source.test',
            rows_extracted=1000,
            duration_seconds=10.0
        )

        # Log transformation
        logger.log_transformation_complete(
            rows_transformed=950,
            rows_rejected=50,
            duration_seconds=5.0
        )

        # Log batch progress
        for i in range(1, 11):
            logger.log_batch_progress(
                current=i * 100,
                total=1000,
                batch_size=100
            )

        # Log DQ issue
        logger.log_dq_issue(
            rule_code='TEST_RULE',
            column_name='test_column',
            error_message='Test error',
            severity='WARNING'
        )

        # Log load complete
        logger.log_load_complete(
            target='target.test',
            rows_loaded=950,
            duration_seconds=8.0
        )

        # Log ETL end
        logger.log_etl_end(
            process_name='INTEGRATION_TEST',
            status='SUCCESS',
            rows_processed=950,
            duration_seconds=23.0
        )

        # No assertions needed - just verify no exceptions

    def test_logging_with_file_output(self, temp_log_file):
        """Test logging with file output."""
        # Setup logger with file
        setup_logging(
            name='file_test',
            log_level='DEBUG',
            log_file=temp_log_file
        )

        logger = logging.getLogger('file_test')

        # Log messages
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")

        # Verify file was created
        assert os.path.exists(temp_log_file)

        # Verify file has content
        with open(temp_log_file, 'r') as f:
            content = f.read()
            assert len(content) > 0


# ============================================================================
# Error Handling Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.utils
class TestLoggingErrorHandling:
    """Test error handling in logging utilities."""

    def test_invalid_log_level(self):
        """Test handling of invalid log level."""
        # Should default to INFO or raise error
        try:
            logger = setup_logging(name='test', log_level='INVALID')
            # If it doesn't raise, verify default level
            assert logger.level in [logging.INFO, logging.WARNING]
        except (ValueError, KeyError):
            # Exception is also acceptable
            pass

    def test_log_to_invalid_directory(self):
        """Test logging to invalid directory."""
        invalid_path = '/nonexistent/directory/test.log'

        # Should handle gracefully
        try:
            logger = setup_logging(
                name='test',
                log_level='INFO',
                log_file=invalid_path
            )
            # If it succeeds, verify logger still works
            assert logger is not None
        except (OSError, IOError, PermissionError):
            # Exception is acceptable
            pass

    @patch('utils.logging_utils.logging.Logger.info')
    def test_log_with_none_values(self, mock_info):
        """Test logging with None values."""
        logger = ETLLogger()

        # Should handle None gracefully
        logger.log_etl_start(
            process_name=None,
            source=None,
            target=None
        )

        # Verify it was called (even with None values)
        assert mock_info.called
