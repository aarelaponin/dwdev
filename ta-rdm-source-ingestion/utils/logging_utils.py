"""
Logging utilities for TA-RDM Source Ingestion.

Provides colored console output, file logging, and structured
logging for ETL operations.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional
from pathlib import Path

from colorama import Fore, Back, Style, init as colorama_init
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize colorama for cross-platform colored output
colorama_init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds colors to console output.

    Uses colorama for cross-platform colored terminal output.
    """

    # Color mapping for log levels
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Back.WHITE + Style.BRIGHT,
    }

    # Emoji/icon prefixes for log levels
    ICONS = {
        'DEBUG': '🔍',
        'INFO': '✓',
        'WARNING': '⚠',
        'ERROR': '✗',
        'CRITICAL': '🔥',
    }

    def __init__(self, use_colors: bool = True, use_icons: bool = True):
        """
        Initialize colored formatter.

        Args:
            use_colors: Enable colored output
            use_icons: Enable icon prefixes
        """
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.use_colors = use_colors
        self.use_icons = use_icons

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with colors and icons.

        Args:
            record: Log record to format

        Returns:
            str: Formatted log message
        """
        # Get base formatted message
        log_message = super().format(record)

        # Add icon prefix if enabled
        if self.use_icons:
            icon = self.ICONS.get(record.levelname, '')
            log_message = f"{icon}  {log_message}"

        # Add color if enabled
        if self.use_colors:
            color = self.COLORS.get(record.levelname, '')
            log_message = f"{color}{log_message}{Style.RESET_ALL}"

        return log_message


class ETLLogger:
    """
    Centralized logger for ETL operations.

    Supports both console (with colors) and file logging.
    """

    def __init__(
        self,
        name: str = 'ta_rdm_ingestion',
        log_level: Optional[str] = None,
        log_file: Optional[str] = None,
        use_colors: Optional[bool] = None,
        log_sql: Optional[bool] = None
    ):
        """
        Initialize ETL logger.

        Args:
            name: Logger name
            log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Path to log file (None for console only)
            use_colors: Enable colored console output
            log_sql: Enable SQL query logging
        """
        self.name = name
        self.logger = logging.getLogger(name)

        # Get configuration from environment or use defaults
        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self.log_file = log_file or os.getenv('LOG_FILE_PATH', '')
        self.use_colors = use_colors if use_colors is not None else \
            os.getenv('LOG_COLORED_OUTPUT', 'true').lower() == 'true'
        self.log_sql = log_sql if log_sql is not None else \
            os.getenv('LOG_SQL_QUERIES', 'false').lower() == 'true'

        # Set log level
        self.logger.setLevel(getattr(logging, self.log_level.upper()))

        # Remove existing handlers
        self.logger.handlers.clear()

        # Add console handler
        self._add_console_handler()

        # Add file handler if specified
        if self.log_file:
            self._add_file_handler()

    def _add_console_handler(self):
        """Add colored console handler."""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)

        # Use colored formatter
        console_formatter = ColoredFormatter(
            use_colors=self.use_colors,
            use_icons=True
        )
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(console_handler)

    def _add_file_handler(self):
        """Add file handler for persistent logging."""
        try:
            # Create log directory if it doesn't exist
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            # Create file handler
            file_handler = logging.FileHandler(
                self.log_file,
                mode='a',
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)

            # Use plain formatter for file (no colors)
            file_formatter = logging.Formatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)

            self.logger.addHandler(file_handler)
            self.logger.info(f"File logging enabled: {self.log_file}")

        except Exception as e:
            self.logger.warning(f"Could not create file handler: {e}")

    def get_logger(self) -> logging.Logger:
        """
        Get the configured logger instance.

        Returns:
            logging.Logger: Configured logger
        """
        return self.logger

    def log_sql(self, query: str, params: Optional[tuple] = None):
        """
        Log SQL query (only if LOG_SQL_QUERIES is enabled).

        Args:
            query: SQL query string
            params: Query parameters
        """
        if self.log_sql:
            self.logger.debug(f"SQL: {query}")
            if params:
                self.logger.debug(f"PARAMS: {params}")

    def log_etl_start(self, process_name: str, source: str, target: str):
        """
        Log ETL process start.

        Args:
            process_name: Name of ETL process
            source: Source system/table
            target: Target system/table
        """
        self.logger.info("=" * 80)
        self.logger.info(f"ETL PROCESS: {process_name}")
        self.logger.info(f"SOURCE: {source}")
        self.logger.info(f"TARGET: {target}")
        self.logger.info(f"STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)

    def log_etl_end(self, process_name: str, status: str,
                    rows_processed: int = 0, duration_seconds: float = 0.0):
        """
        Log ETL process end.

        Args:
            process_name: Name of ETL process
            status: SUCCESS or FAILED
            rows_processed: Number of rows processed
            duration_seconds: Processing duration
        """
        self.logger.info("=" * 80)
        self.logger.info(f"ETL PROCESS COMPLETED: {process_name}")
        self.logger.info(f"STATUS: {status}")
        self.logger.info(f"ROWS PROCESSED: {rows_processed:,}")
        self.logger.info(f"DURATION: {duration_seconds:.2f} seconds")
        self.logger.info(f"ENDED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)

    def log_batch_progress(self, current: int, total: int, batch_size: int):
        """
        Log batch processing progress.

        Args:
            current: Current row number
            total: Total rows to process
            batch_size: Batch size
        """
        percent = (current / total * 100) if total > 0 else 0
        self.logger.info(
            f"Processing batch: {current:,}/{total:,} rows ({percent:.1f}%) - "
            f"Batch size: {batch_size:,}"
        )

    def log_validation_result(self, rule_name: str, passed: bool,
                             violations: int = 0, message: str = ""):
        """
        Log data quality validation result.

        Args:
            rule_name: Validation rule name
            passed: Whether validation passed
            violations: Number of violations
            message: Additional message
        """
        if passed:
            self.logger.info(f"VALIDATION PASSED: {rule_name}")
        else:
            self.logger.error(
                f"VALIDATION FAILED: {rule_name} - "
                f"{violations} violations - {message}"
            )


# Global logger instance
_global_logger: Optional[ETLLogger] = None


def setup_logging(
    name: str = 'ta_rdm_ingestion',
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    use_colors: Optional[bool] = None
) -> logging.Logger:
    """
    Setup and configure logging for the application.

    Args:
        name: Logger name
        log_level: Log level
        log_file: Path to log file
        use_colors: Enable colored output

    Returns:
        logging.Logger: Configured logger instance
    """
    global _global_logger
    _global_logger = ETLLogger(
        name=name,
        log_level=log_level,
        log_file=log_file,
        use_colors=use_colors
    )
    return _global_logger.get_logger()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get logger instance.

    Args:
        name: Logger name (optional)

    Returns:
        logging.Logger: Logger instance
    """
    if name:
        return logging.getLogger(name)

    global _global_logger
    if _global_logger is None:
        return setup_logging()

    return _global_logger.get_logger()
