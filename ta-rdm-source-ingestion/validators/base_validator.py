"""
Base Validator for TA-RDM Source Ingestion.

Provides abstract base class for all data validators.
Validators enforce data quality rules.
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation rule severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ValidationAction(Enum):
    """Actions to take on validation failure."""
    LOG = "LOG"           # Log and continue
    WARN = "WARN"         # Warn and continue
    REJECT = "REJECT"     # Reject row
    FIX = "FIX"          # Attempt to fix
    CONTINUE = "CONTINUE" # Ignore


class ValidationResult:
    """Container for validation results."""

    def __init__(self, passed: bool = True, rule_code: str = "",
                 severity: ValidationSeverity = ValidationSeverity.INFO,
                 message: str = "", column: Optional[str] = None,
                 value: Optional[Any] = None):
        """
        Initialize validation result.

        Args:
            passed: Whether validation passed
            rule_code: Rule code that was violated
            severity: Severity level
            message: Error/warning message
            column: Column name (if applicable)
            value: Value that failed validation
        """
        self.passed = passed
        self.rule_code = rule_code
        self.severity = severity
        self.message = message
        self.column = column
        self.value = value
        self.timestamp = datetime.now()


class BaseValidator(ABC):
    """
    Abstract base class for data validators.

    All validation logic should inherit from this class
    and implement specific validation methods.
    """

    def __init__(self):
        """Initialize base validator."""
        self.stats = {
            'rows_validated': 0,
            'rows_passed': 0,
            'rows_failed': 0,
            'violations': [],
            'validation_start': None,
            'validation_end': None
        }

    @abstractmethod
    def validate(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[ValidationResult]]:
        """
        Validate a batch of rows.

        Args:
            rows: List of rows to validate

        Returns:
            Tuple of (valid_rows, validation_results)
        """
        pass

    @abstractmethod
    def validate_row(self, row: Dict[str, Any]) -> List[ValidationResult]:
        """
        Validate a single row.

        Args:
            row: Row to validate

        Returns:
            List[ValidationResult]: Validation results (empty if all passed)
        """
        pass

    def start_validation(self):
        """Record validation start time."""
        self.stats['validation_start'] = datetime.now()
        self.stats['rows_validated'] = 0
        self.stats['rows_passed'] = 0
        self.stats['rows_failed'] = 0
        self.stats['violations'] = []
        logger.info("Validation started")

    def end_validation(self):
        """Record validation end time."""
        self.stats['validation_end'] = datetime.now()
        duration = (self.stats['validation_end'] -
                   self.stats['validation_start']).total_seconds()

        logger.info(
            f"Validation completed: {self.stats['rows_validated']:,} rows, "
            f"{self.stats['rows_passed']:,} passed, "
            f"{self.stats['rows_failed']:,} failed, "
            f"{len(self.stats['violations'])} violations, "
            f"{duration:.2f} seconds"
        )

    def log_violation(self, result: ValidationResult, row_id: Any):
        """
        Log a validation violation.

        Args:
            result: Validation result
            row_id: Row identifier
        """
        violation = {
            'row_id': row_id,
            'rule_code': result.rule_code,
            'severity': result.severity.value,
            'message': result.message,
            'column': result.column,
            'value': result.value,
            'timestamp': result.timestamp
        }
        self.stats['violations'].append(violation)

        if result.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]:
            logger.error(f"Validation failed for row {row_id}: {result.message}")
        elif result.severity == ValidationSeverity.WARNING:
            logger.warning(f"Validation warning for row {row_id}: {result.message}")
        else:
            logger.info(f"Validation info for row {row_id}: {result.message}")

    def get_validation_stats(self) -> Dict[str, Any]:
        """
        Get validation statistics.

        Returns:
            Dict: Validation statistics
        """
        stats = self.stats.copy()

        if stats['validation_start'] and stats['validation_end']:
            stats['duration_seconds'] = (
                stats['validation_end'] - stats['validation_start']
            ).total_seconds()

            if stats['duration_seconds'] > 0:
                stats['rows_per_second'] = (
                    stats['rows_validated'] / stats['duration_seconds']
                )

        stats['violation_count'] = len(stats['violations'])

        return stats

    def reset_stats(self):
        """Reset validation statistics."""
        self.stats = {
            'rows_validated': 0,
            'rows_passed': 0,
            'rows_failed': 0,
            'violations': [],
            'validation_start': None,
            'validation_end': None
        }

    # =========================================================================
    # Common Validation Methods
    # =========================================================================

    def validate_not_null(self, value: Any, column_name: str) -> ValidationResult:
        """
        Validate that value is not NULL.

        Args:
            value: Value to validate
            column_name: Column name

        Returns:
            ValidationResult
        """
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return ValidationResult(
                passed=False,
                rule_code='NOT_NULL',
                severity=ValidationSeverity.ERROR,
                message=f"Column '{column_name}' cannot be NULL",
                column=column_name,
                value=value
            )
        return ValidationResult(passed=True)

    def validate_pattern(self, value: Any, pattern: str, column_name: str) -> ValidationResult:
        """
        Validate value matches regex pattern.

        Args:
            value: Value to validate
            pattern: Regex pattern
            column_name: Column name

        Returns:
            ValidationResult
        """
        if value is None:
            return ValidationResult(passed=True)

        if not isinstance(value, str):
            value = str(value)

        if not re.match(pattern, value):
            return ValidationResult(
                passed=False,
                rule_code='PATTERN',
                severity=ValidationSeverity.ERROR,
                message=f"Column '{column_name}' does not match pattern: {pattern}",
                column=column_name,
                value=value
            )
        return ValidationResult(passed=True)

    def validate_range(self, value: Any, min_value: Optional[Any],
                      max_value: Optional[Any], column_name: str) -> ValidationResult:
        """
        Validate value is within range.

        Args:
            value: Value to validate
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            column_name: Column name

        Returns:
            ValidationResult
        """
        if value is None:
            return ValidationResult(passed=True)

        try:
            if min_value is not None and value < min_value:
                return ValidationResult(
                    passed=False,
                    rule_code='RANGE',
                    severity=ValidationSeverity.ERROR,
                    message=f"Column '{column_name}' value {value} is less than minimum {min_value}",
                    column=column_name,
                    value=value
                )

            if max_value is not None and value > max_value:
                return ValidationResult(
                    passed=False,
                    rule_code='RANGE',
                    severity=ValidationSeverity.ERROR,
                    message=f"Column '{column_name}' value {value} is greater than maximum {max_value}",
                    column=column_name,
                    value=value
                )

        except Exception as e:
            logger.warning(f"Range validation error: {e}")

        return ValidationResult(passed=True)

    def validate_length(self, value: Any, min_length: Optional[int],
                       max_length: Optional[int], column_name: str) -> ValidationResult:
        """
        Validate string length.

        Args:
            value: Value to validate
            min_length: Minimum length
            max_length: Maximum length
            column_name: Column name

        Returns:
            ValidationResult
        """
        if value is None:
            return ValidationResult(passed=True)

        length = len(str(value))

        if min_length is not None and length < min_length:
            return ValidationResult(
                passed=False,
                rule_code='LENGTH',
                severity=ValidationSeverity.ERROR,
                message=f"Column '{column_name}' length {length} is less than minimum {min_length}",
                column=column_name,
                value=value
            )

        if max_length is not None and length > max_length:
            return ValidationResult(
                passed=False,
                rule_code='LENGTH',
                severity=ValidationSeverity.ERROR,
                message=f"Column '{column_name}' length {length} is greater than maximum {max_length}",
                column=column_name,
                value=value
            )

        return ValidationResult(passed=True)

    def validate_date_range(self, value: Any, min_date: Optional[date],
                           max_date: Optional[date], column_name: str) -> ValidationResult:
        """
        Validate date is within range.

        Args:
            value: Date value to validate
            min_date: Minimum date
            max_date: Maximum date
            column_name: Column name

        Returns:
            ValidationResult
        """
        if value is None:
            return ValidationResult(passed=True)

        # Convert to date if datetime
        if isinstance(value, datetime):
            value = value.date()

        if not isinstance(value, date):
            return ValidationResult(
                passed=False,
                rule_code='DATE_RANGE',
                severity=ValidationSeverity.ERROR,
                message=f"Column '{column_name}' is not a valid date",
                column=column_name,
                value=value
            )

        return self.validate_range(value, min_date, max_date, column_name)
