"""
Base Validator Class

Provides common functionality for all ETL validators.
"""

from datetime import datetime
from typing import Dict, Any, List


class ValidationResult:
    """Container for validation results."""

    def __init__(self, check_name: str, status: str, details: Dict[str, Any]):
        """
        Initialize validation result.

        Args:
            check_name: Name of the validation check
            status: PASS, FAIL, or WARNING
            details: Dictionary of validation details
        """
        self.check_name = check_name
        self.status = status
        self.details = details
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'check_name': self.check_name,
            'status': self.status,
            'details': self.details,
            'timestamp': self.timestamp
        }


class BaseValidator:
    """Base class for all ETL validators."""

    def __init__(self, mysql_conn, clickhouse_client):
        """
        Initialize base validator.

        Args:
            mysql_conn: MySQL database connection
            clickhouse_client: ClickHouse client connection
        """
        self.mysql_conn = mysql_conn
        self.clickhouse_client = clickhouse_client
        self.results: List[ValidationResult] = []

    def validate(self) -> List[ValidationResult]:
        """
        Run validation checks. Override in subclasses.

        Returns:
            List of validation results
        """
        raise NotImplementedError("Subclasses must implement validate()")

    def add_result(self, check_name: str, status: str, details: Dict[str, Any]):
        """
        Record a validation result.

        Args:
            check_name: Name of the check
            status: PASS, FAIL, or WARNING
            details: Dictionary containing validation details
        """
        result = ValidationResult(check_name, status, details)
        self.results.append(result)

    def get_summary(self) -> Dict[str, int]:
        """
        Get summary statistics for validation results.

        Returns:
            Dictionary with counts of passed/failed/warnings
        """
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == 'PASS')
        failed = sum(1 for r in self.results if r.status == 'FAIL')
        warnings = sum(1 for r in self.results if r.status == 'WARNING')

        return {
            'total_checks': total,
            'passed': passed,
            'failed': failed,
            'warnings': warnings,
            'pass_rate': (passed / total * 100) if total > 0 else 0
        }
