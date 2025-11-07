"""
Schema Validator for TA-RDM Source Ingestion.

Validates data against data quality rules from configuration database.
"""

import logging
import json
from typing import List, Dict, Any, Tuple
from datetime import date

from validators.base_validator import (
    BaseValidator, ValidationResult, ValidationSeverity, ValidationAction
)
from metadata.catalog import MetadataCatalog

logger = logging.getLogger(__name__)


class SchemaValidator(BaseValidator):
    """
    Schema validator that enforces data quality rules.

    Validates data against rules defined in the configuration database.
    """

    def __init__(self, mapping_id: int, catalog: MetadataCatalog,
                 action_mode: str = 'REJECT'):
        """
        Initialize schema validator.

        Args:
            mapping_id: Table mapping ID
            catalog: Metadata catalog instance
            action_mode: Default action mode (REJECT, LOG, WARN)
        """
        super().__init__()
        self.mapping_id = mapping_id
        self.catalog = catalog
        self.action_mode = action_mode

        # Load data quality rules
        self.dq_rules = catalog.get_dq_rules(mapping_id, active_only=True)

        logger.info(
            f"Schema validator initialized with {len(self.dq_rules)} DQ rules "
            f"(action mode: {action_mode})"
        )

    def validate(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[ValidationResult]]:
        """
        Validate a batch of rows.

        Args:
            rows: List of rows to validate

        Returns:
            Tuple of (valid_rows, all_validation_results)
        """
        valid_rows = []
        all_results = []

        for row in rows:
            self.stats['rows_validated'] += 1

            # Validate row
            results = self.validate_row(row)

            # Check if any errors
            has_errors = any(
                not r.passed and r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]
                for r in results
            )

            if has_errors and self.action_mode == 'REJECT':
                self.stats['rows_failed'] += 1
                # Log violations
                row_id = self._get_row_id(row)
                for result in results:
                    if not result.passed:
                        self.log_violation(result, row_id)
            else:
                self.stats['rows_passed'] += 1
                valid_rows.append(row)

                # Log warnings and info even for valid rows
                row_id = self._get_row_id(row)
                for result in results:
                    if not result.passed:
                        self.log_violation(result, row_id)

            all_results.extend(results)

        return valid_rows, all_results

    def validate_row(self, row: Dict[str, Any]) -> List[ValidationResult]:
        """
        Validate a single row against all DQ rules.

        Args:
            row: Row to validate

        Returns:
            List[ValidationResult]: Validation results
        """
        results = []

        for rule in self.dq_rules:
            rule_type = rule['rule_type']
            rule_definition = json.loads(rule.get('rule_definition', '{}'))

            # Apply appropriate validation based on rule type
            if rule_type == 'NOT_NULL':
                result = self._validate_not_null_rule(row, rule, rule_definition)

            elif rule_type == 'UNIQUE':
                # UNIQUE validation requires access to all rows - handled at batch level
                result = None

            elif rule_type == 'RANGE':
                result = self._validate_range_rule(row, rule, rule_definition)

            elif rule_type == 'PATTERN':
                result = self._validate_pattern_rule(row, rule, rule_definition)

            elif rule_type == 'LENGTH':
                result = self._validate_length_rule(row, rule, rule_definition)

            elif rule_type == 'DATE_RANGE':
                result = self._validate_date_range_rule(row, rule, rule_definition)

            elif rule_type == 'REFERENTIAL':
                # Referential integrity - requires database lookup
                # TODO: Implement in Phase 4
                result = None

            elif rule_type == 'CUSTOM':
                result = self._validate_custom_rule(row, rule)

            else:
                logger.warning(f"Unknown rule type: {rule_type}")
                result = None

            if result and not result.passed:
                # Override severity from rule
                result.severity = ValidationSeverity[rule['severity']]
                result.rule_code = rule['rule_code']
                results.append(result)

        return results

    def _validate_not_null_rule(self, row: Dict[str, Any], rule: Dict,
                                definition: Dict) -> ValidationResult:
        """Validate NOT_NULL rule."""
        columns = definition.get('columns', [])

        for column in columns:
            value = row.get(column)
            result = self.validate_not_null(value, column)
            if not result.passed:
                return result

        return ValidationResult(passed=True)

    def _validate_range_rule(self, row: Dict[str, Any], rule: Dict,
                            definition: Dict) -> ValidationResult:
        """Validate RANGE rule."""
        column = definition.get('column')
        min_value = definition.get('min')
        max_value = definition.get('max')

        if not column:
            return ValidationResult(passed=True)

        value = row.get(column)
        return self.validate_range(value, min_value, max_value, column)

    def _validate_pattern_rule(self, row: Dict[str, Any], rule: Dict,
                               definition: Dict) -> ValidationResult:
        """Validate PATTERN rule."""
        column = definition.get('column')
        pattern = definition.get('pattern')

        if not column or not pattern:
            return ValidationResult(passed=True)

        value = row.get(column)
        return self.validate_pattern(value, pattern, column)

    def _validate_length_rule(self, row: Dict[str, Any], rule: Dict,
                             definition: Dict) -> ValidationResult:
        """Validate LENGTH rule."""
        column = definition.get('column')
        min_length = definition.get('min')
        max_length = definition.get('max')

        if not column:
            return ValidationResult(passed=True)

        value = row.get(column)
        return self.validate_length(value, min_length, max_length, column)

    def _validate_date_range_rule(self, row: Dict[str, Any], rule: Dict,
                                  definition: Dict) -> ValidationResult:
        """Validate DATE_RANGE rule."""
        column = definition.get('column')
        min_date = definition.get('min_date')
        max_date = definition.get('max_date')

        if not column:
            return ValidationResult(passed=True)

        # Convert string dates to date objects
        if isinstance(min_date, str):
            from datetime import datetime
            if min_date == 'CURRENT_DATE':
                min_date = date.today()
            else:
                min_date = datetime.strptime(min_date, '%Y-%m-%d').date()

        if isinstance(max_date, str):
            from datetime import datetime
            if max_date == 'CURRENT_DATE':
                max_date = date.today()
            else:
                max_date = datetime.strptime(max_date, '%Y-%m-%d').date()

        value = row.get(column)
        return self.validate_date_range(value, min_date, max_date, column)

    def _validate_custom_rule(self, row: Dict[str, Any], rule: Dict) -> ValidationResult:
        """Validate CUSTOM rule using SQL expression."""
        # TODO: Implement custom SQL validation
        # This would require evaluating SQL expressions against the row
        logger.warning(f"Custom rule validation not yet implemented: {rule['rule_code']}")
        return ValidationResult(passed=True)

    def _get_row_id(self, row: Dict[str, Any]) -> str:
        """
        Get row identifier for logging.

        Args:
            row: Row dictionary

        Returns:
            str: Row identifier
        """
        # Use first few values as identifier
        values = list(row.values())[:3]
        return ":".join([str(v) for v in values if v is not None])
