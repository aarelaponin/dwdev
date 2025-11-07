"""
Unit Tests for validators module.

Tests schema validator and data quality rules.
"""

import pytest
from unittest.mock import Mock
from validators.schema_validator import SchemaValidator
from validators.base_validator import ValidationResult


# ============================================================================
# ValidationResult Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.validator
class TestValidationResult:
    """Test ValidationResult class."""

    def test_create_passed_result(self):
        """Test creating passed validation result."""
        result = ValidationResult(
            passed=True,
            rule_code='TEST_RULE',
            column='test_column',
            value='test_value',
            message='Validation passed'
        )

        assert result.passed is True
        assert result.rule_code == 'TEST_RULE'
        assert result.column == 'test_column'
        assert result.value == 'test_value'
        assert result.message == 'Validation passed'

    def test_create_failed_result(self):
        """Test creating failed validation result."""
        result = ValidationResult(
            passed=False,
            rule_code='NOT_NULL',
            column='party_id',
            value=None,
            message='Column cannot be null'
        )

        assert result.passed is False
        assert result.rule_code == 'NOT_NULL'
        assert result.message == 'Column cannot be null'


# ============================================================================
# SchemaValidator Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.validator
class TestSchemaValidator:
    """Test SchemaValidator class."""

    # ------------------------------------------------------------------------
    # Initialization Tests
    # ------------------------------------------------------------------------

    def test_init(self, mock_catalog):
        """Test SchemaValidator initialization."""
        validator = SchemaValidator(
            mapping_id=1,
            catalog=mock_catalog,
            action_mode='REJECT'
        )

        assert validator.mapping_id == 1
        assert validator.catalog == mock_catalog
        assert validator.action_mode == 'REJECT'

    def test_init_loads_dq_rules(self, mock_catalog):
        """Test that initialization loads DQ rules."""
        mock_catalog.get_dq_rules.return_value = [
            {'rule_id': 1, 'rule_type': 'NOT_NULL', 'column_name': 'party_id'}
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Verify catalog was called
        mock_catalog.get_dq_rules.assert_called_once_with(1, active_only=True)

    def test_init_different_action_modes(self, mock_catalog):
        """Test initialization with different action modes."""
        modes = ['REJECT', 'LOG', 'SKIP']

        for mode in modes:
            validator = SchemaValidator(
                mapping_id=1,
                catalog=mock_catalog,
                action_mode=mode
            )
            assert validator.action_mode == mode

    # ------------------------------------------------------------------------
    # NOT_NULL Rule Tests
    # ------------------------------------------------------------------------

    def test_validate_not_null_pass(self, mock_catalog):
        """Test NOT_NULL rule with valid value."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with non-null value
        row = {'party_id': 'TP001', 'party_name': 'John Doe'}
        results = validator.validate_row(row)

        # Find NOT_NULL result
        not_null_result = [r for r in results if r.rule_code == 'PARTY_ID_NOT_NULL'][0]
        assert not_null_result.passed is True

    def test_validate_not_null_fail(self, mock_catalog):
        """Test NOT_NULL rule with null value."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with null value
        row = {'party_id': None, 'party_name': 'John Doe'}
        results = validator.validate_row(row)

        # Find NOT_NULL result
        not_null_result = [r for r in results if r.rule_code == 'PARTY_ID_NOT_NULL'][0]
        assert not_null_result.passed is False

    # ------------------------------------------------------------------------
    # LENGTH Rule Tests
    # ------------------------------------------------------------------------

    def test_validate_length_pass(self, mock_catalog):
        """Test LENGTH rule with valid length."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"min_length": 2, "max_length": 100}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with valid length
        row = {'party_name': 'John Doe'}
        results = validator.validate_row(row)

        # Find LENGTH result
        length_result = [r for r in results if r.rule_code == 'PARTY_NAME_LENGTH'][0]
        assert length_result.passed is True

    def test_validate_length_too_short(self, mock_catalog):
        """Test LENGTH rule with value too short."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"min_length": 5}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with short value
        row = {'party_name': 'Bob'}  # Only 3 characters
        results = validator.validate_row(row)

        # Find LENGTH result
        length_result = [r for r in results if r.rule_code == 'PARTY_NAME_LENGTH'][0]
        assert length_result.passed is False

    def test_validate_length_too_long(self, mock_catalog):
        """Test LENGTH rule with value too long."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"max_length": 10}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with long value
        row = {'party_name': 'This is a very long name'}
        results = validator.validate_row(row)

        # Find LENGTH result
        length_result = [r for r in results if r.rule_code == 'PARTY_NAME_LENGTH'][0]
        assert length_result.passed is False

    # ------------------------------------------------------------------------
    # PATTERN Rule Tests
    # ------------------------------------------------------------------------

    def test_validate_pattern_pass(self, mock_catalog):
        """Test PATTERN rule with matching value."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'EMAIL_PATTERN',
                'rule_type': 'PATTERN',
                'column_name': 'email',
                'rule_parameters': '{"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$"}',
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with valid email
        row = {'email': 'john.doe@example.com'}
        results = validator.validate_row(row)

        # Find PATTERN result
        pattern_result = [r for r in results if r.rule_code == 'EMAIL_PATTERN'][0]
        assert pattern_result.passed is True

    def test_validate_pattern_fail(self, mock_catalog):
        """Test PATTERN rule with non-matching value."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'EMAIL_PATTERN',
                'rule_type': 'PATTERN',
                'column_name': 'email',
                'rule_parameters': '{"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$"}',
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with invalid email
        row = {'email': 'invalid-email'}
        results = validator.validate_row(row)

        # Find PATTERN result
        pattern_result = [r for r in results if r.rule_code == 'EMAIL_PATTERN'][0]
        assert pattern_result.passed is False

    # ------------------------------------------------------------------------
    # RANGE Rule Tests
    # ------------------------------------------------------------------------

    def test_validate_range_pass(self, mock_catalog):
        """Test RANGE rule with value in range."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'AGE_RANGE',
                'rule_type': 'RANGE',
                'column_name': 'age',
                'rule_parameters': '{"min_value": 0, "max_value": 120}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with valid age
        row = {'age': 25}
        results = validator.validate_row(row)

        # Find RANGE result
        range_result = [r for r in results if r.rule_code == 'AGE_RANGE'][0]
        assert range_result.passed is True

    def test_validate_range_below_min(self, mock_catalog):
        """Test RANGE rule with value below minimum."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'AGE_RANGE',
                'rule_type': 'RANGE',
                'column_name': 'age',
                'rule_parameters': '{"min_value": 0}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with negative age
        row = {'age': -5}
        results = validator.validate_row(row)

        # Find RANGE result
        range_result = [r for r in results if r.rule_code == 'AGE_RANGE'][0]
        assert range_result.passed is False

    def test_validate_range_above_max(self, mock_catalog):
        """Test RANGE rule with value above maximum."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'AGE_RANGE',
                'rule_type': 'RANGE',
                'column_name': 'age',
                'rule_parameters': '{"max_value": 120}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with excessive age
        row = {'age': 150}
        results = validator.validate_row(row)

        # Find RANGE result
        range_result = [r for r in results if r.rule_code == 'AGE_RANGE'][0]
        assert range_result.passed is False

    # ------------------------------------------------------------------------
    # IN_LIST Rule Tests
    # ------------------------------------------------------------------------

    def test_validate_in_list_pass(self, mock_catalog):
        """Test IN_LIST rule with value in allowed list."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_TYPE_LIST',
                'rule_type': 'IN_LIST',
                'column_name': 'party_type',
                'rule_parameters': '{"allowed_values": ["INDIVIDUAL", "COMPANY", "PARTNERSHIP"]}',
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with allowed value
        row = {'party_type': 'INDIVIDUAL'}
        results = validator.validate_row(row)

        # Find IN_LIST result
        in_list_result = [r for r in results if r.rule_code == 'PARTY_TYPE_LIST'][0]
        assert in_list_result.passed is True

    def test_validate_in_list_fail(self, mock_catalog):
        """Test IN_LIST rule with value not in allowed list."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_TYPE_LIST',
                'rule_type': 'IN_LIST',
                'column_name': 'party_type',
                'rule_parameters': '{"allowed_values": ["INDIVIDUAL", "COMPANY"]}',
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with disallowed value
        row = {'party_type': 'UNKNOWN'}
        results = validator.validate_row(row)

        # Find IN_LIST result
        in_list_result = [r for r in results if r.rule_code == 'PARTY_TYPE_LIST'][0]
        assert in_list_result.passed is False

    # ------------------------------------------------------------------------
    # Batch Validation Tests
    # ------------------------------------------------------------------------

    def test_validate_multiple_rows(self, mock_catalog):
        """Test validating multiple rows."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(
            mapping_id=1,
            catalog=mock_catalog,
            action_mode='REJECT'
        )

        # Validate batch with mix of valid and invalid
        rows = [
            {'party_id': 'TP001'},  # Valid
            {'party_id': None},     # Invalid
            {'party_id': 'TP003'}   # Valid
        ]
        valid_rows, violations = validator.validate(rows)

        # Verify results
        assert len(valid_rows) == 2  # Two valid rows
        assert len(violations) > 0    # At least one violation

    def test_validate_action_mode_reject(self, mock_catalog):
        """Test REJECT action mode."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            }
        ]

        validator = SchemaValidator(
            mapping_id=1,
            catalog=mock_catalog,
            action_mode='REJECT'
        )

        # Validate row with error
        rows = [{'party_id': None}]
        valid_rows, violations = validator.validate(rows)

        # In REJECT mode, row should be rejected
        assert len(valid_rows) == 0
        assert len(violations) > 0

    def test_validate_action_mode_log(self, mock_catalog):
        """Test LOG action mode."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"min_length": 5}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(
            mapping_id=1,
            catalog=mock_catalog,
            action_mode='LOG'
        )

        # Validate row with warning
        rows = [{'party_name': 'Bob'}]  # Too short
        valid_rows, violations = validator.validate(rows)

        # In LOG mode, row is still included but logged
        # (depending on implementation)
        assert violations is not None

    # ------------------------------------------------------------------------
    # Multiple Rules Tests
    # ------------------------------------------------------------------------

    def test_validate_multiple_rules_all_pass(self, mock_catalog):
        """Test validation with multiple rules all passing."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            },
            {
                'rule_id': 2,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"min_length": 2, "max_length": 100}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row that passes all rules
        row = {'party_id': 'TP001', 'party_name': 'John Doe'}
        results = validator.validate_row(row)

        # All rules should pass
        assert all(r.passed for r in results)

    def test_validate_multiple_rules_some_fail(self, mock_catalog):
        """Test validation with some rules failing."""
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            },
            {
                'rule_id': 2,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"min_length": 10}',
                'severity': 'WARNING'
            }
        ]

        validator = SchemaValidator(mapping_id=1, catalog=mock_catalog)

        # Validate row with one rule failing
        row = {'party_id': 'TP001', 'party_name': 'Bob'}  # Name too short
        results = validator.validate_row(row)

        # At least one rule should fail
        failed_results = [r for r in results if not r.passed]
        assert len(failed_results) > 0


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.validator
class TestSchemaValidatorIntegration:
    """Integration tests for SchemaValidator."""

    def test_complete_validation_workflow(self, mock_catalog, sample_transformed_rows):
        """Test complete validation workflow."""
        # Setup comprehensive DQ rules
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'PARTY_ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'party_id',
                'rule_parameters': None,
                'severity': 'ERROR'
            },
            {
                'rule_id': 2,
                'rule_code': 'PARTY_NAME_LENGTH',
                'rule_type': 'LENGTH',
                'column_name': 'party_name',
                'rule_parameters': '{"min_length": 2, "max_length": 255}',
                'severity': 'WARNING'
            },
            {
                'rule_id': 3,
                'rule_code': 'PARTY_TYPE_LIST',
                'rule_type': 'IN_LIST',
                'column_name': 'party_type',
                'rule_parameters': '{"allowed_values": ["INDIVIDUAL", "COMPANY", "PARTNERSHIP"]}',
                'severity': 'ERROR'
            }
        ]

        # Create validator
        validator = SchemaValidator(
            mapping_id=1,
            catalog=mock_catalog,
            action_mode='REJECT'
        )

        # Validate all rows
        valid_rows, violations = validator.validate(sample_transformed_rows)

        # All sample rows should be valid
        assert len(valid_rows) == len(sample_transformed_rows)
        # No violations expected for valid data
        assert len([v for v in violations if not v.passed]) == 0
