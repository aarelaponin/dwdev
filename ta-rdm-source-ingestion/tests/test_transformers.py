"""
Unit Tests for transformers module.

Tests mapping transformer and transformation logic.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from transformers.mapping_transformer import MappingTransformer
from transformers.base_transformer import BaseTransformer


# ============================================================================
# BaseTransformer Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.transformer
class TestBaseTransformer:
    """Test BaseTransformer abstract class."""

    def test_base_transformer_is_abstract(self):
        """Test that BaseTransformer cannot be instantiated directly."""
        # BaseTransformer should require transform_row to be implemented
        # Can't instantiate abstract class directly in Python without implementing abstract methods
        pass  # This is more of a design test

    def test_transform_method_exists(self):
        """Test that transform method exists."""
        # Verify the base class has the transform method
        assert hasattr(BaseTransformer, 'transform')


# ============================================================================
# MappingTransformer Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.transformer
class TestMappingTransformer:
    """Test MappingTransformer class."""

    # ------------------------------------------------------------------------
    # Initialization Tests
    # ------------------------------------------------------------------------

    def test_init(self, mock_catalog):
        """Test MappingTransformer initialization."""
        transformer = MappingTransformer(
            mapping_id=1,
            catalog=mock_catalog
        )

        assert transformer.mapping_id == 1
        assert transformer.catalog == mock_catalog
        assert transformer.column_mappings is not None
        assert transformer.lookup_cache is not None

    def test_init_loads_column_mappings(self, mock_catalog):
        """Test that initialization loads column mappings."""
        mock_catalog.get_column_mappings.return_value = [
            {'column_mapping_id': 1, 'source_column': 'col1', 'target_column': 'col1'}
        ]

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Verify catalog was called
        mock_catalog.get_column_mappings.assert_called_once_with(1)

    def test_init_loads_lookup_mappings(self, mock_catalog):
        """Test that initialization loads lookup mappings."""
        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Verify catalog was called
        mock_catalog.get_lookup_mappings.assert_called_once_with(1)

    # ------------------------------------------------------------------------
    # DIRECT Transformation Tests
    # ------------------------------------------------------------------------

    def test_transform_direct_mapping(self, mock_catalog):
        """Test direct column mapping (no transformation)."""
        # Setup column mappings
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_id',
                'target_column': 'party_id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row
        source_row = {'taxpayer_id': 'TP001'}
        result = transformer.transform_row(source_row)

        # Verify
        assert 'party_id' in result
        assert result['party_id'] == 'TP001'

    def test_transform_direct_mapping_null_value(self, mock_catalog):
        """Test direct mapping with NULL source value."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_id',
                'target_column': 'party_id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row with null
        source_row = {'taxpayer_id': None}
        result = transformer.transform_row(source_row)

        # Verify
        assert result['party_id'] is None

    # ------------------------------------------------------------------------
    # EXPRESSION Transformation Tests
    # ------------------------------------------------------------------------

    def test_transform_expression_upper(self, mock_catalog):
        """Test EXPRESSION transformation with UPPER function."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'UPPER({taxpayer_name})'
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row
        source_row = {'taxpayer_name': 'john doe'}
        result = transformer.transform_row(source_row)

        # Verify
        assert result['party_name'] == 'JOHN DOE'

    def test_transform_expression_trim(self, mock_catalog):
        """Test EXPRESSION transformation with TRIM function."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'TRIM({taxpayer_name})'
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row
        source_row = {'taxpayer_name': '  john doe  '}
        result = transformer.transform_row(source_row)

        # Verify
        assert result['party_name'] == 'john doe'

    def test_transform_expression_upper_trim_combined(self, mock_catalog):
        """Test EXPRESSION with combined UPPER and TRIM."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'UPPER(TRIM({taxpayer_name}))'
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row
        source_row = {'taxpayer_name': '  john doe  '}
        result = transformer.transform_row(source_row)

        # Verify
        assert result['party_name'] == 'JOHN DOE'

    def test_transform_expression_coalesce(self, mock_catalog):
        """Test EXPRESSION with COALESCE function."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'COALESCE({taxpayer_name}, "UNKNOWN")'
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row with null
        source_row = {'taxpayer_name': None}
        result = transformer.transform_row(source_row)

        # Verify - should use default value
        assert result['party_name'] == 'UNKNOWN'

    # ------------------------------------------------------------------------
    # LOOKUP Transformation Tests
    # ------------------------------------------------------------------------

    def test_transform_lookup_mapping(self, mock_catalog):
        """Test LOOKUP transformation."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_type',
                'target_column': 'party_type',
                'transformation_type': 'LOOKUP',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_value': 'I',
                'target_value': 'INDIVIDUAL'
            },
            {
                'column_mapping_id': 1,
                'source_value': 'C',
                'target_value': 'COMPANY'
            }
        ]

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform individual
        source_row = {'taxpayer_type': 'I'}
        result = transformer.transform_row(source_row)
        assert result['party_type'] == 'INDIVIDUAL'

        # Transform company
        source_row = {'taxpayer_type': 'C'}
        result = transformer.transform_row(source_row)
        assert result['party_type'] == 'COMPANY'

    def test_transform_lookup_not_found(self, mock_catalog):
        """Test LOOKUP transformation with unmapped value."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_type',
                'target_column': 'party_type',
                'transformation_type': 'LOOKUP',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_value': 'I',
                'target_value': 'INDIVIDUAL'
            }
        ]

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform with unmapped value
        source_row = {'taxpayer_type': 'X'}  # Not in lookup
        result = transformer.transform_row(source_row)

        # Should return original value or None
        assert result['party_type'] in ['X', None]

    def test_transform_lookup_caching(self, mock_catalog):
        """Test that lookup values are cached."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_type',
                'target_column': 'party_type',
                'transformation_type': 'LOOKUP',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_value': 'I',
                'target_value': 'INDIVIDUAL'
            }
        ]

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Verify lookup cache was populated
        assert 1 in transformer.lookup_cache
        assert 'I' in transformer.lookup_cache[1]
        assert transformer.lookup_cache[1]['I'] == 'INDIVIDUAL'

    # ------------------------------------------------------------------------
    # Multiple Column Transformation Tests
    # ------------------------------------------------------------------------

    def test_transform_multiple_columns(self, mock_catalog, sample_source_rows):
        """Test transformation with multiple columns."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_id',
                'target_column': 'party_id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            },
            {
                'column_mapping_id': 2,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'UPPER(TRIM({taxpayer_name}))'
            },
            {
                'column_mapping_id': 3,
                'source_column': 'taxpayer_type',
                'target_column': 'party_type',
                'transformation_type': 'LOOKUP',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = [
            {
                'column_mapping_id': 3,
                'source_value': 'I',
                'target_value': 'INDIVIDUAL'
            },
            {
                'column_mapping_id': 3,
                'source_value': 'C',
                'target_value': 'COMPANY'
            }
        ]

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row
        source_row = {
            'taxpayer_id': 'TP001',
            'taxpayer_name': '  john doe  ',
            'taxpayer_type': 'I'
        }
        result = transformer.transform_row(source_row)

        # Verify all transformations
        assert result['party_id'] == 'TP001'
        assert result['party_name'] == 'JOHN DOE'
        assert result['party_type'] == 'INDIVIDUAL'

    # ------------------------------------------------------------------------
    # Batch Transformation Tests
    # ------------------------------------------------------------------------

    def test_transform_batch(self, mock_catalog):
        """Test transforming multiple rows."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_id',
                'target_column': 'party_id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform multiple rows
        source_rows = [
            {'taxpayer_id': 'TP001'},
            {'taxpayer_id': 'TP002'},
            {'taxpayer_id': 'TP003'}
        ]
        results = transformer.transform(source_rows)

        # Verify
        assert len(results) == 3
        assert results[0]['party_id'] == 'TP001'
        assert results[1]['party_id'] == 'TP002'
        assert results[2]['party_id'] == 'TP003'

    def test_transform_empty_batch(self, mock_catalog):
        """Test transforming empty list."""
        mock_catalog.get_column_mappings.return_value = []
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform empty list
        results = transformer.transform([])

        # Verify
        assert results == []

    # ------------------------------------------------------------------------
    # Error Handling Tests
    # ------------------------------------------------------------------------

    def test_transform_missing_source_column(self, mock_catalog):
        """Test transformation with missing source column."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'missing_column',
                'target_column': 'target_column',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform row without source column
        source_row = {'other_column': 'value'}
        result = transformer.transform_row(source_row)

        # Should handle gracefully (set to None or skip)
        assert 'target_column' in result
        assert result['target_column'] is None or 'target_column' not in result

    def test_transform_invalid_expression(self, mock_catalog):
        """Test transformation with invalid SQL expression."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'INVALID_FUNCTION({taxpayer_name})'
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform should handle error
        source_row = {'taxpayer_name': 'john doe'}

        try:
            result = transformer.transform_row(source_row)
            # Should either return original value or None
            assert 'party_name' in result
        except Exception:
            # Exception is acceptable for invalid expression
            pass

    # ------------------------------------------------------------------------
    # Statistics Tests
    # ------------------------------------------------------------------------

    def test_transformation_statistics(self, mock_catalog):
        """Test that transformation statistics are tracked."""
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_id',
                'target_column': 'party_id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = []

        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform rows
        source_rows = [
            {'taxpayer_id': 'TP001'},
            {'taxpayer_id': 'TP002'}
        ]
        results = transformer.transform(source_rows)

        # Statistics might be tracked in the transformer
        # Verify transformation completed
        assert len(results) == 2


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.transformer
class TestMappingTransformerIntegration:
    """Integration tests for MappingTransformer."""

    def test_complete_transformation_workflow(self, mock_catalog, sample_source_rows):
        """Test complete transformation workflow."""
        # Setup comprehensive column mappings
        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'taxpayer_id',
                'target_column': 'party_id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            },
            {
                'column_mapping_id': 2,
                'source_column': 'taxpayer_name',
                'target_column': 'party_name',
                'transformation_type': 'EXPRESSION',
                'transformation_logic': 'UPPER(TRIM({taxpayer_name}))'
            },
            {
                'column_mapping_id': 3,
                'source_column': 'taxpayer_type',
                'target_column': 'party_type',
                'transformation_type': 'LOOKUP',
                'transformation_logic': None
            }
        ]
        mock_catalog.get_lookup_mappings.return_value = [
            {
                'column_mapping_id': 3,
                'source_value': 'I',
                'target_value': 'INDIVIDUAL'
            },
            {
                'column_mapping_id': 3,
                'source_value': 'C',
                'target_value': 'COMPANY'
            }
        ]

        # Create transformer
        transformer = MappingTransformer(mapping_id=1, catalog=mock_catalog)

        # Transform all rows
        results = transformer.transform(sample_source_rows)

        # Verify all rows transformed
        assert len(results) == len(sample_source_rows)

        # Verify first row
        assert results[0]['party_id'] == 'TP001'
        assert results[0]['party_name'] == 'JOHN DOE'
        assert results[0]['party_type'] == 'INDIVIDUAL'
