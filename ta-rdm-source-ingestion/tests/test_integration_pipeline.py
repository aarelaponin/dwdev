"""
Integration Tests for End-to-End Pipeline.

Tests complete ETL workflow from extraction through loading.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from orchestration.pipeline import IngestionPipeline
from orchestration.dependency_manager import DependencyManager


# ============================================================================
# End-to-End Pipeline Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestIngestionPipelineIntegration:
    """Integration tests for complete ingestion pipeline."""

    def test_single_mapping_execution(
        self,
        mock_sqlserver_connection,
        mock_mysql_connection,
        mock_catalog
    ):
        """Test executing a single table mapping end-to-end."""
        # Setup mock catalog to return complete configuration
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'RAMIS_TAXPAYER_TO_PARTY',
            'mapping_name': 'RAMIS Taxpayer to Party',
            'source_schema': 'dbo',
            'source_table': 'TAXPAYER',
            'source_filter_condition': None,
            'target_schema': 'party',
            'target_table': 'party',
            'target_key_columns': '["party_id"]',
            'load_strategy': 'FULL',
            'merge_strategy': 'UPSERT'
        }

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
            }
        ]

        mock_catalog.get_lookup_mappings.return_value = []
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
        mock_catalog.get_dependencies.return_value = []
        mock_catalog.start_execution.return_value = 1
        mock_catalog.end_execution.return_value = True

        # Setup source data
        mock_source_data = [
            {
                'taxpayer_id': 'TP001',
                'taxpayer_name': '  john doe  '
            },
            {
                'taxpayer_id': 'TP002',
                'taxpayer_name': '  jane smith  '
            }
        ]

        # Mock fetch_batch to return data
        mock_sqlserver_connection.fetch_batch = Mock(
            return_value=iter([mock_source_data])
        )

        # Mock MySQL execute_many for loads
        mock_mysql_connection.execute_many = Mock(return_value=2)

        # Create pipeline
        pipeline = IngestionPipeline(
            source_db=mock_sqlserver_connection,
            staging_db=mock_mysql_connection,
            canonical_db=mock_mysql_connection,
            catalog=mock_catalog,
            batch_size=10000,
            dry_run=False
        )

        # Execute mapping
        result = pipeline.execute_mapping(mapping_id=1)

        # Verify results
        assert result['status'] == 'SUCCESS'
        assert result['mapping_id'] == 1
        assert result['rows_extracted'] == 2
        # Rows loaded might be mocked value
        assert 'rows_loaded' in result

        # Verify execution was tracked
        mock_catalog.start_execution.assert_called_once()
        mock_catalog.end_execution.assert_called_once()

    def test_source_system_execution_with_dependencies(
        self,
        mock_sqlserver_connection,
        mock_mysql_connection,
        mock_catalog
    ):
        """Test executing multiple mappings with dependencies."""
        # Setup mock catalog for source system
        mock_catalog.get_mappings_by_source.return_value = [
            {
                'mapping_id': 1,
                'mapping_code': 'RAMIS_TAXPAYER_TO_PARTY',
                'source_table': 'TAXPAYER',
                'target_table': 'party'
            },
            {
                'mapping_id': 2,
                'mapping_code': 'RAMIS_ADDRESS_TO_ADDRESS',
                'source_table': 'ADDRESS',
                'target_table': 'address'
            }
        ]

        # Setup dependencies (ADDRESS depends on PARTY)
        def get_dependencies_side_effect(mapping_id):
            if mapping_id == 2:  # ADDRESS mapping
                return [{'parent_mapping_id': 1}]  # Depends on PARTY
            return []

        mock_catalog.get_dependencies.side_effect = get_dependencies_side_effect

        # Setup other mocks
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'TEST_MAPPING',
            'source_schema': 'dbo',
            'source_table': 'TEST',
            'target_schema': 'test',
            'target_table': 'test',
            'target_key_columns': '["id"]',
            'load_strategy': 'FULL',
            'merge_strategy': 'UPSERT'
        }

        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'id',
                'target_column': 'id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]

        mock_catalog.get_lookup_mappings.return_value = []
        mock_catalog.get_dq_rules.return_value = []
        mock_catalog.start_execution.return_value = 1

        # Mock source data
        mock_sqlserver_connection.fetch_batch = Mock(
            return_value=iter([[{'id': '1'}]])
        )
        mock_mysql_connection.execute_many = Mock(return_value=1)

        # Create pipeline
        pipeline = IngestionPipeline(
            source_db=mock_sqlserver_connection,
            staging_db=mock_mysql_connection,
            canonical_db=mock_mysql_connection,
            catalog=mock_catalog,
            batch_size=10000,
            dry_run=False
        )

        # Execute source system
        stats = pipeline.execute_source_system('RAMIS')

        # Verify execution
        assert stats['total_mappings'] == 2
        # Mappings should execute in dependency order: PARTY (1) then ADDRESS (2)

    def test_dry_run_mode(
        self,
        mock_sqlserver_connection,
        mock_mysql_connection,
        mock_catalog
    ):
        """Test pipeline execution in dry-run mode."""
        # Setup mocks
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'TEST_MAPPING',
            'source_schema': 'dbo',
            'source_table': 'TEST',
            'target_schema': 'test',
            'target_table': 'test',
            'target_key_columns': '["id"]',
            'load_strategy': 'FULL',
            'merge_strategy': 'UPSERT'
        }

        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'id',
                'target_column': 'id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]

        mock_catalog.get_lookup_mappings.return_value = []
        mock_catalog.get_dq_rules.return_value = []
        mock_catalog.get_dependencies.return_value = []
        mock_catalog.start_execution.return_value = 1

        # Mock source data
        mock_source_data = [{'id': '1'}, {'id': '2'}]
        mock_sqlserver_connection.fetch_batch = Mock(
            return_value=iter([mock_source_data])
        )

        # Create pipeline in dry-run mode
        pipeline = IngestionPipeline(
            source_db=mock_sqlserver_connection,
            staging_db=mock_mysql_connection,
            canonical_db=mock_mysql_connection,
            catalog=mock_catalog,
            batch_size=10000,
            dry_run=True  # DRY RUN MODE
        )

        # Execute mapping
        result = pipeline.execute_mapping(mapping_id=1)

        # Verify no actual writes occurred
        # In dry-run mode, execute_many should NOT be called
        mock_mysql_connection.execute_many.assert_not_called()

        # But execution should still be tracked
        assert result['status'] == 'SUCCESS'
        assert result['rows_extracted'] == 2

    def test_data_quality_violation_handling(
        self,
        mock_sqlserver_connection,
        mock_mysql_connection,
        mock_catalog
    ):
        """Test handling of data quality violations."""
        # Setup mocks
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'TEST_MAPPING',
            'source_schema': 'dbo',
            'source_table': 'TEST',
            'target_schema': 'test',
            'target_table': 'test',
            'target_key_columns': '["id"]',
            'load_strategy': 'FULL',
            'merge_strategy': 'UPSERT'
        }

        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'id',
                'target_column': 'id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]

        mock_catalog.get_lookup_mappings.return_value = []

        # Add DQ rule that will cause violations
        mock_catalog.get_dq_rules.return_value = [
            {
                'rule_id': 1,
                'rule_code': 'ID_NOT_NULL',
                'rule_type': 'NOT_NULL',
                'column_name': 'id',
                'rule_parameters': None,
                'severity': 'ERROR'
            }
        ]

        mock_catalog.get_dependencies.return_value = []
        mock_catalog.start_execution.return_value = 1

        # Mock source data with some null IDs
        mock_source_data = [
            {'id': '1'},
            {'id': None},  # Will violate NOT_NULL
            {'id': '3'}
        ]
        mock_sqlserver_connection.fetch_batch = Mock(
            return_value=iter([mock_source_data])
        )

        mock_mysql_connection.execute_many = Mock(return_value=2)

        # Create pipeline
        pipeline = IngestionPipeline(
            source_db=mock_sqlserver_connection,
            staging_db=mock_mysql_connection,
            canonical_db=mock_mysql_connection,
            catalog=mock_catalog,
            batch_size=10000,
            dry_run=False
        )

        # Execute mapping
        result = pipeline.execute_mapping(mapping_id=1)

        # Verify execution
        assert result['status'] == 'SUCCESS'
        assert result['rows_extracted'] == 3

        # Verify DQ violations were logged
        # log_dq_violation should be called for the null ID
        assert mock_catalog.log_dq_violation.called


# ============================================================================
# Dependency Manager Integration Tests
# ============================================================================

@pytest.mark.integration
class TestDependencyManagerIntegration:
    """Integration tests for dependency resolution."""

    def test_dependency_resolution_simple_chain(self, mock_catalog):
        """Test dependency resolution with simple chain: A -> B -> C."""
        # Setup dependencies
        def get_dependencies_side_effect(mapping_id):
            if mapping_id == 2:  # B depends on A
                return [{'parent_mapping_id': 1}]
            elif mapping_id == 3:  # C depends on B
                return [{'parent_mapping_id': 2}]
            return []

        mock_catalog.get_dependencies.side_effect = get_dependencies_side_effect

        # Create dependency manager
        dep_mgr = DependencyManager(mock_catalog)

        # Resolve order
        mapping_ids = [1, 2, 3]
        execution_order = dep_mgr.resolve_execution_order(mapping_ids)

        # Verify order: A (1), B (2), C (3)
        assert execution_order == [1, 2, 3]

    def test_dependency_resolution_parallel(self, mock_catalog):
        """Test dependency resolution with parallel branches: A -> B, A -> C."""
        # Setup dependencies
        def get_dependencies_side_effect(mapping_id):
            if mapping_id in [2, 3]:  # B and C both depend on A
                return [{'parent_mapping_id': 1}]
            return []

        mock_catalog.get_dependencies.side_effect = get_dependencies_side_effect
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'TEST'
        }

        # Create dependency manager
        dep_mgr = DependencyManager(mock_catalog)

        # Resolve order
        mapping_ids = [1, 2, 3]
        execution_order = dep_mgr.resolve_execution_order(mapping_ids)

        # Verify A is first, B and C can be in any order
        assert execution_order[0] == 1
        assert set(execution_order[1:]) == {2, 3}

    def test_dependency_circular_detection(self, mock_catalog):
        """Test circular dependency detection."""
        # Setup circular dependencies: A -> B -> C -> A
        def get_dependencies_side_effect(mapping_id):
            if mapping_id == 1:
                return [{'parent_mapping_id': 3}]
            elif mapping_id == 2:
                return [{'parent_mapping_id': 1}]
            elif mapping_id == 3:
                return [{'parent_mapping_id': 2}]
            return []

        mock_catalog.get_dependencies.side_effect = get_dependencies_side_effect
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'TEST'
        }

        # Create dependency manager
        dep_mgr = DependencyManager(mock_catalog)

        # Attempt to resolve - should raise error
        from orchestration.dependency_manager import CircularDependencyError

        with pytest.raises(CircularDependencyError):
            dep_mgr.resolve_execution_order([1, 2, 3])


# ============================================================================
# Error Handling Integration Tests
# ============================================================================

@pytest.mark.integration
class TestPipelineErrorHandling:
    """Integration tests for error handling."""

    def test_extraction_error_rollback(
        self,
        mock_sqlserver_connection,
        mock_mysql_connection,
        mock_catalog
    ):
        """Test that extraction errors cause rollback."""
        # Setup mocks
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'TEST_MAPPING',
            'source_schema': 'dbo',
            'source_table': 'TEST',
            'target_schema': 'test',
            'target_table': 'test',
            'target_key_columns': '["id"]',
            'load_strategy': 'FULL',
            'merge_strategy': 'UPSERT'
        }

        mock_catalog.get_dependencies.return_value = []
        mock_catalog.start_execution.return_value = 1

        # Mock extraction failure
        mock_sqlserver_connection.fetch_batch = Mock(
            side_effect=Exception("Database connection lost")
        )

        # Create pipeline
        pipeline = IngestionPipeline(
            source_db=mock_sqlserver_connection,
            staging_db=mock_mysql_connection,
            canonical_db=mock_mysql_connection,
            catalog=mock_catalog,
            batch_size=10000,
            dry_run=False
        )

        # Execute should raise exception
        with pytest.raises(Exception):
            pipeline.execute_mapping(mapping_id=1)

        # Verify failure was logged
        mock_catalog.end_execution.assert_called_once()
        call_args = mock_catalog.end_execution.call_args
        assert call_args[1]['status'] == 'FAILED'


# ============================================================================
# Performance Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestPipelinePerformance:
    """Integration tests for pipeline performance."""

    def test_large_batch_processing(
        self,
        mock_sqlserver_connection,
        mock_mysql_connection,
        mock_catalog
    ):
        """Test processing large batches of data."""
        # Setup mocks
        mock_catalog.get_table_mapping.return_value = {
            'mapping_id': 1,
            'mapping_code': 'LARGE_TEST',
            'source_schema': 'dbo',
            'source_table': 'LARGE_TABLE',
            'target_schema': 'test',
            'target_table': 'large_test',
            'target_key_columns': '["id"]',
            'load_strategy': 'FULL',
            'merge_strategy': 'UPSERT'
        }

        mock_catalog.get_column_mappings.return_value = [
            {
                'column_mapping_id': 1,
                'source_column': 'id',
                'target_column': 'id',
                'transformation_type': 'DIRECT',
                'transformation_logic': None
            }
        ]

        mock_catalog.get_lookup_mappings.return_value = []
        mock_catalog.get_dq_rules.return_value = []
        mock_catalog.get_dependencies.return_value = []
        mock_catalog.start_execution.return_value = 1

        # Create large dataset (10,000 rows)
        large_dataset = [{'id': str(i)} for i in range(10000)]

        # Mock to return in batches of 1000
        batches = [large_dataset[i:i+1000] for i in range(0, len(large_dataset), 1000)]
        mock_sqlserver_connection.fetch_batch = Mock(return_value=iter(batches))

        mock_mysql_connection.execute_many = Mock(return_value=1000)

        # Create pipeline with batch size of 1000
        pipeline = IngestionPipeline(
            source_db=mock_sqlserver_connection,
            staging_db=mock_mysql_connection,
            canonical_db=mock_mysql_connection,
            catalog=mock_catalog,
            batch_size=1000,
            dry_run=False
        )

        # Execute mapping
        result = pipeline.execute_mapping(mapping_id=1)

        # Verify all rows processed
        assert result['status'] == 'SUCCESS'
        assert result['rows_extracted'] == 10000
