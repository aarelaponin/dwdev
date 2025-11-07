"""
ETL Pipeline Orchestrator for TA-RDM Source Ingestion.

Coordinates all ETL components (extract, transform, validate, load)
for a complete source-to-canonical ingestion workflow.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from utils.db_utils import DatabaseConnection, DatabaseType
from utils.logging_utils import ETLLogger
from metadata.catalog import MetadataCatalog
from orchestration.dependency_manager import DependencyManager

from extractors.sql_server_extractor import SQLServerExtractor
from transformers.mapping_transformer import MappingTransformer
from validators.schema_validator import SchemaValidator
from loaders.staging_loader import StagingLoader
from loaders.canonical_loader import CanonicalLoader

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Main ETL pipeline orchestrator.

    Coordinates extraction, transformation, validation, and loading
    for complete source-to-canonical data ingestion.
    """

    def __init__(self,
                 source_db: DatabaseConnection,
                 staging_db: DatabaseConnection,
                 canonical_db: DatabaseConnection,
                 catalog: MetadataCatalog,
                 batch_size: int = 10000,
                 dry_run: bool = False):
        """
        Initialize ingestion pipeline.

        Args:
            source_db: Source database connection
            staging_db: Staging database connection
            canonical_db: Canonical database connection
            catalog: Metadata catalog
            batch_size: Batch size for processing
            dry_run: If True, do not commit changes
        """
        self.source_db = source_db
        self.staging_db = staging_db
        self.canonical_db = canonical_db
        self.catalog = catalog
        self.batch_size = batch_size
        self.dry_run = dry_run

        # Initialize components
        self.dependency_manager = DependencyManager(catalog)
        self.etl_logger = ETLLogger()

        # Statistics
        self.pipeline_stats = {
            'total_mappings': 0,
            'successful_mappings': 0,
            'failed_mappings': 0,
            'total_rows_extracted': 0,
            'total_rows_loaded': 0,
            'execution_id': None
        }

        logger.info(
            f"Pipeline initialized (batch_size: {batch_size}, dry_run: {dry_run})"
        )

    def execute_source_system(self, source_code: str) -> Dict[str, Any]:
        """
        Execute all mappings for a source system.

        Args:
            source_code: Source system code (e.g., 'RAMIS')

        Returns:
            Dict: Execution statistics
        """
        logger.info(f"=" * 80)
        logger.info(f"Executing source system: {source_code}")
        logger.info(f"=" * 80)

        # Get all mappings for source
        mappings = self.catalog.get_mappings_by_source(source_code, active_only=True)

        if not mappings:
            logger.warning(f"No active mappings found for source: {source_code}")
            return self.pipeline_stats

        self.pipeline_stats['total_mappings'] = len(mappings)
        logger.info(f"Found {len(mappings)} mappings for {source_code}")

        # Get mapping IDs
        mapping_ids = [m['mapping_id'] for m in mappings]

        # Resolve execution order
        try:
            execution_order = self.dependency_manager.resolve_execution_order(mapping_ids)
            logger.info(f"Execution order resolved: {len(execution_order)} mappings")
        except Exception as e:
            logger.error(f"Failed to resolve dependencies: {e}")
            raise

        # Execute mappings in order
        for mapping_id in execution_order:
            try:
                self.execute_mapping(mapping_id)
                self.pipeline_stats['successful_mappings'] += 1
            except Exception as e:
                logger.error(f"Mapping {mapping_id} failed: {e}")
                self.pipeline_stats['failed_mappings'] += 1
                # Continue with next mapping or abort?
                # For now, continue
                continue

        # Summary
        logger.info(f"\n" + "=" * 80)
        logger.info(f"Source System Execution Complete: {source_code}")
        logger.info(f"=" * 80)
        logger.info(f"Total mappings: {self.pipeline_stats['total_mappings']}")
        logger.info(f"Successful: {self.pipeline_stats['successful_mappings']}")
        logger.info(f"Failed: {self.pipeline_stats['failed_mappings']}")
        logger.info(f"Total rows extracted: {self.pipeline_stats['total_rows_extracted']:,}")
        logger.info(f"Total rows loaded: {self.pipeline_stats['total_rows_loaded']:,}")
        logger.info(f"=" * 80)

        return self.pipeline_stats

    def execute_mapping(self, mapping_id: int) -> Dict[str, Any]:
        """
        Execute a single table mapping.

        Args:
            mapping_id: Table mapping ID

        Returns:
            Dict: Execution statistics
        """
        # Get mapping configuration
        mapping = self.catalog.get_table_mapping(mapping_id)
        if not mapping:
            raise ValueError(f"Mapping ID {mapping_id} not found")

        mapping_code = mapping['mapping_code']
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Executing mapping: {mapping_code} (ID: {mapping_id})")
        logger.info(f"Source: {mapping['source_schema']}.{mapping['source_table']}")
        logger.info(f"Target: {mapping['target_schema']}.{mapping['target_table']}")
        logger.info(f"={'=' * 80}")

        # Log ETL start
        self.etl_logger.log_etl_start(
            process_name=mapping_code,
            source=f"{mapping['source_schema']}.{mapping['source_table']}",
            target=f"{mapping['target_schema']}.{mapping['target_table']}"
        )

        # Start execution tracking
        execution_id = self.catalog.start_execution(
            mapping_id=mapping_id,
            execution_type=mapping['load_strategy'],
            execution_mode='MANUAL',
            triggered_by='PIPELINE'
        )
        self.pipeline_stats['execution_id'] = execution_id

        try:
            # EXTRACT
            extracted_rows = self._extract_phase(mapping)

            # TRANSFORM
            transformed_rows = self._transform_phase(mapping_id, extracted_rows)

            # VALIDATE
            validated_rows, violations = self._validate_phase(
                mapping_id, transformed_rows, execution_id
            )

            # LOAD
            loaded_count = self._load_phase(mapping, validated_rows)

            # Update statistics
            self.pipeline_stats['total_rows_extracted'] += len(extracted_rows)
            self.pipeline_stats['total_rows_loaded'] += loaded_count

            # Log success
            self.catalog.end_execution(
                execution_id=execution_id,
                status='SUCCESS',
                rows_extracted=len(extracted_rows),
                rows_validated=len(validated_rows),
                rows_rejected=len(extracted_rows) - len(validated_rows),
                rows_loaded=loaded_count
            )

            self.etl_logger.log_etl_end(
                process_name=mapping_code,
                status='SUCCESS',
                rows_processed=loaded_count,
                duration_seconds=0  # TODO: Calculate duration
            )

            logger.info(f"✓ Mapping completed successfully: {mapping_code}")

            return {
                'mapping_id': mapping_id,
                'mapping_code': mapping_code,
                'status': 'SUCCESS',
                'rows_extracted': len(extracted_rows),
                'rows_loaded': loaded_count
            }

        except Exception as e:
            # Log failure
            self.catalog.end_execution(
                execution_id=execution_id,
                status='FAILED',
                error_message=str(e)
            )

            self.etl_logger.log_etl_end(
                process_name=mapping_code,
                status='FAILED',
                rows_processed=0,
                duration_seconds=0
            )

            logger.error(f"✗ Mapping failed: {mapping_code} - {e}")
            raise

    def _extract_phase(self, mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        EXTRACT phase - Extract data from source.

        Args:
            mapping: Mapping configuration

        Returns:
            List[Dict]: Extracted rows
        """
        logger.info("PHASE 1: EXTRACT")
        logger.info("-" * 80)

        # Create extractor
        extractor = SQLServerExtractor(self.source_db, batch_size=self.batch_size)

        # Get extraction parameters
        import json
        filter_condition = mapping.get('source_filter_condition')

        # Extract all data (accumulate batches)
        all_rows = []
        for batch in extractor.extract(
            table_name=mapping['source_table'],
            schema=mapping.get('source_schema'),
            filter_condition=filter_condition
        ):
            all_rows.extend(batch)

        logger.info(f"Extracted {len(all_rows):,} rows from source")
        logger.info("-" * 80)

        return all_rows

    def _transform_phase(self, mapping_id: int,
                        rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        TRANSFORM phase - Apply column mappings.

        Args:
            mapping_id: Mapping ID
            rows: Source rows

        Returns:
            List[Dict]: Transformed rows
        """
        logger.info("PHASE 2: TRANSFORM")
        logger.info("-" * 80)

        # Create transformer
        transformer = MappingTransformer(mapping_id, self.catalog)

        # Transform all rows
        transformed_rows = transformer.transform(rows)

        logger.info(f"Transformed {len(transformed_rows):,} rows")
        logger.info("-" * 80)

        return transformed_rows

    def _validate_phase(self, mapping_id: int, rows: List[Dict[str, Any]],
                       execution_id: int) -> tuple:
        """
        VALIDATE phase - Apply data quality rules.

        Args:
            mapping_id: Mapping ID
            rows: Transformed rows
            execution_id: Execution ID for logging violations

        Returns:
            Tuple of (valid_rows, violations)
        """
        logger.info("PHASE 3: VALIDATE")
        logger.info("-" * 80)

        # Create validator
        validator = SchemaValidator(
            mapping_id,
            self.catalog,
            action_mode='REJECT'  # TODO: Make configurable
        )

        # Validate rows
        valid_rows, violations = validator.validate(rows)

        logger.info(f"Validated {len(rows):,} rows")
        logger.info(f"  Passed: {len(valid_rows):,}")
        logger.info(f"  Rejected: {len(rows) - len(valid_rows):,}")
        logger.info(f"  Violations: {len(violations)}")

        # Log violations to database (first 1000)
        if violations:
            for violation in violations[:1000]:
                if not violation.passed:
                    self.catalog.log_dq_violation(
                        execution_id=execution_id,
                        rule_id=None,  # TODO: Get rule_id from violation
                        rule_violated=violation.rule_code,
                        source_table='',  # TODO: Get from mapping
                        source_row_id='',
                        error_message=violation.message,
                        column_name=violation.column,
                        column_value=str(violation.value) if violation.value else None,
                        action_taken='REJECTED'
                    )

        logger.info("-" * 80)

        return valid_rows, violations

    def _load_phase(self, mapping: Dict[str, Any],
                   rows: List[Dict[str, Any]]) -> int:
        """
        LOAD phase - Load to staging and canonical.

        Args:
            mapping: Mapping configuration
            rows: Validated rows

        Returns:
            int: Number of rows loaded to canonical
        """
        logger.info("PHASE 4: LOAD")
        logger.info("-" * 80)

        if self.dry_run:
            logger.warning("DRY RUN mode - skipping actual load")
            return len(rows)

        # Load to staging
        staging_loader = StagingLoader(
            self.staging_db,
            batch_size=self.batch_size,
            truncate_before_load=True
        )

        staging_table = f"stg_{mapping['target_table']}"
        staging_loader.load(
            rows,
            table_name=staging_table,
            schema='staging'
        )

        logger.info(f"Loaded {len(rows):,} rows to staging.{staging_table}")

        # Load to canonical
        canonical_loader = CanonicalLoader(
            self.canonical_db,
            batch_size=self.batch_size,
            merge_strategy=mapping.get('merge_strategy', 'UPSERT')
        )

        # Get key columns from mapping
        import json
        key_columns_json = mapping.get('target_key_columns', '[]')
        key_columns = json.loads(key_columns_json) if key_columns_json else []

        rows_loaded = canonical_loader.load(
            rows,
            table_name=mapping['target_table'],
            schema=mapping['target_schema'],
            key_columns=key_columns
        )

        logger.info(
            f"Loaded {rows_loaded:,} rows to "
            f"{mapping['target_schema']}.{mapping['target_table']}"
        )
        logger.info("-" * 80)

        return rows_loaded
