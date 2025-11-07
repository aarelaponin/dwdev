#!/usr/bin/env python3
"""
Run TA-RDM Source Ingestion ETL Pipeline

Main command-line interface for executing source-to-canonical data ingestion.

Usage:
    # Run all mappings for a source system
    python scripts/run_ingestion.py --source RAMIS

    # Run a specific mapping
    python scripts/run_ingestion.py --mapping RAMIS_TAXPAYER_TO_PARTY

    # Run specific mapping by ID
    python scripts/run_ingestion.py --mapping-id 1

    # Dry run (no commits)
    python scripts/run_ingestion.py --source RAMIS --dry-run

    # Custom batch size
    python scripts/run_ingestion.py --source RAMIS --batch-size 5000
"""

import sys
import os
import argparse
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_utils import setup_logging
from utils.db_utils import DatabaseConnection, DatabaseType
from config.database_config import (
    RAMIS_DB_CONFIG, STAGING_DB_CONFIG, L2_DB_CONFIG,
    ETL_CONFIG
)
from metadata.catalog import MetadataCatalog
from orchestration.pipeline import IngestionPipeline

# Setup logging
logger = setup_logging(name='run_ingestion', log_level='INFO')


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='TA-RDM Source Ingestion ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all RAMIS mappings
  python scripts/run_ingestion.py --source RAMIS

  # Run specific mapping by code
  python scripts/run_ingestion.py --mapping RAMIS_TAXPAYER_TO_PARTY

  # Run specific mapping by ID
  python scripts/run_ingestion.py --mapping-id 1

  # Dry run (no commits)
  python scripts/run_ingestion.py --source RAMIS --dry-run

  # Custom batch size
  python scripts/run_ingestion.py --source RAMIS --batch-size 5000

  # List available mappings
  python scripts/run_ingestion.py --list-mappings --source RAMIS
        """
    )

    # Execution mode
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--source',
        type=str,
        help='Source system code (e.g., RAMIS)'
    )
    mode_group.add_argument(
        '--mapping',
        type=str,
        help='Mapping code to execute'
    )
    mode_group.add_argument(
        '--mapping-id',
        type=int,
        help='Mapping ID to execute'
    )
    mode_group.add_argument(
        '--list-mappings',
        action='store_true',
        help='List available mappings'
    )

    # Options
    parser.add_argument(
        '--batch-size',
        type=int,
        default=ETL_CONFIG['batch_size'],
        help=f'Batch size for processing (default: {ETL_CONFIG["batch_size"]})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without committing changes'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue processing if a mapping fails'
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("TA-RDM Source Ingestion Pipeline")
    logger.info("=" * 80)
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("=" * 80)
    logger.info("")

    # Initialize metadata catalog
    try:
        logger.info("Connecting to metadata catalog...")
        catalog = MetadataCatalog()
        catalog.connect()
        logger.info("✓ Connected to metadata catalog")
    except Exception as e:
        logger.error(f"✗ Failed to connect to metadata catalog: {e}")
        sys.exit(1)

    # List mappings mode
    if args.list_mappings:
        list_mappings(catalog, args.source)
        sys.exit(0)

    # Connect to databases
    try:
        logger.info("\nConnecting to databases...")

        # Source database (RAMIS SQL Server)
        logger.info(f"  Connecting to source: {RAMIS_DB_CONFIG['host']}")
        source_db = DatabaseConnection(DatabaseType.SQL_SERVER, RAMIS_DB_CONFIG)
        source_db.connect()
        logger.info("  ✓ Connected to source database")

        # Staging database (MySQL)
        logger.info(f"  Connecting to staging: {STAGING_DB_CONFIG['host']}")
        staging_db = DatabaseConnection(DatabaseType.MYSQL, STAGING_DB_CONFIG)
        staging_db.connect()
        logger.info("  ✓ Connected to staging database")

        # Canonical database (MySQL L2)
        logger.info(f"  Connecting to canonical: {L2_DB_CONFIG['host']}")
        canonical_db = DatabaseConnection(DatabaseType.MYSQL, L2_DB_CONFIG)
        canonical_db.connect()
        logger.info("  ✓ Connected to canonical database")

    except Exception as e:
        logger.error(f"✗ Failed to connect to databases: {e}")
        sys.exit(1)

    # Initialize pipeline
    try:
        logger.info("\nInitializing ETL pipeline...")
        pipeline = IngestionPipeline(
            source_db=source_db,
            staging_db=staging_db,
            canonical_db=canonical_db,
            catalog=catalog,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
        logger.info("✓ Pipeline initialized")
        logger.info("")

    except Exception as e:
        logger.error(f"✗ Failed to initialize pipeline: {e}")
        sys.exit(1)

    # Execute based on mode
    try:
        if args.source:
            # Execute all mappings for source
            logger.info(f"Executing source system: {args.source}")
            stats = pipeline.execute_source_system(args.source)

        elif args.mapping:
            # Execute specific mapping by code
            logger.info(f"Executing mapping: {args.mapping}")
            mapping = catalog.get_table_mapping_by_code(args.mapping)
            if not mapping:
                logger.error(f"Mapping not found: {args.mapping}")
                sys.exit(1)
            stats = pipeline.execute_mapping(mapping['mapping_id'])

        elif args.mapping_id:
            # Execute specific mapping by ID
            logger.info(f"Executing mapping ID: {args.mapping_id}")
            stats = pipeline.execute_mapping(args.mapping_id)

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Status: SUCCESS")
        if 'total_mappings' in stats:
            logger.info(f"Total mappings: {stats['total_mappings']}")
            logger.info(f"Successful: {stats['successful_mappings']}")
            logger.info(f"Failed: {stats['failed_mappings']}")
        logger.info(f"Total rows processed: {stats.get('total_rows_loaded', 0):,}")
        logger.info("=" * 80)

        sys.exit(0)

    except Exception as e:
        logger.error(f"\n✗ Pipeline execution failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)

    finally:
        # Cleanup connections
        logger.info("\nClosing database connections...")
        try:
            source_db.disconnect()
            staging_db.disconnect()
            canonical_db.disconnect()
            catalog.disconnect()
            logger.info("✓ All connections closed")
        except Exception as e:
            logger.warning(f"Error closing connections: {e}")


def list_mappings(catalog: MetadataCatalog, source_code: Optional[str] = None):
    """
    List available mappings.

    Args:
        catalog: Metadata catalog
        source_code: Optional source code to filter
    """
    logger.info("\n" + "=" * 80)
    logger.info("Available Mappings")
    logger.info("=" * 80)

    if source_code:
        mappings = catalog.get_mappings_by_source(source_code, active_only=True)
        logger.info(f"Source System: {source_code}")
    else:
        # Get all mappings
        mappings = catalog.get_all_source_systems(active_only=True)
        logger.info("All Active Source Systems")

    logger.info(f"Total: {len(mappings)}")
    logger.info("-" * 80)

    for mapping in mappings:
        logger.info(f"\nID: {mapping['mapping_id']}")
        logger.info(f"Code: {mapping['mapping_code']}")
        logger.info(f"Name: {mapping['mapping_name']}")
        logger.info(f"Source: {mapping.get('source_schema', 'dbo')}.{mapping['source_table']}")
        logger.info(f"Target: {mapping['target_schema']}.{mapping['target_table']}")
        logger.info(f"Strategy: {mapping.get('load_strategy', 'FULL')}")
        logger.info(f"Merge: {mapping.get('merge_strategy', 'UPSERT')}")

        # Get column count
        column_mappings = catalog.get_column_mappings(mapping['mapping_id'])
        logger.info(f"Columns: {len(column_mappings)}")

        # Get DQ rules count
        dq_rules = catalog.get_dq_rules(mapping['mapping_id'], active_only=True)
        logger.info(f"DQ Rules: {len(dq_rules)}")

    logger.info("\n" + "=" * 80)


if __name__ == '__main__':
    main()
