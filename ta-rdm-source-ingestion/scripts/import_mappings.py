#!/usr/bin/env python3
"""
Import Mapping Configurations from YAML

Parses YAML mapping configuration files and populates the
configuration database with source systems, table mappings,
column mappings, lookups, and data quality rules.

Usage:
    python scripts/import_mappings.py --config metadata/mappings/ramis_example.yaml
    python scripts/import_mappings.py --config metadata/mappings/ramis_example.yaml --dry-run
    python scripts/import_mappings.py --config metadata/mappings/ramis_example.yaml --update
"""

import sys
import os
import argparse
import yaml
import json
from typing import Dict, Any, List
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_utils import setup_logging
from utils.db_utils import DatabaseConnection, DatabaseType
from config.database_config import CONFIG_DB_CONFIG

# Setup logging
logger = setup_logging(name='import_mappings', log_level='INFO')


class MappingImporter:
    """Import YAML mapping configurations into configuration database."""

    def __init__(self, db_config: Dict[str, Any], dry_run: bool = False):
        """
        Initialize mapping importer.

        Args:
            db_config: Database configuration
            dry_run: If True, do not commit changes
        """
        self.db_config = db_config
        self.dry_run = dry_run
        self.db: DatabaseConnection = None
        self.stats = {
            'source_systems': 0,
            'table_mappings': 0,
            'column_mappings': 0,
            'lookup_mappings': 0,
            'dq_rules': 0
        }

    def connect(self):
        """Connect to configuration database."""
        self.db = DatabaseConnection(DatabaseType.MYSQL, self.db_config)
        self.db.connect()
        logger.info("Connected to configuration database")

    def disconnect(self):
        """Disconnect from database."""
        if self.db:
            self.db.disconnect()

    def load_yaml_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load and parse YAML configuration file.

        Args:
            file_path: Path to YAML file

        Returns:
            Dict: Parsed YAML content
        """
        logger.info(f"Loading YAML file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"✓ YAML file loaded successfully")
            return config

        except FileNotFoundError:
            logger.error(f"✗ YAML file not found: {file_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"✗ YAML parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"✗ Error loading YAML: {e}")
            raise

    def import_source_system(self, source_config: Dict[str, Any],
                            created_by: str = 'ADMIN') -> int:
        """
        Import source system configuration.

        Args:
            source_config: Source system configuration from YAML
            created_by: User who created the configuration

        Returns:
            int: source_system_id
        """
        logger.info(f"Importing source system: {source_config['source_code']}")

        # Check if exists
        check_query = """
            SELECT source_system_id
            FROM config.source_systems
            WHERE source_code = %s
        """
        existing = self.db.fetch_one(check_query, (source_config['source_code'],))

        connection = source_config.get('connection', {})
        extraction = source_config.get('extraction', {})

        if existing:
            source_system_id = existing[0]
            logger.info(f"  Source system already exists (ID: {source_system_id})")
            # TODO: Add update logic
            return source_system_id

        # Insert new source system
        insert_query = """
            INSERT INTO config.source_systems (
                source_code, source_name, source_type,
                connection_host, connection_port, connection_database, connection_schema,
                extraction_method, extraction_schedule, incremental_column, batch_size,
                is_active, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            source_config['source_code'],
            source_config['source_name'],
            source_config['source_type'],
            connection.get('host'),
            connection.get('port'),
            connection.get('database'),
            connection.get('schema'),
            extraction.get('method'),
            extraction.get('schedule'),
            extraction.get('incremental_column'),
            extraction.get('batch_size', 10000),
            True,
            created_by
        )

        self.db.execute_query(insert_query, params)
        source_system_id = self.db.fetch_one("SELECT LAST_INSERT_ID()")[0]

        self.stats['source_systems'] += 1
        logger.info(f"  ✓ Created source system (ID: {source_system_id})")

        return source_system_id

    def import_table_mapping(self, source_system_id: int,
                            mapping_config: Dict[str, Any],
                            created_by: str = 'ADMIN') -> int:
        """
        Import table mapping configuration.

        Args:
            source_system_id: Source system ID
            mapping_config: Table mapping configuration from YAML
            created_by: User who created the configuration

        Returns:
            int: mapping_id
        """
        mapping_code = mapping_config['mapping_code']
        logger.info(f"  Importing table mapping: {mapping_code}")

        source = mapping_config['source']
        target = mapping_config['target']
        load_config = mapping_config.get('load_config', {})

        # Check if exists
        check_query = """
            SELECT mapping_id
            FROM config.table_mappings
            WHERE mapping_code = %s
        """
        existing = self.db.fetch_one(check_query, (mapping_code,))

        if existing:
            mapping_id = existing[0]
            logger.info(f"    Table mapping already exists (ID: {mapping_id})")
            # TODO: Add update logic
            return mapping_id

        # Insert table mapping
        insert_query = """
            INSERT INTO config.table_mappings (
                mapping_code, mapping_name, source_system_id,
                source_schema, source_table, source_key_columns, source_filter_condition,
                target_schema, target_table, target_key_columns,
                mapping_type, load_strategy, merge_strategy, load_priority,
                is_active, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            mapping_code,
            mapping_config['mapping_name'],
            source_system_id,
            source.get('schema'),
            source['table'],
            json.dumps(source.get('key_columns', [])),
            source.get('filter'),
            target['schema'],
            target['table'],
            json.dumps(target.get('key_columns', [])),
            load_config.get('type', 'DIRECT'),
            load_config.get('strategy', 'FULL'),
            load_config.get('merge', 'UPSERT'),
            load_config.get('priority', 100),
            True,
            created_by
        )

        self.db.execute_query(insert_query, params)
        mapping_id = self.db.fetch_one("SELECT LAST_INSERT_ID()")[0]

        self.stats['table_mappings'] += 1
        logger.info(f"    ✓ Created table mapping (ID: {mapping_id})")

        return mapping_id

    def import_column_mappings(self, mapping_id: int,
                              column_mappings: List[Dict[str, Any]],
                              created_by: str = 'ADMIN'):
        """
        Import column mapping configurations.

        Args:
            mapping_id: Table mapping ID
            column_mappings: List of column mapping configurations
            created_by: User who created the configuration
        """
        logger.info(f"    Importing {len(column_mappings)} column mappings...")

        for col_map in column_mappings:
            transformation = col_map.get('transformation', {})
            validation = col_map.get('validation', [])

            insert_query = """
                INSERT INTO config.column_mappings (
                    mapping_id, source_column, target_column,
                    transformation_type, transformation_sql,
                    is_key_column, is_nullable, default_value,
                    created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                mapping_id,
                col_map['source'],
                col_map['target'],
                transformation.get('type', 'DIRECT'),
                transformation.get('sql'),
                col_map.get('is_key', False),
                col_map.get('nullable', True),
                col_map.get('default'),
                created_by
            )

            self.db.execute_query(insert_query, params)
            column_mapping_id = self.db.fetch_one("SELECT LAST_INSERT_ID()")[0]

            self.stats['column_mappings'] += 1

            # Import lookups if type is LOOKUP
            if transformation.get('type') == 'LOOKUP':
                self.import_lookup_mappings(
                    mapping_id,
                    column_mapping_id,
                    transformation.get('mapping', {}),
                    transformation.get('default'),
                    created_by
                )

        logger.info(f"    ✓ Created {len(column_mappings)} column mappings")

    def import_lookup_mappings(self, mapping_id: int, column_mapping_id: int,
                              lookup_map: Dict[str, str], fallback: str,
                              created_by: str = 'ADMIN'):
        """
        Import lookup mappings.

        Args:
            mapping_id: Table mapping ID
            column_mapping_id: Column mapping ID
            lookup_map: Source value -> Target value mapping
            fallback: Fallback value
            created_by: User who created the configuration
        """
        for source_val, target_val in lookup_map.items():
            insert_query = """
                INSERT INTO config.lookup_mappings (
                    mapping_id, column_mapping_id, lookup_type,
                    source_value, target_value, fallback_value, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                mapping_id,
                column_mapping_id,
                'HARDCODED',
                source_val,
                target_val,
                fallback,
                created_by
            )

            self.db.execute_query(insert_query, params)
            self.stats['lookup_mappings'] += 1

    def import_dq_rules(self, mapping_id: int,
                       dq_rules: List[Dict[str, Any]],
                       created_by: str = 'ADMIN'):
        """
        Import data quality rules.

        Args:
            mapping_id: Table mapping ID
            dq_rules: List of DQ rule configurations
            created_by: User who created the configuration
        """
        if not dq_rules:
            return

        logger.info(f"    Importing {len(dq_rules)} data quality rules...")

        for rule in dq_rules:
            insert_query = """
                INSERT INTO config.data_quality_rules (
                    mapping_id, rule_code, rule_name, rule_type, rule_scope,
                    rule_definition, severity, action_on_failure,
                    is_active, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                mapping_id,
                rule['rule_code'],
                rule['rule_name'],
                rule['rule_type'],
                rule.get('scope', 'COLUMN'),
                json.dumps(rule.get('definition', {})),
                rule.get('severity', 'ERROR'),
                rule.get('action', 'REJECT'),
                True,
                created_by
            )

            self.db.execute_query(insert_query, params)
            self.stats['dq_rules'] += 1

        logger.info(f"    ✓ Created {len(dq_rules)} DQ rules")

    def import_yaml_config(self, yaml_path: str, created_by: str = 'ADMIN'):
        """
        Import complete YAML configuration.

        Args:
            yaml_path: Path to YAML configuration file
            created_by: User who created the configuration
        """
        # Load YAML
        config = self.load_yaml_file(yaml_path)

        # Import source system
        source_system = config.get('source_system')
        if not source_system:
            raise ValueError("YAML must contain 'source_system' section")

        source_system_id = self.import_source_system(source_system, created_by)

        # Import table mappings
        table_mappings = config.get('table_mappings', [])
        logger.info(f"Importing {len(table_mappings)} table mappings...")

        for mapping_config in table_mappings:
            mapping_id = self.import_table_mapping(
                source_system_id,
                mapping_config,
                created_by
            )

            # Import column mappings
            column_mappings = mapping_config.get('column_mappings', [])
            if column_mappings:
                self.import_column_mappings(mapping_id, column_mappings, created_by)

            # Import data quality rules
            dq_rules = mapping_config.get('data_quality_rules', [])
            if dq_rules:
                self.import_dq_rules(mapping_id, dq_rules, created_by)

        # Commit or rollback
        if self.dry_run:
            logger.warning("DRY RUN mode - rolling back changes")
            self.db.rollback()
        else:
            logger.info("Committing changes to database...")
            self.db.commit()

        # Print statistics
        self.print_statistics()

    def print_statistics(self):
        """Print import statistics."""
        logger.info("\n" + "=" * 80)
        logger.info("Import Statistics")
        logger.info("=" * 80)
        logger.info(f"  Source Systems:    {self.stats['source_systems']}")
        logger.info(f"  Table Mappings:    {self.stats['table_mappings']}")
        logger.info(f"  Column Mappings:   {self.stats['column_mappings']}")
        logger.info(f"  Lookup Mappings:   {self.stats['lookup_mappings']}")
        logger.info(f"  DQ Rules:          {self.stats['dq_rules']}")
        logger.info("=" * 80)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Import YAML mapping configurations to database'
    )
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to YAML configuration file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse and validate but do not commit changes'
    )
    parser.add_argument(
        '--created-by',
        type=str,
        default='ADMIN',
        help='User creating the configuration (default: ADMIN)'
    )

    args = parser.parse_args()

    # Resolve config file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_file = os.path.join(project_root, args.config)

    if not os.path.exists(config_file):
        logger.error(f"Configuration file not found: {config_file}")
        sys.exit(1)

    logger.info("=" * 80)
    logger.info("TA-RDM Source Ingestion - Import Mapping Configurations")
    logger.info("=" * 80)
    logger.info(f"Config file: {config_file}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Created by: {args.created_by}")
    logger.info("=" * 80)
    logger.info("")

    # Create importer
    importer = MappingImporter(CONFIG_DB_CONFIG, dry_run=args.dry_run)

    try:
        # Connect to database
        importer.connect()

        # Import configuration
        importer.import_yaml_config(config_file, args.created_by)

        logger.info("\n✓ Import completed successfully!")

        if not args.dry_run:
            logger.info("\nNext steps:")
            logger.info("  1. Verify imported mappings:")
            logger.info("     SELECT * FROM config.v_active_mappings;")
            logger.info("  2. Test extraction:")
            logger.info("     python scripts/run_ingestion.py --mapping RAMIS_TAXPAYER_TO_PARTY --dry-run")

    except Exception as e:
        logger.error(f"\n✗ Import failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)

    finally:
        importer.disconnect()


if __name__ == '__main__':
    main()
