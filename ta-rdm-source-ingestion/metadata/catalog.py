"""
Metadata Catalog Manager for TA-RDM Source Ingestion.

Provides Python API to interact with the configuration database:
- Source systems
- Table mappings
- Column mappings
- Lookup mappings
- Data quality rules
- Execution logs
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from utils.db_utils import DatabaseConnection, DatabaseType, get_db_connection
from config.database_config import CONFIG_DB_CONFIG

logger = logging.getLogger(__name__)


class MetadataCatalog:
    """
    Metadata catalog manager for configuration database.

    Provides high-level API for:
    - Querying mapping configurations
    - Managing execution logs
    - Recording data quality violations
    """

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """
        Initialize metadata catalog.

        Args:
            db_config: Database configuration (defaults to CONFIG_DB_CONFIG)
        """
        self.db_config = db_config or CONFIG_DB_CONFIG
        self.db: Optional[DatabaseConnection] = None

    def connect(self) -> DatabaseConnection:
        """
        Establish connection to configuration database.

        Returns:
            DatabaseConnection: Active database connection
        """
        if not self.db:
            self.db = DatabaseConnection(DatabaseType.MYSQL, self.db_config)
            self.db.connect()
            logger.info("Connected to configuration database")
        return self.db

    def disconnect(self):
        """Close database connection."""
        if self.db:
            self.db.disconnect()
            self.db = None

    # =========================================================================
    # SOURCE SYSTEMS
    # =========================================================================

    def get_source_system(self, source_code: str) -> Optional[Dict[str, Any]]:
        """
        Get source system by code.

        Args:
            source_code: Source system code (e.g., 'RAMIS')

        Returns:
            Dict with source system details or None
        """
        query = """
            SELECT *
            FROM config.source_systems
            WHERE source_code = %s AND is_active = TRUE
        """
        self.connect()
        row = self.db.fetch_one(query, (source_code,), dictionary=True)
        return row

    def get_all_source_systems(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all source systems.

        Args:
            active_only: Return only active source systems

        Returns:
            List of source system dictionaries
        """
        query = "SELECT * FROM config.source_systems"
        if active_only:
            query += " WHERE is_active = TRUE"
        query += " ORDER BY source_code"

        self.connect()
        rows = self.db.fetch_all(query, dictionary=True)
        return rows

    # =========================================================================
    # TABLE MAPPINGS
    # =========================================================================

    def get_table_mapping(self, mapping_id: int) -> Optional[Dict[str, Any]]:
        """
        Get table mapping by ID.

        Args:
            mapping_id: Mapping ID

        Returns:
            Dict with mapping details or None
        """
        query = """
            SELECT *
            FROM config.table_mappings
            WHERE mapping_id = %s
        """
        self.connect()
        row = self.db.fetch_one(query, (mapping_id,), dictionary=True)
        return row

    def get_table_mapping_by_code(self, mapping_code: str) -> Optional[Dict[str, Any]]:
        """
        Get table mapping by code.

        Args:
            mapping_code: Mapping code

        Returns:
            Dict with mapping details or None
        """
        query = """
            SELECT *
            FROM config.table_mappings
            WHERE mapping_code = %s AND is_active = TRUE
        """
        self.connect()
        row = self.db.fetch_one(query, (mapping_code,), dictionary=True)
        return row

    def get_mappings_by_source(self, source_code: str,
                               active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all table mappings for a source system.

        Args:
            source_code: Source system code
            active_only: Return only active mappings

        Returns:
            List of table mapping dictionaries
        """
        query = """
            SELECT tm.*
            FROM config.table_mappings tm
            JOIN config.source_systems ss ON tm.source_system_id = ss.source_system_id
            WHERE ss.source_code = %s
        """
        if active_only:
            query += " AND tm.is_active = TRUE"
        query += " ORDER BY tm.load_priority, tm.mapping_id"

        self.connect()
        rows = self.db.fetch_all(query, (source_code,), dictionary=True)
        return rows

    def get_mappings_execution_order(self, source_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get table mappings in execution order (resolving dependencies).

        Args:
            source_code: Optional source system code to filter

        Returns:
            List of mappings sorted by dependency order
        """
        # This is a simplified version - Phase 4 will implement proper dependency resolution
        query = """
            SELECT tm.*
            FROM config.table_mappings tm
            JOIN config.source_systems ss ON tm.source_system_id = ss.source_system_id
            WHERE tm.is_active = TRUE
        """
        params = []
        if source_code:
            query += " AND ss.source_code = %s"
            params.append(source_code)

        query += " ORDER BY tm.load_priority, tm.mapping_id"

        self.connect()
        rows = self.db.fetch_all(query, tuple(params), dictionary=True)
        return rows

    # =========================================================================
    # COLUMN MAPPINGS
    # =========================================================================

    def get_column_mappings(self, mapping_id: int) -> List[Dict[str, Any]]:
        """
        Get all column mappings for a table mapping.

        Args:
            mapping_id: Table mapping ID

        Returns:
            List of column mapping dictionaries
        """
        query = """
            SELECT *
            FROM config.column_mappings
            WHERE mapping_id = %s
            ORDER BY column_mapping_id
        """
        self.connect()
        rows = self.db.fetch_all(query, (mapping_id,), dictionary=True)
        return rows

    # =========================================================================
    # LOOKUP MAPPINGS
    # =========================================================================

    def get_lookup_mappings(self, mapping_id: int) -> List[Dict[str, Any]]:
        """
        Get all lookup mappings for a table mapping.

        Args:
            mapping_id: Table mapping ID

        Returns:
            List of lookup mapping dictionaries
        """
        query = """
            SELECT *
            FROM config.lookup_mappings
            WHERE mapping_id = %s
            ORDER BY lookup_mapping_id
        """
        self.connect()
        rows = self.db.fetch_all(query, (mapping_id,), dictionary=True)
        return rows

    def get_lookup_value(self, mapping_id: int, source_value: str,
                        fallback: Optional[str] = None) -> Optional[str]:
        """
        Get target value for a lookup mapping.

        Args:
            mapping_id: Table mapping ID
            source_value: Source value to lookup
            fallback: Fallback value if not found

        Returns:
            Target value or fallback
        """
        query = """
            SELECT target_value, fallback_value
            FROM config.lookup_mappings
            WHERE mapping_id = %s AND source_value = %s
            LIMIT 1
        """
        self.connect()
        row = self.db.fetch_one(query, (mapping_id, source_value), dictionary=True)

        if row:
            return row['target_value']
        elif row and row['fallback_value']:
            return row['fallback_value']
        else:
            return fallback

    # =========================================================================
    # DATA QUALITY RULES
    # =========================================================================

    def get_dq_rules(self, mapping_id: int, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get data quality rules for a table mapping.

        Args:
            mapping_id: Table mapping ID
            active_only: Return only active rules

        Returns:
            List of data quality rule dictionaries
        """
        query = """
            SELECT *
            FROM config.data_quality_rules
            WHERE mapping_id = %s
        """
        if active_only:
            query += " AND is_active = TRUE"
        query += " ORDER BY severity DESC, rule_id"

        self.connect()
        rows = self.db.fetch_all(query, (mapping_id,), dictionary=True)
        return rows

    # =========================================================================
    # ETL EXECUTION LOG
    # =========================================================================

    def start_execution(self, mapping_id: int, execution_type: str = 'FULL',
                       execution_mode: str = 'MANUAL',
                       triggered_by: str = 'SYSTEM',
                       execution_params: Optional[Dict] = None) -> int:
        """
        Record start of ETL execution.

        Args:
            mapping_id: Table mapping ID
            execution_type: FULL, INCREMENTAL, REPROCESS
            execution_mode: MANUAL, SCHEDULED, EVENT_DRIVEN
            triggered_by: User or system identifier
            execution_params: Additional parameters (JSON)

        Returns:
            execution_id: Generated execution ID
        """
        # Get source_system_id from mapping
        mapping = self.get_table_mapping(mapping_id)
        if not mapping:
            raise ValueError(f"Mapping ID {mapping_id} not found")

        query = """
            INSERT INTO config.etl_execution_log (
                source_system_id, mapping_id, execution_type, execution_mode,
                execution_start, status, triggered_by, execution_params
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        import json
        params = (
            mapping['source_system_id'],
            mapping_id,
            execution_type,
            execution_mode,
            datetime.now(),
            'RUNNING',
            triggered_by,
            json.dumps(execution_params) if execution_params else None
        )

        self.connect()
        self.db.execute_query(query, params)
        execution_id = self.db.fetch_one("SELECT LAST_INSERT_ID()")[0]
        self.db.commit()

        logger.info(f"Started execution {execution_id} for mapping {mapping_id}")
        return execution_id

    def end_execution(self, execution_id: int, status: str,
                     rows_extracted: int = 0, rows_validated: int = 0,
                     rows_rejected: int = 0, rows_loaded: int = 0,
                     error_message: Optional[str] = None,
                     error_stack_trace: Optional[str] = None):
        """
        Record end of ETL execution.

        Args:
            execution_id: Execution ID from start_execution()
            status: SUCCESS, FAILED, CANCELLED
            rows_extracted: Number of rows extracted
            rows_validated: Number of rows validated
            rows_rejected: Number of rows rejected
            rows_loaded: Number of rows loaded
            error_message: Error message if failed
            error_stack_trace: Full stack trace if failed
        """
        query = """
            UPDATE config.etl_execution_log
            SET execution_end = %s,
                status = %s,
                rows_extracted = %s,
                rows_validated = %s,
                rows_rejected = %s,
                rows_loaded = %s,
                error_message = %s,
                error_stack_trace = %s
            WHERE execution_id = %s
        """
        params = (
            datetime.now(),
            status,
            rows_extracted,
            rows_validated,
            rows_rejected,
            rows_loaded,
            error_message,
            error_stack_trace,
            execution_id
        )

        self.connect()
        self.db.execute_query(query, params)
        self.db.commit()

        logger.info(
            f"Ended execution {execution_id}: {status} - "
            f"{rows_loaded}/{rows_extracted} rows loaded"
        )

    def get_execution_history(self, mapping_id: Optional[int] = None,
                             limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get execution history.

        Args:
            mapping_id: Optional mapping ID to filter
            limit: Maximum number of records

        Returns:
            List of execution log dictionaries
        """
        query = """
            SELECT *
            FROM config.etl_execution_log
        """
        params = []
        if mapping_id:
            query += " WHERE mapping_id = %s"
            params.append(mapping_id)

        query += " ORDER BY execution_start DESC LIMIT %s"
        params.append(limit)

        self.connect()
        rows = self.db.fetch_all(query, tuple(params), dictionary=True)
        return rows

    # =========================================================================
    # DATA QUALITY LOG
    # =========================================================================

    def log_dq_violation(self, execution_id: int, rule_id: Optional[int],
                        rule_violated: str, source_table: str,
                        source_row_id: str, error_message: str,
                        column_name: Optional[str] = None,
                        column_value: Optional[str] = None,
                        action_taken: str = 'LOGGED'):
        """
        Log a data quality violation.

        Args:
            execution_id: Execution ID
            rule_id: Data quality rule ID
            rule_violated: Rule code that was violated
            source_table: Source table name
            source_row_id: Source row identifier
            error_message: Error description
            column_name: Column name (if applicable)
            column_value: Column value (if applicable)
            action_taken: REJECTED, FIXED, LOGGED
        """
        query = """
            INSERT INTO config.data_quality_log (
                execution_id, rule_id, rule_violated, source_table,
                source_row_id, error_message, column_name, column_value,
                action_taken
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            execution_id,
            rule_id,
            rule_violated,
            source_table,
            source_row_id,
            error_message,
            column_name,
            column_value,
            action_taken
        )

        self.connect()
        self.db.execute_query(query, params)

    def get_dq_violations(self, execution_id: int,
                         limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get data quality violations for an execution.

        Args:
            execution_id: Execution ID
            limit: Maximum number of violations

        Returns:
            List of violation dictionaries
        """
        query = """
            SELECT *
            FROM config.data_quality_log
            WHERE execution_id = %s
            ORDER BY violation_timestamp DESC
            LIMIT %s
        """
        self.connect()
        rows = self.db.fetch_all(query, (execution_id, limit), dictionary=True)
        return rows

    # =========================================================================
    # STAGING TABLES
    # =========================================================================

    def get_staging_table(self, mapping_id: int) -> Optional[Dict[str, Any]]:
        """
        Get staging table metadata for a mapping.

        Args:
            mapping_id: Table mapping ID

        Returns:
            Staging table dictionary or None
        """
        query = """
            SELECT *
            FROM config.staging_tables
            WHERE mapping_id = %s AND is_active = TRUE
        """
        self.connect()
        row = self.db.fetch_one(query, (mapping_id,), dictionary=True)
        return row

    # =========================================================================
    # TABLE DEPENDENCIES
    # =========================================================================

    def get_dependencies(self, mapping_id: int) -> List[Dict[str, Any]]:
        """
        Get dependencies for a table mapping.

        Args:
            mapping_id: Table mapping ID

        Returns:
            List of parent mapping IDs that must be loaded first
        """
        query = """
            SELECT parent_mapping_id, dependency_type, dependency_notes
            FROM config.table_dependencies
            WHERE child_mapping_id = %s AND is_active = TRUE
        """
        self.connect()
        rows = self.db.fetch_all(query, (mapping_id,), dictionary=True)
        return rows


# =============================================================================
# Context Manager
# =============================================================================

def get_metadata_catalog(db_config: Optional[Dict[str, Any]] = None):
    """
    Context manager for metadata catalog.

    Args:
        db_config: Optional database configuration

    Yields:
        MetadataCatalog: Catalog instance

    Usage:
        with get_metadata_catalog() as catalog:
            mappings = catalog.get_mappings_by_source('RAMIS')
    """
    catalog = MetadataCatalog(db_config)
    try:
        catalog.connect()
        yield catalog
    finally:
        catalog.disconnect()
