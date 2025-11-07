-- =====================================================================
-- TA-RDM Source-to-L2 Ingestion Configuration Schema
-- =====================================================================
-- Purpose: Metadata-driven configuration for source system ingestion
-- Database: MySQL 8.0+
-- Version: 1.0.0
-- =====================================================================

-- Create configuration schema
CREATE SCHEMA IF NOT EXISTS config
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE config;

-- =====================================================================
-- 1. SOURCE SYSTEM REGISTRY
-- =====================================================================

CREATE TABLE source_systems (
    source_system_id INT AUTO_INCREMENT PRIMARY KEY,
    source_code VARCHAR(50) NOT NULL UNIQUE COMMENT 'Unique identifier (e.g., RAMIS_SL)',
    source_name VARCHAR(200) NOT NULL COMMENT 'Display name',
    source_type VARCHAR(50) NOT NULL COMMENT 'SQL_SERVER, MYSQL, POSTGRESQL, CSV, API',
    
    -- Connection details (encrypted in production)
    connection_host VARCHAR(255) COMMENT 'Database host or API endpoint',
    connection_port INT COMMENT 'Connection port',
    connection_database VARCHAR(100) COMMENT 'Database name',
    connection_schema VARCHAR(100) COMMENT 'Default schema',
    connection_user VARCHAR(100) COMMENT 'Username',
    connection_password_ref VARCHAR(255) COMMENT 'Password reference (vault key)',
    connection_params JSON COMMENT 'Additional connection parameters',
    
    -- Extraction configuration
    extraction_method VARCHAR(50) COMMENT 'AIRBYTE, JDBC, API, FILE',
    extraction_schedule VARCHAR(100) COMMENT 'Cron expression',
    incremental_column VARCHAR(100) COMMENT 'Column for incremental loads',
    batch_size INT DEFAULT 10000 COMMENT 'Batch size for extraction',
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_validated BOOLEAN DEFAULT FALSE COMMENT 'Connection validated',
    last_validation_date TIMESTAMP NULL,
    
    -- Audit
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_source_code (source_code),
    INDEX idx_source_type (source_type),
    INDEX idx_active (is_active)
) ENGINE=InnoDB COMMENT='Registry of source systems';

-- =====================================================================
-- 2. TABLE MAPPING REGISTRY
-- =====================================================================

CREATE TABLE table_mappings (
    mapping_id INT AUTO_INCREMENT PRIMARY KEY,
    mapping_code VARCHAR(100) NOT NULL UNIQUE COMMENT 'Unique mapping identifier',
    mapping_name VARCHAR(200) NOT NULL,
    
    -- Source definition
    source_system_id INT NOT NULL,
    source_schema VARCHAR(100),
    source_table VARCHAR(100) NOT NULL,
    source_key_columns JSON COMMENT 'Array of key column names',
    source_filter_condition TEXT COMMENT 'WHERE clause for filtering',
    
    -- Target definition (L2 canonical)
    target_schema VARCHAR(100) NOT NULL COMMENT 'party, tax_framework, etc.',
    target_table VARCHAR(100) NOT NULL,
    target_key_columns JSON COMMENT 'Array of key column names',
    
    -- Mapping configuration
    mapping_type VARCHAR(50) DEFAULT 'DIRECT' COMMENT 'DIRECT, LOOKUP, AGGREGATE, SPLIT',
    load_strategy VARCHAR(50) DEFAULT 'FULL' COMMENT 'FULL, INCREMENTAL, CDC',
    merge_strategy VARCHAR(50) DEFAULT 'UPSERT' COMMENT 'INSERT, UPDATE, UPSERT, DELETE',
    
    -- Dependencies and execution
    load_priority INT DEFAULT 100 COMMENT 'Execution order (lower first)',
    depends_on_mappings JSON COMMENT 'Array of mapping_ids this depends on',
    
    -- Statistics
    last_extract_date TIMESTAMP NULL,
    last_extract_row_count INT DEFAULT 0,
    last_load_date TIMESTAMP NULL,
    last_load_row_count INT DEFAULT 0,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_validated BOOLEAN DEFAULT FALSE,
    validation_notes TEXT,
    
    -- Audit
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (source_system_id) REFERENCES source_systems(source_system_id),
    INDEX idx_source_table (source_system_id, source_schema, source_table),
    INDEX idx_target_table (target_schema, target_table),
    INDEX idx_priority (load_priority),
    INDEX idx_active (is_active)
) ENGINE=InnoDB COMMENT='Source-to-target table mappings';

-- =====================================================================
-- 3. COLUMN MAPPING REGISTRY
-- =====================================================================

CREATE TABLE column_mappings (
    column_mapping_id INT AUTO_INCREMENT PRIMARY KEY,
    mapping_id INT NOT NULL,
    
    -- Source column
    source_column VARCHAR(100) NOT NULL COMMENT 'Source column name',
    source_data_type VARCHAR(50) COMMENT 'Source data type',
    
    -- Target column
    target_column VARCHAR(100) NOT NULL COMMENT 'Target column name',
    target_data_type VARCHAR(50) COMMENT 'Target data type',
    
    -- Transformation
    transformation_type VARCHAR(50) DEFAULT 'DIRECT' COMMENT 'DIRECT, EXPRESSION, LOOKUP, FUNCTION',
    transformation_sql TEXT COMMENT 'SQL expression or function',
    transformation_params JSON COMMENT 'Additional parameters',
    
    -- Metadata
    is_key_column BOOLEAN DEFAULT FALSE COMMENT 'Part of primary/unique key',
    is_nullable BOOLEAN DEFAULT TRUE,
    default_value VARCHAR(200),
    
    -- Validation rules
    validation_rules JSON COMMENT 'Array of validation rules',
    
    -- Audit
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE,
    INDEX idx_mapping (mapping_id),
    INDEX idx_source_column (source_column),
    INDEX idx_target_column (target_column)
) ENGINE=InnoDB COMMENT='Column-level mapping definitions';

-- =====================================================================
-- 4. LOOKUP MAPPINGS
-- =====================================================================

CREATE TABLE lookup_mappings (
    lookup_mapping_id INT AUTO_INCREMENT PRIMARY KEY,
    mapping_id INT NOT NULL,
    column_mapping_id INT,
    
    -- Lookup definition
    lookup_type VARCHAR(50) COMMENT 'TABLE, HARDCODED, FUNCTION',
    lookup_table VARCHAR(100) COMMENT 'Lookup table name',
    lookup_key_column VARCHAR(100) COMMENT 'Key column in lookup table',
    lookup_value_column VARCHAR(100) COMMENT 'Value column in lookup table',
    
    -- Hardcoded mappings
    source_value VARCHAR(100),
    target_value VARCHAR(100),
    
    -- Fallback behavior
    fallback_value VARCHAR(200),
    fallback_on_null BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE,
    FOREIGN KEY (column_mapping_id) REFERENCES column_mappings(column_mapping_id) ON DELETE CASCADE,
    INDEX idx_mapping (mapping_id),
    INDEX idx_lookup_table (lookup_table)
) ENGINE=InnoDB COMMENT='Lookup transformation definitions';

-- =====================================================================
-- 5. DATA QUALITY RULES
-- =====================================================================

CREATE TABLE data_quality_rules (
    rule_id INT AUTO_INCREMENT PRIMARY KEY,
    mapping_id INT,
    column_mapping_id INT,
    
    -- Rule definition
    rule_code VARCHAR(100) NOT NULL COMMENT 'Unique rule identifier',
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(50) NOT NULL COMMENT 'NOT_NULL, UNIQUE, RANGE, PATTERN, REFERENTIAL, CUSTOM',
    rule_scope VARCHAR(50) DEFAULT 'COLUMN' COMMENT 'COLUMN, TABLE, CROSS_TABLE',
    
    -- Rule configuration
    rule_definition JSON COMMENT 'Rule parameters',
    rule_sql TEXT COMMENT 'SQL expression for CUSTOM rules',
    
    -- Severity and action
    severity VARCHAR(20) DEFAULT 'ERROR' COMMENT 'ERROR, WARNING, INFO',
    action_on_failure VARCHAR(50) DEFAULT 'REJECT' COMMENT 'REJECT, LOG, FIX, CONTINUE',
    error_message_template TEXT,
    
    -- Statistics
    last_execution_date TIMESTAMP NULL,
    total_violations INT DEFAULT 0,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE,
    FOREIGN KEY (column_mapping_id) REFERENCES column_mappings(column_mapping_id) ON DELETE CASCADE,
    INDEX idx_mapping (mapping_id),
    INDEX idx_rule_type (rule_type),
    INDEX idx_active (is_active)
) ENGINE=InnoDB COMMENT='Data quality validation rules';

-- =====================================================================
-- 6. ETL EXECUTION LOG
-- =====================================================================

CREATE TABLE etl_execution_log (
    execution_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Execution context
    source_system_id INT,
    mapping_id INT,
    execution_type VARCHAR(50) COMMENT 'FULL, INCREMENTAL, REPROCESS',
    execution_mode VARCHAR(50) COMMENT 'MANUAL, SCHEDULED, EVENT_DRIVEN',
    
    -- Timing
    execution_start TIMESTAMP NOT NULL,
    execution_end TIMESTAMP NULL,
    duration_seconds INT GENERATED ALWAYS AS (TIMESTAMPDIFF(SECOND, execution_start, execution_end)) STORED,
    
    -- Status
    status VARCHAR(50) NOT NULL COMMENT 'RUNNING, SUCCESS, FAILED, CANCELLED',
    
    -- Metrics
    rows_extracted INT DEFAULT 0,
    rows_validated INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    rows_loaded INT DEFAULT 0,
    
    -- Error handling
    error_message TEXT,
    error_stack_trace TEXT,
    
    -- Metadata
    triggered_by VARCHAR(100),
    execution_params JSON,
    
    FOREIGN KEY (source_system_id) REFERENCES source_systems(source_system_id),
    FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id),
    INDEX idx_execution_date (execution_start),
    INDEX idx_status (status),
    INDEX idx_mapping (mapping_id)
) ENGINE=InnoDB COMMENT='ETL execution audit trail';

-- =====================================================================
-- 7. DATA QUALITY LOG
-- =====================================================================

CREATE TABLE data_quality_log (
    dq_log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_id BIGINT NOT NULL,
    rule_id INT,
    
    -- Violation details
    violation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(100),
    source_row_id VARCHAR(255),
    
    -- Error details
    rule_violated VARCHAR(100),
    error_message TEXT,
    column_name VARCHAR(100),
    column_value TEXT,
    
    -- Action taken
    action_taken VARCHAR(50) COMMENT 'REJECTED, FIXED, LOGGED',
    
    FOREIGN KEY (execution_id) REFERENCES etl_execution_log(execution_id),
    FOREIGN KEY (rule_id) REFERENCES data_quality_rules(rule_id),
    INDEX idx_execution (execution_id),
    INDEX idx_violation_time (violation_timestamp),
    INDEX idx_rule (rule_id)
) ENGINE=InnoDB COMMENT='Data quality violation log';

-- =====================================================================
-- 8. STAGING METADATA
-- =====================================================================

CREATE TABLE staging_tables (
    staging_table_id INT AUTO_INCREMENT PRIMARY KEY,
    mapping_id INT NOT NULL,
    
    -- Staging table definition
    staging_schema VARCHAR(100) DEFAULT 'staging' COMMENT 'Usually staging',
    staging_table VARCHAR(100) NOT NULL,
    
    -- Retention policy
    retention_days INT DEFAULT 7 COMMENT 'Days to keep staging data',
    last_cleanup_date TIMESTAMP NULL,
    
    -- Statistics
    current_row_count INT DEFAULT 0,
    total_data_size_mb DECIMAL(15,2),
    last_refresh_date TIMESTAMP NULL,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE,
    UNIQUE KEY uk_staging_table (staging_schema, staging_table),
    INDEX idx_mapping (mapping_id)
) ENGINE=InnoDB COMMENT='Staging layer metadata';

-- =====================================================================
-- 9. DEPENDENCY GRAPH
-- =====================================================================

CREATE TABLE table_dependencies (
    dependency_id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Dependency relationship
    parent_mapping_id INT NOT NULL COMMENT 'Must be loaded first',
    child_mapping_id INT NOT NULL COMMENT 'Depends on parent',
    
    -- Dependency type
    dependency_type VARCHAR(50) DEFAULT 'FK' COMMENT 'FK, LOOKUP, BUSINESS_RULE',
    dependency_notes TEXT,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    FOREIGN KEY (parent_mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE,
    FOREIGN KEY (child_mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE,
    INDEX idx_parent (parent_mapping_id),
    INDEX idx_child (child_mapping_id),
    
    CONSTRAINT chk_no_self_dependency CHECK (parent_mapping_id != child_mapping_id)
) ENGINE=InnoDB COMMENT='Table load dependencies';

-- =====================================================================
-- SAMPLE DATA: Reference lookup mappings
-- =====================================================================

-- Status code mappings
INSERT INTO lookup_mappings (
    mapping_id, lookup_type, source_value, target_value, 
    fallback_value, created_by
) VALUES
    -- Party status mappings (example)
    (1, 'HARDCODED', 'A', 'ACTIVE', 'UNKNOWN', 'SYSTEM'),
    (1, 'HARDCODED', 'I', 'INACTIVE', 'UNKNOWN', 'SYSTEM'),
    (1, 'HARDCODED', 'D', 'DEREGISTERED', 'UNKNOWN', 'SYSTEM'),
    (1, 'HARDCODED', 'S', 'SUSPENDED', 'UNKNOWN', 'SYSTEM');

-- =====================================================================
-- UTILITY VIEWS
-- =====================================================================

-- View: Active mappings with dependencies
CREATE OR REPLACE VIEW v_active_mappings AS
SELECT 
    tm.mapping_id,
    tm.mapping_code,
    tm.mapping_name,
    ss.source_code,
    ss.source_name,
    CONCAT(tm.source_schema, '.', tm.source_table) AS source_full_name,
    CONCAT(tm.target_schema, '.', tm.target_table) AS target_full_name,
    tm.load_priority,
    tm.load_strategy,
    tm.last_load_date,
    tm.last_load_row_count,
    COUNT(DISTINCT cm.column_mapping_id) AS column_count,
    COUNT(DISTINCT dqr.rule_id) AS dq_rule_count
FROM table_mappings tm
JOIN source_systems ss ON tm.source_system_id = ss.source_system_id
LEFT JOIN column_mappings cm ON tm.mapping_id = cm.mapping_id
LEFT JOIN data_quality_rules dqr ON tm.mapping_id = dqr.mapping_id AND dqr.is_active = TRUE
WHERE tm.is_active = TRUE
GROUP BY tm.mapping_id, tm.mapping_code, tm.mapping_name, ss.source_code, 
         ss.source_name, tm.source_schema, tm.source_table, tm.target_schema, 
         tm.target_table, tm.load_priority, tm.load_strategy, 
         tm.last_load_date, tm.last_load_row_count
ORDER BY tm.load_priority;

-- View: Recent execution summary
CREATE OR REPLACE VIEW v_execution_summary AS
SELECT 
    DATE(execution_start) AS execution_date,
    ss.source_code,
    tm.target_schema,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(rows_loaded) AS total_rows_loaded,
    AVG(duration_seconds) AS avg_duration_seconds
FROM etl_execution_log el
LEFT JOIN source_systems ss ON el.source_system_id = ss.source_system_id
LEFT JOIN table_mappings tm ON el.mapping_id = tm.mapping_id
WHERE execution_start >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
GROUP BY DATE(execution_start), ss.source_code, tm.target_schema
ORDER BY execution_date DESC, ss.source_code;

-- View: Data quality summary
CREATE OR REPLACE VIEW v_dq_summary AS
SELECT 
    dqr.rule_code,
    dqr.rule_name,
    dqr.rule_type,
    dqr.severity,
    tm.target_schema,
    tm.target_table,
    COUNT(DISTINCT dql.dq_log_id) AS violation_count,
    MAX(dql.violation_timestamp) AS last_violation
FROM data_quality_rules dqr
LEFT JOIN table_mappings tm ON dqr.mapping_id = tm.mapping_id
LEFT JOIN data_quality_log dql ON dqr.rule_id = dql.rule_id
WHERE dqr.is_active = TRUE
    AND dql.violation_timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY dqr.rule_code, dqr.rule_name, dqr.rule_type, 
         dqr.severity, tm.target_schema, tm.target_table
HAVING violation_count > 0
ORDER BY violation_count DESC;

-- =====================================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================================

-- Additional performance indexes
ALTER TABLE etl_execution_log 
    ADD INDEX idx_composite_lookup (source_system_id, status, execution_start);

ALTER TABLE data_quality_log 
    ADD INDEX idx_recent_violations (violation_timestamp, rule_id);

-- =====================================================================
-- GRANTS (Example)
-- =====================================================================

-- Create roles
-- CREATE ROLE 'config_admin', 'config_read', 'config_write';

-- Grant permissions
-- GRANT ALL ON config.* TO 'config_admin';
-- GRANT SELECT ON config.* TO 'config_read';
-- GRANT SELECT, INSERT, UPDATE ON config.* TO 'config_write';

-- =====================================================================
-- END OF SCHEMA
-- =====================================================================
