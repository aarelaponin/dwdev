I'll analyze your documentation to understand the current state and propose an approach for configurable source-to-L2 ingestion.Now let me analyze the uploaded RAMIStables.xlsx file to better understand what RAMIS tables contain:Based on my analysis of your project documentation, I understand your architecture and the missing piece. Let me provide a comprehensive approach for configurable source-to-L2 ingestion.

## Current State Analysis

**You Have:**
- ✅ **Layer 2 (L2)**: TA-RDM canonical model in MySQL (109 tables across 12 schemas)
- ✅ **Layer 3 (L3)**: Dimensional analytics model in ClickHouse
- ✅ **L2→L3 ETL**: Well-documented Python-based ETL pipeline
- ✅ **RAMIS Description**: ~100 source tables with millions of rows

**You Need:**
- ❌ **Source→L2 Ingestion**: Configurable mapping from RAMIS (and other sources) to TA-RDM

## Recommended Architecture: Modern ELT with Configuration Layer

I propose a **3-layer ingestion architecture** that aligns with the medallion pattern referenced in your Data Platform Reference Architecture document:

```
┌─────────────────────────────────────────────────────────────────┐
│                 SOURCE-TO-L2 INGESTION ARCHITECTURE             │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐
│   RAMIS      │
│ (SQL Server) │     ┌──────────────────────────────────┐
│  ~100 tables │────>│  LAYER 0: RAW LANDING ZONE       │
└──────────────┘     │  - Airbyte / Custom Connectors   │
                     │  - 1:1 copy of source tables     │
┌──────────────┐     │  - No transformations            │
│  Other Tax   │     │  - Full audit trail              │
│  Sources     │────>│                                  │
└──────────────┘     │  Location: MySQL/PostgreSQL      │
                     │  Schema: raw_ramis.*             │
                     └────────────┬─────────────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────────────┐
                     │  LAYER 1: STAGING & MAPPING      │
                     │  - Configuration-driven          │
                     │  - Source-to-TA-RDM mappings     │
                     │  - Data quality checks           │
                     │  - Type conversions              │
                     │                                  │
                     │  Location: MySQL/PostgreSQL      │
                     │  Schema: stg_ramis.*             │
                     └────────────┬─────────────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────────────┐
                     │  LAYER 2: TA-RDM CANONICAL       │
                     │  - Your existing TA-RDM model    │
                     │  - 109 normalized tables         │
                     │  - 12 domain schemas             │
                     │                                  │
                     │  Location: MySQL/PostgreSQL      │
                     │  Schemas: party.*, tax_framework │
                     │           filing_assessment.*    │
                     └──────────────────────────────────┘
```

## Component 1: Source Metadata Registry

Create a **configuration database** to store source system metadata and mappings:

```yaml
# source_systems.yaml
source_systems:
  - source_id: "RAMIS_SL"
    source_name: "Sri Lanka RAMIS"
    source_type: "SQL_SERVER"
    connection:
      host: "${RAMIS_HOST}"
      port: 1433
      database: "RAMIS"
      schema: "dbo"
    
    extraction:
      method: "AIRBYTE"  # or "CUSTOM_JDBC"
      schedule: "0 2 * * *"  # Daily at 2 AM
      incremental_column: "ModifiedDate"
      
  - source_id: "LEGACY_VAT"
    source_name: "Legacy VAT System"
    ...
```

## Component 2: Table Mapping Configuration

Define **declarative mappings** from source tables to TA-RDM tables:

```yaml
# mappings/ramis_to_ta_rdm.yaml

# Example: RAMIS taxpayer table -> TA-RDM party table
table_mappings:
  
  - mapping_id: "RAMIS_TAXPAYER_TO_PARTY"
    source:
      system: "RAMIS_SL"
      table: "T_BT_TAXPAYER"
      key_columns: ["TIN"]
      
    target:
      schema: "party"
      table: "party"
      key_columns: ["tax_identification_number"]
      
    column_mappings:
      - source_column: "TIN"
        target_column: "tax_identification_number"
        transformation: "UPPER(TRIM({source_column}))"
        
      - source_column: "TAXPAYER_NAME"
        target_column: "party_name"
        transformation: "{source_column}"
        
      - source_column: "TAXPAYER_TYPE"
        target_column: "party_type_code"
        transformation: |
          CASE 
            WHEN {source_column} = 'I' THEN 'INDIVIDUAL'
            WHEN {source_column} = 'C' THEN 'ENTERPRISE'
            ELSE 'UNKNOWN'
          END
          
      - source_column: "STATUS"
        target_column: "party_status_code"
        lookup_table: "ref_party_status"
        lookup_key: "status_code"
        
      - source_column: "REGISTRATION_DATE"
        target_column: "registration_date"
        transformation: "CAST({source_column} AS DATE)"
        
    filters:
      - "STATUS != 'DELETED'"
      
    data_quality_rules:
      - rule: "NOT_NULL"
        columns: ["TIN", "TAXPAYER_NAME"]
      - rule: "UNIQUE"
        columns: ["TIN"]
      - rule: "VALID_DATE_RANGE"
        column: "REGISTRATION_DATE"
        min_date: "1980-01-01"
        max_date: "CURRENT_DATE"

  - mapping_id: "RAMIS_VAT_RETURN_TO_TAX_RETURN"
    source:
      system: "RAMIS_SL"
      table: "T_BT_ASMT_SVAT_05_HEADER"
      key_columns: ["RETURN_ID"]
      
    target:
      schema: "filing_assessment"
      table: "tax_return"
      key_columns: ["return_id"]
      
    column_mappings:
      - source_column: "RETURN_ID"
        target_column: "return_id"
        transformation: "'RAMIS_' || {source_column}"
        
      - source_column: "TIN"
        target_column: "tax_account_id"
        lookup_query: |
          SELECT tax_account_id 
          FROM registration.tax_account 
          WHERE party_id = (
            SELECT party_id FROM party.party 
            WHERE tax_identification_number = {source_column}
          )
          
      - source_column: "TAX_PERIOD"
        target_column: "tax_period_id"
        transformation: "TO_CHAR({source_column}, 'YYYYMM')"
        
    join_tables:
      - table: "T_BT_ASMT_SVAT_05_DETAIL"
        join_type: "LEFT"
        join_condition: "T_BT_ASMT_SVAT_05_HEADER.RETURN_ID = T_BT_ASMT_SVAT_05_DETAIL.RETURN_ID"
```

## Component 3: Configuration Management Database

Create tables to store and manage configurations:

```sql
-- Configuration Schema
CREATE SCHEMA config;

-- Source System Registry
CREATE TABLE config.source_systems (
    source_system_id INT PRIMARY KEY AUTO_INCREMENT,
    source_code VARCHAR(50) UNIQUE NOT NULL,
    source_name VARCHAR(200),
    source_type VARCHAR(50),
    connection_string TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP
);

-- Table Mapping Registry
CREATE TABLE config.table_mappings (
    mapping_id INT PRIMARY KEY AUTO_INCREMENT,
    mapping_code VARCHAR(100) UNIQUE NOT NULL,
    source_system_id INT,
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    target_schema VARCHAR(100),
    target_table VARCHAR(100),
    mapping_type VARCHAR(50), -- 'DIRECT', 'LOOKUP', 'AGGREGATE'
    is_active BOOLEAN DEFAULT TRUE,
    load_priority INT DEFAULT 100,
    FOREIGN KEY (source_system_id) REFERENCES config.source_systems(source_system_id)
);

-- Column Mapping Registry
CREATE TABLE config.column_mappings (
    column_mapping_id INT PRIMARY KEY AUTO_INCREMENT,
    mapping_id INT,
    source_column VARCHAR(100),
    target_column VARCHAR(100),
    transformation_sql TEXT,
    is_key_column BOOLEAN DEFAULT FALSE,
    data_type VARCHAR(50),
    is_nullable BOOLEAN DEFAULT TRUE,
    default_value VARCHAR(200),
    FOREIGN KEY (mapping_id) REFERENCES config.table_mappings(mapping_id)
);

-- Lookup Tables for Reference Data
CREATE TABLE config.lookup_mappings (
    lookup_mapping_id INT PRIMARY KEY AUTO_INCREMENT,
    mapping_id INT,
    source_column VARCHAR(100),
    source_value VARCHAR(100),
    target_value VARCHAR(100),
    lookup_table VARCHAR(100),
    lookup_key_column VARCHAR(100),
    FOREIGN KEY (mapping_id) REFERENCES config.table_mappings(mapping_id)
);

-- Data Quality Rules
CREATE TABLE config.data_quality_rules (
    rule_id INT PRIMARY KEY AUTO_INCREMENT,
    mapping_id INT,
    rule_type VARCHAR(50), -- 'NOT_NULL', 'UNIQUE', 'RANGE', 'PATTERN'
    column_name VARCHAR(100),
    rule_definition JSON,
    severity VARCHAR(20), -- 'ERROR', 'WARNING'
    FOREIGN KEY (mapping_id) REFERENCES config.table_mappings(mapping_id)
);

-- ETL Execution Log
CREATE TABLE config.etl_execution_log (
    execution_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    source_system_id INT,
    mapping_id INT,
    execution_start TIMESTAMP,
    execution_end TIMESTAMP,
    status VARCHAR(50), -- 'RUNNING', 'SUCCESS', 'FAILED'
    rows_extracted INT,
    rows_loaded INT,
    rows_rejected INT,
    error_message TEXT,
    FOREIGN KEY (source_system_id) REFERENCES config.source_systems(source_system_id),
    FOREIGN KEY (mapping_id) REFERENCES config.table_mappings(mapping_id)
);
```

## Component 4: Metadata-Driven ETL Engine

Create a Python-based ETL engine that reads configurations and executes mappings:

```python
# etl_engine/source_to_l2_etl.py

from typing import Dict, List
import yaml
import logging
from jinja2 import Template

class ConfigurableETL:
    """
    Metadata-driven ETL engine for Source -> L2 ingestion.
    """
    
    def __init__(self, config_db_conn, source_conn, target_conn):
        self.config_db = config_db_conn
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.logger = logging.getLogger(__name__)
    
    def load_mapping_config(self, mapping_id: int) -> Dict:
        """Load mapping configuration from config database."""
        query = """
            SELECT 
                tm.mapping_code,
                tm.source_schema,
                tm.source_table,
                tm.target_schema,
                tm.target_table,
                ss.connection_string,
                ss.source_type
            FROM config.table_mappings tm
            JOIN config.source_systems ss ON tm.source_system_id = ss.source_system_id
            WHERE tm.mapping_id = %s AND tm.is_active = TRUE
        """
        return self.config_db.query_one(query, (mapping_id,))
    
    def load_column_mappings(self, mapping_id: int) -> List[Dict]:
        """Load column mapping rules."""
        query = """
            SELECT 
                source_column,
                target_column,
                transformation_sql,
                is_key_column,
                data_type
            FROM config.column_mappings
            WHERE mapping_id = %s
            ORDER BY column_mapping_id
        """
        return self.config_db.query_all(query, (mapping_id,))
    
    def generate_extraction_sql(self, mapping_config: Dict, 
                                 column_mappings: List[Dict]) -> str:
        """Generate SQL for extraction with transformations."""
        
        source_table = f"{mapping_config['source_schema']}.{mapping_config['source_table']}"
        
        select_clauses = []
        for col_map in column_mappings:
            if col_map['transformation_sql']:
                # Apply transformation
                transformed = col_map['transformation_sql'].replace(
                    '{source_column}', col_map['source_column']
                )
                select_clauses.append(f"{transformed} AS {col_map['target_column']}")
            else:
                # Direct mapping
                select_clauses.append(
                    f"{col_map['source_column']} AS {col_map['target_column']}"
                )
        
        sql = f"""
            SELECT 
                {',\n            '.join(select_clauses)}
            FROM {source_table}
            WHERE 1=1
        """
        
        return sql
    
    def extract(self, mapping_id: int) -> List[Dict]:
        """Extract data from source using configuration."""
        self.logger.info(f"Starting extraction for mapping_id={mapping_id}")
        
        mapping_config = self.load_mapping_config(mapping_id)
        column_mappings = self.load_column_mappings(mapping_id)
        
        extraction_sql = self.generate_extraction_sql(mapping_config, column_mappings)
        
        self.logger.debug(f"Extraction SQL:\n{extraction_sql}")
        
        # Execute on source system
        extracted_data = self.source_conn.query_all(extraction_sql)
        
        self.logger.info(f"Extracted {len(extracted_data)} rows")
        
        return extracted_data
    
    def validate(self, data: List[Dict], mapping_id: int) -> tuple:
        """Apply data quality rules."""
        rules = self.load_dq_rules(mapping_id)
        
        valid_rows = []
        rejected_rows = []
        
        for row in data:
            violations = []
            for rule in rules:
                if not self.check_rule(row, rule):
                    violations.append(rule)
            
            if violations:
                rejected_rows.append((row, violations))
            else:
                valid_rows.append(row)
        
        return valid_rows, rejected_rows
    
    def load_to_staging(self, data: List[Dict], mapping_config: Dict):
        """Load validated data to staging layer."""
        target_table = f"stg_{mapping_config['source_table'].lower()}"
        
        self.target_conn.bulk_insert(
            table=f"staging.{target_table}",
            data=data,
            batch_size=1000
        )
    
    def transform_to_canonical(self, mapping_id: int):
        """Transform staging data to TA-RDM canonical tables."""
        # This uses dbt or SQL-based transformations
        # to move data from staging.* to party.*, tax_framework.*, etc.
        pass
    
    def execute_mapping(self, mapping_id: int):
        """Execute complete ETL pipeline for one mapping."""
        try:
            self.log_execution_start(mapping_id)
            
            # Step 1: Extract
            data = self.extract(mapping_id)
            
            # Step 2: Validate
            valid_data, rejected_data = self.validate(data, mapping_id)
            
            if rejected_data:
                self.log_rejected_rows(mapping_id, rejected_data)
            
            # Step 3: Load to Staging
            mapping_config = self.load_mapping_config(mapping_id)
            self.load_to_staging(valid_data, mapping_config)
            
            # Step 4: Transform to Canonical
            self.transform_to_canonical(mapping_id)
            
            self.log_execution_success(mapping_id, len(data), len(valid_data))
            
        except Exception as e:
            self.log_execution_failure(mapping_id, str(e))
            raise
```

## Component 5: Implementation Steps for RAMIS

Here's how to configure RAMIS specifically:

### Step 1: Create Configuration Files

```bash
# Create directory structure
mkdir -p config/mappings
mkdir -p config/sources

# Create RAMIS source configuration
cat > config/sources/ramis_sl.yaml << 'EOF'
source_system:
  source_code: "RAMIS_SL"
  source_name: "Sri Lanka RAMIS"
  source_type: "SQL_SERVER"
  connection:
    host: "${RAMIS_HOST}"
    port: 1433
    database: "RAMIS"
    username: "${RAMIS_USER}"
    password: "${RAMIS_PASSWORD}"
  
  extraction:
    method: "AIRBYTE"
    connector: "mssql"
    sync_mode: "incremental"
    cursor_field: "ModifiedDate"
    schedule: "0 2 * * *"
EOF
```

### Step 2: Define Priority Mappings

Start with these critical RAMIS tables:

```yaml
# config/mappings/ramis_priority_tables.yaml

priority_mappings:
  # Priority 1: Foundation data
  - source_table: "T_BT_TAXPAYER"
    target_table: "party.party"
    priority: 1
    
  - source_table: "T_BT_TAXPAYER_INDIVIDUAL"
    target_table: "party.individual"
    priority: 2
    
  # Priority 2: Registration
  - source_table: "T_BT_TAX_ACCOUNT"
    target_table: "registration.tax_account"
    priority: 10
    
  - source_table: "T_BT_VAT_REGISTRATION"
    target_table: "registration.vat_registration"
    priority: 11
    
  # Priority 3: Filing & Assessment
  - source_table: "T_BT_ASMT_SVAT_05_HEADER"
    target_table: "filing_assessment.tax_return"
    priority: 20
    
  - source_table: "T_BT_ASSESSMENT"
    target_table: "filing_assessment.assessment"
    priority: 21
```

### Step 3: Run Configuration Import

```python
# scripts/import_ramis_config.py

def import_ramis_mappings():
    """Import RAMIS mapping configurations into database."""
    
    # Load YAML configurations
    with open('config/mappings/ramis_to_ta_rdm.yaml') as f:
        mappings = yaml.safe_load(f)
    
    # Insert into config tables
    for mapping in mappings['table_mappings']:
        insert_mapping(mapping)
        insert_column_mappings(mapping['column_mappings'])
        insert_dq_rules(mapping.get('data_quality_rules', []))
```

### Step 4: Execute ETL

```python
# Run ETL for specific mapping
from etl_engine import ConfigurableETL

etl = ConfigurableETL(config_db, ramis_db, l2_db)
etl.execute_mapping(mapping_id=1)  # TAXPAYER -> party

# Or run all mappings in priority order
etl.execute_all_mappings(source_system='RAMIS_SL')
```

## Recommended Tools Stack

1. **Airbyte** (or Fivetran): For RAW layer extraction
   - Pre-built connectors for SQL Server, MySQL, PostgreSQL
   - Handles incremental syncs automatically
   - Web UI for configuration

2. **dbt** (data build tool): For STAGING → TA-RDM transformations
   - SQL-based transformations
   - Version control
   - Testing framework
   - Documentation

3. **Great Expectations**: For data quality validation
   - Configurable validation rules
   - Automated testing
   - Data profiling

4. **Apache Airflow** (or Prefect): For orchestration
   - DAG-based workflows
   - Dependency management
   - Scheduling

## Next Steps

1. **Start Small**: Map 3-5 critical RAMIS tables first (party, tax_account, tax_return)
2. **Create Configuration Database**: Implement the config schema
3. **Build Core ETL Engine**: Implement the metadata-driven Python engine
4. **Integrate Airbyte**: Set up Airbyte for RAW layer extraction
5. **Implement dbt Models**: Create dbt transformations for STAGING → TA-RDM
6. **Add Orchestration**: Wrap everything in Airflow DAGs

Would you like me to:
1. Create detailed SQL DDL for the configuration database?
2. Develop complete Python code for the ETL engine?
3. Create specific RAMIS table mappings based on the RAMIStables.xlsx file?
4. Design the dbt project structure for transformations?