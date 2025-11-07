# Source-to-L2 Ingestion Implementation Summary

## Problem Statement

You have:
- ✅ Layer 2 (L2): TA-RDM canonical model in MySQL (109 tables)
- ✅ Layer 3 (L3): Dimensional model in ClickHouse  
- ✅ L2→L3 ETL: Documented Python pipeline
- ✅ RAMIS source info: ~100 tables description

You need:
- ❌ **Source→L2 Ingestion**: Configurable mapping from RAMIS to TA-RDM

## Recommended Solution: Metadata-Driven Ingestion Package

Create a new **ta-rdm-source-ingestion** package alongside your existing packages:

```
dwdev/
├── ta-rdm-etl/              # Existing: L2 → L3 
├── ta-rdm-testdata/         # Existing: Test data generation
└── ta-rdm-source-ingestion/ # NEW: Source → L2
```

## Architecture Overview (3 Layers)

**LAYER 0: RAW Landing (Optional)**
- Purpose: 1:1 copy of source tables for audit
- Database: `raw_ramis.*`
- Tool: Airbyte or JDBC connector

**LAYER 1: Staging & Transformation**  
- Purpose: Apply mappings, transformations, validations
- Database: `staging.stg_party`, `staging.stg_tax_account`, etc.
- Process: Metadata-driven ETL engine

**LAYER 2: TA-RDM Canonical**
- Purpose: Your existing normalized operational database
- Database: `party.*`, `tax_framework.*`, `registration.*`, etc.
- Total: 109 tables across 12 schemas

## Key Components Delivered

### 1. Configuration Database Schema (9 tables)
File: `ta_rdm_source_config_schema.sql`

Stores all metadata for configurable ingestion:
- `source_systems` - Register RAMIS and other sources
- `table_mappings` - Source table → Target table mappings
- `column_mappings` - Column-level transformations
- `lookup_mappings` - Value lookups (e.g., "I" → "INDIVIDUAL")
- `data_quality_rules` - Validation rules
- `etl_execution_log` - Execution audit trail
- `data_quality_log` - DQ violation tracking
- `staging_tables` - Staging layer metadata
- `table_dependencies` - Load order dependencies

### 2. ETL Pipeline Engine  
File: `ta_rdm_source_ingestion_pipeline.py`

Core `IngestionPipeline` class with methods:
- `execute_mapping(mapping_id)` - Execute single table mapping
- `execute_source_system(source_code)` - Execute all mappings for a source
- `_extract()` - Extract from source with transformations
- `_transform()` - Apply additional business rules
- `_validate()` - Run data quality checks
- `_load_to_staging()` - Load to L1 staging
- `_merge_to_canonical()` - Merge to L2 canonical

### 3. YAML Mapping Configuration Example
File: `ramis_to_ta_rdm_mappings.yaml`

Declarative configuration showing:
- Source system registration
- 5 example table mappings:
  - RAMIS_TAXPAYER → party.party
  - RAMIS_INDIVIDUAL → party.individual  
  - RAMIS_TAX_ACCOUNT → registration.tax_account
  - RAMIS_VAT_RETURN → filing_assessment.tax_return
  - RAMIS_PAYMENT → payment_refund.payment

Each mapping includes:
- Column transformations (SQL expressions, lookups)
- Data quality rules (NOT_NULL, UNIQUE, RANGE, PATTERN)
- Dependencies (load order)
- Merge strategy (INSERT, UPDATE, UPSERT)

## Implementation Approach

### Phase 1: Setup (Week 1)

1. **Create package structure**
```bash
mkdir ta-rdm-source-ingestion
cd ta-rdm-source-ingestion
mkdir -p {config,metadata,extractors,transformers,loaders,validators,orchestration,utils,scripts,docs}
```

2. **Setup configuration database**
```bash
mysql -u root -p < ta_rdm_source_config_schema.sql
```

3. **Create staging schema**
```sql
CREATE SCHEMA staging;
CREATE TABLE staging.stg_party LIKE party.party;
CREATE TABLE staging.stg_tax_account LIKE registration.tax_account;
-- etc for all target tables
```

### Phase 2: Configuration (Week 2-3)

4. **Register RAMIS source system**
```python
# Insert into config.source_systems
# connection details, extraction method, schedule
```

5. **Import mapping configurations**
```python
# Parse YAML files
# Insert into config.table_mappings
# Insert into config.column_mappings
# Insert into config.data_quality_rules
```

6. **Define dependencies**
```python
# Establish load order:
# party → tax_account → tax_return → payment
```

### Phase 3: Development (Week 3-4)

7. **Implement source extractors**
- SQL Server extractor for RAMIS
- Handle incremental loads
- Batch processing

8. **Implement transformers**
- Column mapping engine
- Lookup resolution
- Business rule execution

9. **Implement validators**
- Data quality rule engine
- Error logging
- Rejection handling

10. **Implement loaders**
- Staging bulk insert
- Canonical merge/upsert
- Transaction management

### Phase 4: Testing (Week 5)

11. **Unit testing** - Test individual components
12. **Integration testing** - End-to-end pipeline tests
13. **Performance testing** - Optimize for large volumes

### Phase 5: Production (Week 6)

14. **Deploy to production**
15. **Schedule with Airflow/Prefect**
16. **Monitor and optimize**

## Technology Stack Recommendations

**Core ETL:**
- Python 3.9+
- MySQL Connector for L2 canonical database
- PyMSSQL for RAMIS (SQL Server)

**Optional Enhancements:**
- **Airbyte**: For Layer 0 raw extraction (GUI configuration)
- **dbt**: For SQL-based staging → canonical transformations
- **Apache Airflow**: For orchestration and scheduling
- **Great Expectations**: For advanced data quality validation

## Comparison with Your Existing L2→L3 Pipeline

Your existing `ta-rdm-etl` package (L2→L3):
- ✅ 16 ETL scripts (4 dimensions + 11 facts + validation)
- ✅ Hardcoded Python logic
- ✅ Works with known TA-RDM structure
- ✅ Optimized for ClickHouse dimensional model

New `ta-rdm-source-ingestion` package (Source→L2):
- ✅ Metadata-driven (configuration, not code)
- ✅ Reusable across different source systems
- ✅ YAML-based mappings (business-friendly)
- ✅ Comprehensive data quality framework
- ✅ Flexible for evolving source schemas

## Key Advantages of This Approach

1. **Configuration over Code**: Add new tables via YAML, not Python
2. **Country-Agnostic**: Works for any tax administration, not just Sri Lanka
3. **Version Controlled**: YAML configs in Git track mapping changes
4. **Business-Friendly**: Tax experts can review/modify YAML files
5. **Comprehensive Audit**: Full lineage from source to canonical
6. **Scalable**: Metadata approach scales to 1000+ tables
7. **Quality-First**: Built-in validation framework prevents bad data

## Next Steps

**Option A: Build Complete Package (4-6 weeks)**
- Full implementation of all components
- Production-ready with testing
- Documentation and training

**Option B: Start with Core Mappings (1-2 weeks)**
- Focus on 5-10 critical RAMIS tables
- Prove the approach works
- Iterate based on learnings

**Option C: Hybrid Approach (2-3 weeks)**
- Use Airbyte for Layer 0 extraction (quick setup)
- Build custom metadata-driven L1→L2 pipeline
- Leverage existing patterns from ta-rdm-etl

## Files Delivered

1. **ta_rdm_source_config_schema.sql** (450 lines)
   - Complete configuration database schema
   - 9 tables with views and indexes
   - Ready to deploy

2. **ta_rdm_source_ingestion_pipeline.py** (800+ lines)
   - Core ETL engine implementation
   - IngestionPipeline class
   - Example usage patterns

3. **ramis_to_ta_rdm_mappings.yaml** (500+ lines)
   - 5 complete mapping examples
   - Shows all transformation types
   - Data quality rule examples

## Questions to Decide

1. **Layer 0 Strategy**: Use Airbyte for raw landing or skip directly to staging?
2. **Orchestration**: Use Airflow, Prefect, or simple cron jobs?
3. **Priority Tables**: Which 10 RAMIS tables should we map first?
4. **Data Quality**: How strict should validation be? (Reject vs. Warn vs. Fix)
5. **Incremental Loads**: Full refresh or CDC/incremental approach?

Would you like me to:
- A) Develop complete Python modules for the full package?
- B) Create specific RAMIS table mappings for your 10 priority tables?
- C) Design the Airflow DAGs for orchestration?
- D) Build a quick prototype with 2-3 tables end-to-end?
