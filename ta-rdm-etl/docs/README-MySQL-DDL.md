# TA-RDM MySQL DDL Script

## Overview

This MySQL DDL script contains the complete Tax Administration Reference Data Model (TA-RDM) database schema, generated from the YAML domain specifications in your project knowledge.

## Script Details

- **File**: `ta-rdm-mysql-ddl.sql`
- **Size**: 275 KB
- **Target Database**: MySQL 8.0+
- **Character Set**: UTF-8 (utf8mb4)
- **Collation**: utf8mb4_unicode_ci
- **Storage Engine**: InnoDB

## Statistics

- **Total Tables**: 109
- **Total Indexes**: 201
- **Total Foreign Keys**: 69
- **Database Schemas**: 12

## Database Schemas

The DDL creates the following 12 schemas:

1. **party** - Party management (taxpayers, individuals, organizations, addresses)
2. **tax_framework** - Tax type definitions, periods, rates, forms
3. **registration** - Taxpayer registration, VAT registration, licenses
4. **filing_assessment** - Tax returns, assessments, penalties, interest
5. **accounting** - Tax accounts, transactions, balances, ledgers
6. **payment_refund** - Payments, refunds, payment allocations
7. **compliance_control** - Audits, collections, appeals, enforcement
8. **document_management** - Documents, workflows, communications
9. **reference** - Reference data and lookup tables
10. **withholding_tax** - Withholding tax (PAYE) specific tables
11. **vat** - VAT-specific extension tables
12. **income_tax** - Income tax-specific extension tables

## Domain Coverage

### 01. Party Management (2 tables)
- party
- individual

### 02. Tax Framework (12 tables)
- tax_type
- tax_period
- tax_period_template
- tax_form
- form_field
- form_section
- tax_rate
- paye_tax_scale
- paye_scale_bracket
- tax_calculation_rule
- calculation_rule_parameter
- tax_base_definition

### 03. Registration (9 tables)
- tax_account
- tax_account_period
- vat_registration
- vat_certificate
- business_license
- taxable_object
- tax_account_object
- taxpayer_segmentation
- registration_compliance_case

### 04. Filing & Assessment (6 tables)
- tax_return
- return_field_value
- assessment
- penalty_assessment
- interest_assessment
- tax_loss

### 05. Accounting (9 tables)
- account
- transaction
- transaction_line
- accounting_period
- balance
- balance_history
- receivable
- journal_entry
- journal_entry_line

### 06. Payment & Refund (11 tables)
- payment
- payment_allocation
- payment_reversal
- refund
- refund_allocation
- refund_voucher
- installment_plan
- installment_payment
- suspended_payment
- bank_interface
- bank_reconciliation

### 07. Compliance & Control (10 tables)
- audit_plan
- audit_case
- audit_finding
- collection_case
- collection_action
- objection
- appeal
- compliance_program
- risk_profile
- risk_assessment_result

### 08. Document Management (13 tables)
- document
- document_version
- document_attachment
- document_relationship
- correspondence
- correspondence_recipient
- communication_template
- notification
- task
- task_assignment
- workflow
- workflow_step
- approval

### 09. Reference Data (27 tables)
All lookup and reference tables (ref_* prefix)

### 10. Withholding Tax (3 tables)
- employment_relationship
- withholding_return
- withholding_detail

### 11. VAT Tax (3 tables)
- vat_return
- vat_goods_services_line
- vat_input_output_line

### 12. Income Tax (4 tables)
- income_tax_return
- income_source
- tax_deduction
- tax_credit

## Key Features

### MySQL-Specific Optimizations

1. **Data Types**:
   - PostgreSQL types mapped to MySQL equivalents
   - `timestamp with time zone` → `TIMESTAMP`
   - `jsonb` → `JSON`
   - `uuid` → `CHAR(36)`
   - `boolean` → `BOOLEAN`

2. **Constraints**:
   - Primary keys with AUTO_INCREMENT
   - Foreign keys with proper references
   - UNIQUE constraints
   - CHECK constraints (MySQL 8.0.16+)

3. **Indexes**:
   - Primary key indexes
   - Foreign key indexes
   - Performance optimization indexes
   - Unique indexes

4. **Character Set**:
   - UTF-8 support (utf8mb4)
   - Unicode collation (utf8mb4_unicode_ci)
   - Full emoji support

5. **Comments**:
   - Table-level comments from descriptions
   - Column-level comments for documentation
   - All metadata preserved

### Audit Trail

All tables include standard audit columns:
- `created_by` - User who created the record
- `created_date` - Timestamp of creation
- `modified_by` - User who last modified the record
- `modified_date` - Timestamp of last modification

### Temporal Data

Tables supporting historical data include:
- `valid_from` - Start date of validity
- `valid_to` - End date of validity
- `is_current` - Flag for current record

## Usage Instructions

### Loading the DDL

```sql
-- Connect to MySQL
mysql -u root -p

-- Create a new database (if needed)
CREATE DATABASE ta_rdm;
USE ta_rdm;

-- Run the DDL script
SOURCE ta-rdm-mysql-ddl.sql;
```

### Verification

```sql
-- Check all schemas were created
SELECT SCHEMA_NAME 
FROM information_schema.SCHEMATA 
WHERE SCHEMA_NAME IN ('party', 'tax_framework', 'registration', 
                      'filing_assessment', 'accounting', 'payment_refund', 
                      'compliance_control', 'document_management', 'reference',
                      'withholding_tax', 'vat', 'income_tax');

-- Count tables per schema
SELECT TABLE_SCHEMA, COUNT(*) as table_count
FROM information_schema.TABLES
WHERE TABLE_SCHEMA IN ('party', 'tax_framework', 'registration', 
                       'filing_assessment', 'accounting', 'payment_refund', 
                       'compliance_control', 'document_management', 'reference',
                       'withholding_tax', 'vat', 'income_tax')
GROUP BY TABLE_SCHEMA
ORDER BY TABLE_SCHEMA;

-- Check foreign keys
SELECT COUNT(*) as fk_count
FROM information_schema.TABLE_CONSTRAINTS
WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'
AND TABLE_SCHEMA IN ('party', 'tax_framework', 'registration', 
                     'filing_assessment', 'accounting', 'payment_refund', 
                     'compliance_control', 'document_management', 'reference',
                     'withholding_tax', 'vat', 'income_tax');
```

## Important Notes

### Execution Order

The DDL script executes in this order:
1. Disable foreign key checks and unique checks
2. Create all schemas
3. Create all tables
4. Create all indexes
5. Add all foreign key constraints
6. Re-enable checks

This order ensures no dependency issues during creation.

### Foreign Key Constraints

Foreign keys are added **after** all tables are created to avoid circular dependency issues. You can disable foreign key checks during initial data loading:

```sql
SET FOREIGN_KEY_CHECKS=0;
-- Load your data
SET FOREIGN_KEY_CHECKS=1;
```

### Performance Considerations

1. **Indexes**: The script creates indexes for performance, but you may need additional indexes based on your query patterns
2. **Partitioning**: Consider partitioning large tables (payments, transactions) by date
3. **Storage**: InnoDB engine provides ACID compliance and good performance
4. **Buffer Pool**: Tune `innodb_buffer_pool_size` for your workload

### Customization

You may want to customize:
- Table partitioning strategies
- Additional indexes for specific queries
- Storage parameters for large tables
- Backup and archiving strategies
- Row-level security policies

## Testing Recommendations

1. **Schema Validation**: Verify all tables, indexes, and foreign keys created successfully
2. **Sample Data**: Load test data to validate constraints
3. **Query Testing**: Test common query patterns
4. **Performance Testing**: Benchmark with realistic data volumes
5. **Integration Testing**: Test with your application

## Source Information

This DDL was automatically generated from the TA-RDM YAML specifications:

- Based on SIGTAS (Standard Integrated Government Tax Administration System)
- Aligned with GovStack Building Blocks
- Following international tax administration best practices
- Incorporating patterns from Moldova SRS and other implementations

## Version Information

- **TA-RDM Version**: 1.0.0
- **Generated**: 2025-11-05
- **Source**: Project Knowledge YAML files
- **Domains**: 12 functional domains
- **Total Entities**: 109 tables

## Support and Documentation

For more information about the TA-RDM model:
- Refer to the project knowledge YAML files
- Review the TA-RDM Analysis Report
- Check the YAML Guide for understanding the data model
- Consult SIGTAS mapping matrix for source traceability

## License

The TA-RDM is provided under CC-BY-4.0 license as part of the GovStack Initiative.

---

**Generated by**: Claude (Anthropic)  
**Generation Date**: November 5, 2025  
**Script Version**: 1.0.0
