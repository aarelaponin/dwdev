# Tax Administration Reference Data Model (TA-RDM)
## YAML Schema Guide and Usage Instructions

**Version:** 1.0.0  
**Date:** 2025-11-04  
**Author:** Aare Keeman  

---

## Executive Summary

This document introduces a comprehensive YAML-based approach for documenting the Tax Administration Reference Data Model (TA-RDM). The approach provides:

✅ **Machine-readable** format for automated code generation  
✅ **Version control** friendly structure  
✅ **Comprehensive documentation** embedded with the schema  
✅ **Validation capabilities** to ensure consistency  
✅ **Tool ecosystem** support for DDL, ORM, API generation  

## Why YAML for Data Modeling?

### Traditional Approach Limitations
- **Documentation drift**: Separate docs become outdated
- **Manual processes**: DDL created manually, error-prone
- **Inconsistency**: Different conventions across modules
- **Poor visibility**: Changes hard to track and review

### YAML-Based Approach Benefits

| **Benefit** | **Description** | **Impact** |
|------------|----------------|-----------|
| **Single Source of Truth** | Schema definition, documentation, and rules in one place | Eliminates documentation drift |
| **Code Generation** | Auto-generate DDL, ORMs, APIs from YAML | Reduces errors, saves time |
| **Version Control** | Git-friendly format with clear diffs | Better collaboration and audit trail |
| **Validation** | Automated schema validation | Enforces consistency |
| **Tool Integration** | Feed into dbt, CI/CD, documentation generators | Enables modern DevOps practices |
| **Accessibility** | Domain experts can review/contribute | Better stakeholder engagement |

## Schema Architecture

### File Structure

```
ta-rdm/
├── ta-rdm-schema.yaml          # Meta-schema definition (the schema for schemas)
├── domains/
│   ├── party-management.yaml    # Party Management domain
│   ├── tax-framework.yaml       # Tax Framework domain
│   ├── registration.yaml        # Registration domain
│   ├── assessment.yaml          # Assessment & Liability domain
│   ├── accounting.yaml          # Accounting domain
│   ├── payment.yaml             # Payment & Refund domain
│   ├── compliance.yaml          # Compliance & Control domain
│   └── reference-data.yaml      # Reference data definitions
├── scripts/
│   ├── generate-ddl.py         # Generate PostgreSQL DDL
│   ├── generate-docs.py        # Generate HTML documentation
│   ├── validate-schema.py      # Validate YAML against schema
│   └── generate-erd.py         # Generate ERD diagrams
└── docs/
    └── generated/              # Auto-generated documentation
```

### Schema Layers

The schema is organized in three conceptual layers:

#### **Layer 1: Meta-Schema** (`ta-rdm-schema.yaml`)
- Defines the structure and rules for domain YAML files
- Documents all possible properties and their meanings
- Serves as validation schema
- Acts as contract for tooling

#### **Layer 2: Domain Models** (`domains/*.yaml`)
- Actual data model definitions for each domain
- Tables, columns, relationships, constraints
- Business rules and data quality rules
- Integration specifications

#### **Layer 3: Generated Artifacts**
- DDL scripts (PostgreSQL, Oracle, MySQL)
- Documentation (HTML, Markdown, PDF)
- ORM models (SQLAlchemy, Hibernate)
- API schemas (OpenAPI/Swagger)
- dbt models for Data Warehouse

## Schema Structure Deep Dive

### Top-Level Sections

Every domain YAML file contains these sections:

```yaml
metadata:               # Model version, authors, standards
domains:               # Business domain definitions
  - schemas:           # Database schemas
    - tables:          # Table definitions
      - columns:       # Column specifications
      - indexes:       # Index definitions
      - constraints:   # Constraints
      - relationships: # Relationships
reference_data:        # Static lookup tables
business_rules:        # Cross-table rules
data_quality_rules:    # DQ checks
integrations:          # External system connections
security_policies:     # Security requirements
```

### Key Design Principles

#### 1. **Comprehensive Metadata**
Every element includes:
- Technical specifications (types, constraints)
- Business context (descriptions, rules)
- Usage notes and examples
- Security classifications
- Performance hints

#### 2. **Temporal Support**
Built-in support for:
- Effective dating (`valid_from`, `valid_to`)
- Current version tracking (`is_current`)
- Audit trail (created/modified by/date)

#### 3. **Security by Design**
- Data classification levels
- Encryption requirements
- Masking rules for non-production
- PII and GDPR flags

#### 4. **Integration Ready**
- Domain events specification
- API endpoint definitions
- External system mappings
- Data flow documentation

## Working with the Schema

### Creating a New Domain

**Step 1: Start from Template**
```yaml
metadata:
  model_name: "Tax Administration Reference Data Model"
  model_code: "TA-RDM"
  version: "1.0.0"
  # ... standard metadata

domains:
  - name: "your_domain_name"
    code: "YD"
    display_name: "Your Domain Name"
    description: |
      Comprehensive description of the domain
    # ... continue with schemas
```

**Step 2: Define Schemas and Tables**
```yaml
schemas:
  - schema_name: "your_schema"
    description: "Schema purpose"
    tables:
      - name: "your_table"
        description: "Table purpose"
        business_rule: "Rules enforced by this table"
        columns:
          # Column definitions
```

**Step 3: Add Columns with Full Specs**
```yaml
columns:
  - name: "column_name"
    type: "data_type"
    description: "What this column represents"
    constraints:
      not_null: true
      unique: false
      default: "default_value"
    business_rule: "Business rules for this column"
    security:
      classification: "confidential"
      encryption: "AES-256"
      pii: true
```

**Step 4: Define Relationships**
```yaml
relationships:
  - type: "one_to_many"
    target_table: "target_table"
    source_column: "source_column"
    target_column: "target_column"
    cardinality: "1:N"
    description: "Business meaning"
```

**Step 5: Add Business Rules**
```yaml
business_rules:
  - id: "BR-YD-001"
    domain: "your_domain"
    name: "Rule Name"
    description: "Rule description"
    severity: "CRITICAL"
    validation_query: |
      SELECT * FROM your_table
      WHERE validation_fails
```

### Best Practices

#### **Naming Conventions**
```yaml
# Tables: singular, snake_case
name: "party"              # ✓ Correct
name: "parties"            # ✗ Incorrect
name: "Party"              # ✗ Incorrect

# Columns: descriptive, snake_case
name: "party_name"         # ✓ Correct
name: "partyName"          # ✗ Incorrect
name: "name"               # ✗ Too generic

# Indexes: prefixed
name: "idx_party_name"     # ✓ Correct
name: "party_name_idx"     # ✗ Incorrect

# Foreign keys
name: "{referenced_table}_id"  # ✓ Pattern
```

#### **Documentation Quality**
```yaml
# Bad: Too brief
description: "Party name"

# Good: Comprehensive
description: |
  Full name or business name of the party.
  For individuals: "{last_name}, {first_name} {middle_name}"
  For enterprises: Official registered business name
business_name: "Name"
usage_note: "Used in reports and UI where space is limited"
examples:
  - "Smith, John Michael"
  - "ABC Corporation Ltd."
```

#### **Constraint Specifications**
```yaml
# Always specify constraints explicitly
constraints:
  not_null: true           # Never rely on defaults
  unique: true
  check: "amount >= 0"     # Include validation logic
  foreign_key:
    table: "party"
    column: "party_id"
    on_delete: "RESTRICT"  # Be explicit
    on_update: "CASCADE"
```

## Code Generation

### 1. PostgreSQL DDL Generation

```python
# scripts/generate-ddl.py
import yaml

def generate_create_table(table_spec):
    """Generate CREATE TABLE statement"""
    sql = f"CREATE TABLE {table_spec['name']} (\n"
    
    # Generate columns
    for col in table_spec['columns']:
        sql += f"  {col['name']} {col['type']}"
        if col.get('constraints', {}).get('not_null'):
            sql += " NOT NULL"
        if col.get('constraints', {}).get('default'):
            sql += f" DEFAULT {col['constraints']['default']}"
        sql += ",\n"
    
    # Generate constraints
    # ... add PK, FK, CHECK constraints
    
    sql += ");\n"
    return sql

# Usage
with open('domains/party-management.yaml') as f:
    model = yaml.safe_load(f)
    
for domain in model['domains']:
    for schema in domain['schemas']:
        for table in schema['tables']:
            print(generate_create_table(table))
```

### 2. Documentation Generation

```python
# scripts/generate-docs.py
def generate_markdown_docs(model):
    """Generate Markdown documentation"""
    md = f"# {model['metadata']['model_name']}\n\n"
    md += f"Version: {model['metadata']['version']}\n\n"
    
    for domain in model['domains']:
        md += f"## {domain['display_name']}\n\n"
        md += f"{domain['description']}\n\n"
        
        for schema in domain['schemas']:
            md += f"### Schema: {schema['schema_name']}\n\n"
            
            for table in schema['tables']:
                md += f"#### Table: {table['name']}\n\n"
                md += f"{table['description']}\n\n"
                
                # Generate table of columns
                md += "| Column | Type | Description |\n"
                md += "|--------|------|-------------|\n"
                for col in table['columns']:
                    md += f"| {col['name']} | {col['type']} | {col['description']} |\n"
                md += "\n"
    
    return md
```

### 3. ERD Generation

Use tools like `graphviz` to generate entity-relationship diagrams:

```python
# scripts/generate-erd.py
import graphviz

def generate_erd(model):
    """Generate ERD using graphviz"""
    dot = graphviz.Digraph(comment='Tax Administration Data Model')
    
    # Add tables as nodes
    for domain in model['domains']:
        for schema in domain['schemas']:
            for table in schema['tables']:
                dot.node(table['name'], 
                        label=f"{table['name']}|{format_columns(table)}")
    
    # Add relationships as edges
    for domain in model['domains']:
        for schema in domain['schemas']:
            for table in schema['tables']:
                for rel in table.get('relationships', []):
                    dot.edge(table['name'], rel['target_table'],
                           label=rel['cardinality'])
    
    dot.render('erd-output', format='png')
```

### 4. dbt Model Generation

Generate dbt models for the Data Warehouse:

```python
# scripts/generate-dbt.py
def generate_dbt_source(model):
    """Generate dbt source.yml"""
    sources = {
        'version': 2,
        'sources': []
    }
    
    for domain in model['domains']:
        source = {
            'name': domain['name'],
            'description': domain['description'],
            'tables': []
        }
        
        for schema in domain['schemas']:
            for table in schema['tables']:
                source['tables'].append({
                    'name': table['name'],
                    'description': table['description'],
                    'columns': [
                        {
                            'name': col['name'],
                            'description': col['description']
                        }
                        for col in table['columns']
                    ]
                })
        
        sources['sources'].append(source)
    
    return yaml.dump(sources)
```

## Validation

### Schema Validation Script

```python
# scripts/validate-schema.py
import yaml
import jsonschema

def validate_domain_file(domain_file, schema_file):
    """Validate domain YAML against meta-schema"""
    with open(schema_file) as f:
        schema = yaml.safe_load(f)
    
    with open(domain_file) as f:
        domain = yaml.safe_load(f)
    
    # Convert YAML schema to JSON Schema format
    json_schema = convert_to_json_schema(schema)
    
    # Validate
    try:
        jsonschema.validate(domain, json_schema)
        print(f"✓ {domain_file} is valid")
    except jsonschema.ValidationError as e:
        print(f"✗ {domain_file} validation failed:")
        print(e.message)
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: validate-ta-rdm
        name: Validate TA-RDM YAML
        entry: python scripts/validate-schema.py
        language: python
        files: 'domains/.*\.yaml$'
      
      - id: yamllint
        name: YAML Lint
        entry: yamllint
        language: python
        files: '\.yaml$'
```

## CI/CD Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/ta-rdm.yml
name: TA-RDM Validation and Generation

on:
  push:
    paths:
      - 'domains/**/*.yaml'
  pull_request:
    paths:
      - 'domains/**/*.yaml'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install pyyaml jsonschema
      
      - name: Validate YAML schemas
        run: |
          python scripts/validate-schema.py
      
      - name: Generate DDL
        run: |
          python scripts/generate-ddl.py
      
      - name: Generate Documentation
        run: |
          python scripts/generate-docs.py
      
      - name: Upload artifacts
        uses: actions/upload-artifact@v2
        with:
          name: generated-artifacts
          path: |
            generated/ddl/*.sql
            generated/docs/*.md
```

## Governance and Maintenance

### Version Control Strategy

```
main branch:
  ├── domains/
  │   ├── party-management.yaml (v1.0.0)
  │   └── ...
  └── CHANGELOG.md

feature/new-domain branch:
  └── domains/
      └── new-domain.yaml (draft)
```

### Change Management Process

1. **Propose Change**: Create feature branch
2. **Update YAML**: Modify domain files
3. **Validate**: Run validation scripts
4. **Review**: Pull request review by:
   - Business owners (domain expert review)
   - Technical owners (DBA review)
   - Data architects (consistency review)
5. **Generate Artifacts**: Auto-generate DDL, docs
6. **Test**: Test DDL in dev environment
7. **Merge**: Merge to main after approval
8. **Deploy**: Apply changes to target environments

### Documentation Requirements

Every change must include:
- Updated `description` fields
- Business rule documentation
- Example queries (if applicable)
- Migration notes (breaking changes)
- Updated changelog

## Next Steps

### Immediate Actions

1. **Review Schema Structure**
   - Review `ta-rdm-schema.yaml` 
   - Validate it meets all requirements
   - Suggest refinements

2. **Create Remaining Domains**
   Following the Party Management example:
   - Tax Framework
   - Registration
   - Assessment & Liability
   - Accounting
   - Payment & Refund
   - Compliance & Control
   - Reference Data

3. **Develop Tooling**
   - DDL generator (PostgreSQL first)
   - Documentation generator
   - Validation script
   - ERD generator

4. **Establish Governance**
   - Define review process
   - Set up CI/CD pipeline
   - Create contribution guidelines

### Medium-Term Goals

1. **Complete Model**
   - All 8-9 domains fully specified
   - All reference data documented
   - All business rules captured

2. **Tool Ecosystem**
   - Multi-database DDL generation
   - ORM generation (SQLAlchemy)
   - API schema generation
   - dbt integration

3. **Integration**
   - Link to TCRM-BB (Risk Management BB)
   - Link to Data Warehouse model
   - Link to ARMS requirements

## Conclusion

The YAML-based approach provides:

✅ **Single Source of Truth** for data model  
✅ **Automated Processes** for DDL, docs, ORMs  
✅ **Version Control** with clear change history  
✅ **Validation** ensuring consistency  
✅ **Collaboration** enabling stakeholder review  
✅ **Maintainability** through clear structure  

This foundation will support:
- Rapid prototyping and iteration
- Multiple database platforms
- Data warehouse development
- API and service development
- Documentation generation
- Quality assurance

The investment in comprehensive schema definition pays dividends throughout the entire development lifecycle and long-term maintenance of the Tax Administration system.

---

## Appendix: Quick Reference

### Essential Files

| **File** | **Purpose** |
|----------|------------|
| `ta-rdm-schema.yaml` | Meta-schema defining structure |
| `ta-rdm-example.yaml` | Party Management domain example |
| `generate-ddl.py` | PostgreSQL DDL generator (to create) |
| `validate-schema.py` | YAML validation script (to create) |

### Key Concepts

| **Concept** | **Description** |
|------------|----------------|
| Domain | Logical grouping of related tables |
| Schema | Database namespace containing tables |
| Temporal Data | Support for valid_from/valid_to |
| Audit Trail | created_by, created_date, etc. |
| Reference Data | Static lookup tables |
| Business Rule | Cross-table constraint or validation |

### Common Patterns

```yaml
# Surrogate key pattern
columns:
  - name: "{table}_id"
    type: "bigint"
    constraints:
      primary_key: true
      auto_increment: true

# Foreign key pattern
columns:
  - name: "{referenced_table}_id"
    type: "bigint"
    constraints:
      foreign_key:
        table: "{referenced_table}"
        column: "{referenced_table}_id"
        on_delete: "RESTRICT"

# Temporal pattern
columns:
  - name: "valid_from"
    type: "date"
    constraints:
      not_null: true
  - name: "valid_to"
    type: "date"
  - name: "is_current"
    type: "boolean"
    constraints:
      not_null: true
      default: true

# Audit pattern
columns:
  - name: "created_by"
    type: "varchar(100)"
    constraints:
      not_null: true
  - name: "created_date"
    type: "timestamp with time zone"
    constraints:
      not_null: true
      default: "CURRENT_TIMESTAMP"
```

---

**Document Version:** 1.0.0  
**Last Updated:** 2025-11-04  
**Next Review:** Upon completion of first domain
