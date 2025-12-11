#!/usr/bin/env python3
"""
MTCA Form YAML to dbt Model Generator
=====================================

Reads MTCA form YAML specifications and generates:
1. Source definitions (_sources.yml)
2. Staging models (stg_*.sql)  
3. Intermediate models (int_*.sql)
4. Test definitions (_models.yml)
5. Seed files for reference data

The generator leverages the legacy_mapping and ors_mapping fields in the
form YAML to create complete source-to-L2 ETL pipelines.

Usage:
    python yaml_to_dbt_generator.py --input /path/to/mtca-forms/ --output /path/to/dbt/models/

Requirements:
    pip install pyyaml jinja2

Author: IMF STX Advisory
Date: December 2025
"""

import yaml
import os
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FieldMapping:
    """Represents a field mapping extracted from form YAML"""
    field_id: str
    field_code: str
    field_name_en: str
    field_name_mt: str
    data_type: str
    format: Optional[str]
    max_length: Optional[int]
    required: bool
    source_database: str
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    target_data_type: str
    validation_rules: List[Dict] = field(default_factory=list)
    allowed_values: List[Dict] = field(default_factory=list)
    form_code: str = ""
    section_id: str = ""
    

@dataclass
class FormMetadata:
    """Metadata about a form"""
    form_code: str
    form_name_en: str
    form_name_mt: str
    tax_type: str
    filing_frequency: str
    submission_method: str
    legislative_basis: str
    

class FormYAMLParser:
    """Parses MTCA form YAML specifications"""
    
    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path
        with open(yaml_path, 'r', encoding='utf-8') as f:
            self.data = yaml.safe_load(f)
        self.forms = self.data.get('forms', [])
        
    def get_form_metadata(self) -> List[FormMetadata]:
        """Extract metadata from all forms in the file"""
        metadata_list = []
        
        for form_entry in self.forms:
            form = form_entry.get('form', {})
            meta = form.get('metadata', {})
            
            metadata_list.append(FormMetadata(
                form_code=meta.get('form_code', 'UNKNOWN'),
                form_name_en=meta.get('form_name_en', ''),
                form_name_mt=meta.get('form_name_mt', ''),
                tax_type=meta.get('tax_type', ''),
                filing_frequency=meta.get('filing_frequency', ''),
                submission_method=meta.get('submission_method', ''),
                legislative_basis=meta.get('legislative_basis', '')
            ))
            
        return metadata_list
    
    def extract_mappings(self) -> List[FieldMapping]:
        """Extract all field mappings from forms"""
        mappings = []
        
        for form_entry in self.forms:
            form = form_entry.get('form', {})
            metadata = form.get('metadata', {})
            form_code = metadata.get('form_code', 'UNKNOWN')
            
            for section in form.get('sections', []):
                section_id = section.get('section_id', 'UNKNOWN')
                
                for fld in section.get('fields', []):
                    legacy = fld.get('legacy_mapping')
                    ors = fld.get('ors_mapping')
                    
                    # Only process fields with both mappings
                    if legacy and ors:
                        mappings.append(FieldMapping(
                            field_id=fld.get('field_id', ''),
                            field_code=fld.get('field_code', ''),
                            field_name_en=fld.get('field_name_en', ''),
                            field_name_mt=fld.get('field_name_mt', ''),
                            data_type=fld.get('data_type', 'STRING'),
                            format=fld.get('format'),
                            max_length=fld.get('max_length'),
                            required=fld.get('required', False),
                            source_database=legacy.get('database', ''),
                            source_table=legacy.get('table', ''),
                            source_column=legacy.get('column', ''),
                            target_table=ors.get('table', ''),
                            target_column=ors.get('column', ''),
                            target_data_type=ors.get('data_type', 'String'),
                            validation_rules=fld.get('validation_rules', []),
                            allowed_values=fld.get('allowed_values', []),
                            form_code=form_code,
                            section_id=section_id
                        ))
        
        return mappings
    
    def group_by_source_table(self, mappings: List[FieldMapping]) -> Dict[Tuple[str, str], List[FieldMapping]]:
        """Group mappings by (source_database, source_table)"""
        grouped = defaultdict(list)
        for m in mappings:
            key = (m.source_database, m.source_table)
            grouped[key].append(m)
        return dict(grouped)
    
    def group_by_form_section(self, mappings: List[FieldMapping]) -> Dict[Tuple[str, str], List[FieldMapping]]:
        """Group mappings by (form_code, section_id)"""
        grouped = defaultdict(list)
        for m in mappings:
            key = (m.form_code, m.section_id)
            grouped[key].append(m)
        return dict(grouped)


class DBTModelGenerator:
    """Generates dbt models from field mappings"""
    
    # Informix type to SQL type mapping
    INFORMIX_TO_SQL_TYPES = {
        'TIN': 'varchar(20)',
        'STRING': 'varchar',
        'INTEGER': 'integer',
        'DECIMAL': 'numeric',
        'DATE': 'date',
        'DATETIME': 'timestamp',
        'YEAR': 'integer',
        'CODE': 'varchar(20)',
        'BOOLEAN': 'boolean',
        'CURRENCY': 'numeric(18,2)',
        'PERCENTAGE': 'numeric(5,2)',
        'EMAIL': 'varchar(255)',
        'PHONE': 'varchar(50)',
    }
    
    # dbt transformation macros by data type
    TRANSFORM_MACROS = {
        'TIN': "{{ clean_tin('{col}') }}",
        'STRING': "trim({col})",
        'DATE': "{{ standardize_date('{col}') }}",
        'DATETIME': "{{ standardize_date('{col}') }}",
        'CODE': "upper(trim({col}))",
        'EMAIL': "lower(trim({col}))",
        'DECIMAL': "coalesce({col}, 0)",
        'CURRENCY': "coalesce({col}, 0)",
        'INTEGER': "coalesce({col}, 0)",
        'YEAR': "{col}",
        'BOOLEAN': "coalesce({col}, false)",
        'PERCENTAGE': "coalesce({col}, 0)",
    }
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.generated_files = []
        
    def _sanitize_name(self, name: str) -> str:
        """Sanitize names for SQL identifiers"""
        return re.sub(r'[^a-z0-9_]', '_', name.lower())
    
    def _get_sql_type(self, data_type: str, max_length: Optional[int] = None) -> str:
        """Get SQL type from form data type"""
        base_type = self.INFORMIX_TO_SQL_TYPES.get(data_type, 'varchar')
        if base_type == 'varchar' and max_length:
            return f'varchar({max_length})'
        return base_type
    
    def _get_transform(self, mapping: FieldMapping) -> str:
        """Get transformation SQL for a field"""
        template = self.TRANSFORM_MACROS.get(mapping.data_type, "{col}")
        return template.format(col=mapping.source_column)
    
    def generate_source_yml(self, database: str, tables: List[str]) -> str:
        """Generate _sources.yml for a database"""
        db_safe = self._sanitize_name(database)
        
        source_yml = f'''version: 2

sources:
  - name: {db_safe}
    description: "MTCA {database} Informix database - Auto-generated"
    database: "{{{{ var('informix_{db_safe}') }}}}"
    schema: informix
    freshness:
      warn_after: {{count: 24, period: hour}}
      error_after: {{count: 48, period: hour}}
    loaded_at_field: modified_timestamp
    
    tables:
'''
        for table in sorted(set(tables)):
            table_safe = self._sanitize_name(table)
            source_yml += f'''      - name: {table_safe}
        description: "Source table {table} from {database}"
        identifier: "{table}"
        columns:
          - name: modified_timestamp
            description: "Last modification timestamp for CDC"
'''
        return source_yml
    
    def generate_staging_model(self, database: str, table: str, 
                                mappings: List[FieldMapping]) -> str:
        """Generate staging model SQL"""
        db_safe = self._sanitize_name(database)
        table_safe = self._sanitize_name(table)
        
        # Get primary key columns (required fields, limit to 3)
        pk_cols = [m.source_column for m in mappings if m.required][:3]
        if not pk_cols:
            pk_cols = [mappings[0].source_column] if mappings else ['id']
        
        model_sql = f'''{{{{
  config(
    materialized='incremental',
    unique_key='_surrogate_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['staging', '{db_safe}']
  )
}}}}

/*
  ==========================================================================
  Staging Model: stg_{db_safe}__{table_safe}
  Source: {database}.{table}
  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
  ==========================================================================
  
  Forms using this source:
'''
        # Add form references
        forms = sorted(set(m.form_code for m in mappings))
        for form in forms:
            model_sql += f"    - {form}\n"
        
        model_sql += f'''  
  Mapped columns ({len(mappings)}):
'''
        for m in mappings:
            model_sql += f"    - {m.source_column} -> {m.target_column} ({m.data_type})\n"
        
        model_sql += f'''*/

with source as (
    select * from {{{{ source('{db_safe}', '{table_safe}') }}}}
    {{% if is_incremental() %}}
    where modified_timestamp > (
        select coalesce(max(modified_timestamp), '1900-01-01'::timestamp)
        from {{{{ this }}}}
    )
    {{% endif %}}
),

renamed as (
    select
        -- Surrogate key for incremental processing
        {{{{ dbt_utils.generate_surrogate_key([
'''
        model_sql += ",\n".join([f"            '{col}'" for col in pk_cols])
        model_sql += f'''
        ]) }}}} as _surrogate_key,
        
        -- === Mapped Source Columns ===
'''
        # Add column mappings with type casting
        for m in mappings:
            sql_type = self._get_sql_type(m.data_type, m.max_length)
            comment = f"-- {m.field_code}: {m.field_name_en[:50]}" if m.field_name_en else ""
            model_sql += f"        cast({m.source_column} as {sql_type}) as {m.target_column},  {comment}\n"
        
        model_sql += f'''
        -- === Metadata Columns ===
        modified_timestamp,
        current_timestamp as _loaded_at,
        '{database}' as _source_database,
        '{table}' as _source_table
        
    from source
    where {pk_cols[0]} is not null  -- Filter null keys
)

select * from renamed
'''
        return model_sql
    
    def generate_intermediate_model(self, form_code: str, section_id: str,
                                     mappings: List[FieldMapping]) -> str:
        """Generate intermediate model SQL with transformations"""
        form_safe = self._sanitize_name(form_code)
        section_safe = self._sanitize_name(section_id)
        
        # Get staging references needed
        staging_refs = {}
        for m in mappings:
            db_safe = self._sanitize_name(m.source_database)
            table_safe = self._sanitize_name(m.source_table)
            ref_name = f"stg_{db_safe}__{table_safe}"
            staging_refs[ref_name] = (m.source_database, m.source_table)
        
        # Get primary columns for deduplication
        pk_cols = [m.target_column for m in mappings if m.required][:3]
        
        model_sql = f'''{{{{
  config(
    materialized='incremental',
    unique_key='_record_key',
    incremental_strategy='merge',
    tags=['intermediate', '{form_safe}']
  )
}}}}

/*
  ==========================================================================
  Intermediate Model: int_{form_safe}__{section_safe}
  Form: {form_code}
  Section: {section_id}
  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
  ==========================================================================
  
  Business transformations applied:
    - TIN cleaning and standardization
    - Date validation and standardization
    - Code value normalization
    - Null handling for numeric fields
*/

'''
        # Add CTEs for staging sources
        cte_names = []
        for i, (ref_name, (db, table)) in enumerate(staging_refs.items()):
            alias = self._sanitize_name(table)
            cte_names.append(alias)
            
            if i == 0:
                model_sql += f"with {alias} as (\n"
            else:
                model_sql += f"),\n\n{alias} as (\n"
            
            model_sql += f'''    select * from {{{{ ref('{ref_name}') }}}}
    {{% if is_incremental() %}}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{{{ this }}}})
    {{% endif %}}
'''
        
        model_sql += '''),

transformed as (
    select
        -- Record key for incremental
        {{ dbt_utils.generate_surrogate_key([
'''
        model_sql += ",\n".join([f"            '{col}'" for col in pk_cols])
        model_sql += f'''
        ]) }}}} as _record_key,
        
        -- === Transformed Columns ===
'''
        # Add transformed columns
        for m in mappings:
            transform = self._get_transform(m)
            comment = f"-- {m.field_code}" if m.field_code else ""
            model_sql += f"        {transform} as {m.target_column},  {comment}\n"
        
        model_sql += f'''
        -- === Audit Columns ===
        current_timestamp as _transformed_at,
        '{form_code}' as _source_form,
        '{section_id}' as _source_section
        
    from {cte_names[0]}
)

select * from transformed
'''
        return model_sql
    
    def generate_model_yml(self, model_name: str, mappings: List[FieldMapping]) -> str:
        """Generate model schema YML with tests"""
        
        yml = f'''  - name: {model_name}
    description: "Auto-generated from MTCA form YAML"
    columns:
'''
        for m in mappings:
            yml += f'''      - name: {m.target_column}
        description: "{m.field_name_en}"
'''
            # Add tests based on field properties
            tests = []
            if m.required:
                tests.append("not_null")
            
            # Add tests from validation_rules
            for rule in m.validation_rules:
                rule_type = rule.get('rule_type')
                if rule_type == 'pattern':
                    pattern = rule.get('rule_expression', '')
                    # Escape pattern for YAML
                    pattern_escaped = pattern.replace('"', '\\"')
                    tests.append(f'''dbt_expectations.expect_column_values_to_match_regex:
              regex: "{pattern_escaped}"''')
                elif rule_type == 'range':
                    tests.append('''dbt_expectations.expect_column_values_to_be_between:
              min_value: 0''')
            
            if tests:
                yml += "        tests:\n"
                for test in tests:
                    if '\n' in test:
                        yml += f"          - {test}\n"
                    else:
                        yml += f"          - {test}\n"
        
        return yml
    
    def generate_seed_from_allowed_values(self, mappings: List[FieldMapping]) -> Dict[str, str]:
        """Generate seed CSV files from allowed_values"""
        seeds = {}
        
        for m in mappings:
            if m.allowed_values:
                seed_name = f"ref_{m.target_column}_values"
                csv_content = "code,description_en,description_mt\n"
                for av in m.allowed_values:
                    code = av.get('code', '')
                    desc_en = av.get('description_en', '').replace(',', ';')
                    desc_mt = av.get('description_mt', '').replace(',', ';')
                    csv_content += f"{code},{desc_en},{desc_mt}\n"
                seeds[seed_name] = csv_content
        
        return seeds
    
    def write_file(self, path: Path, content: str):
        """Write content to file, creating directories as needed"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        self.generated_files.append(str(path))
        logger.info(f"Generated: {path}")


def generate_macros() -> Dict[str, str]:
    """Generate standard transformation macros"""
    
    macros = {}
    
    macros['clean_tin.sql'] = '''{% macro clean_tin(column_name) %}
    -- Clean and standardize Tax Identification Number
    upper(trim(regexp_replace({{ column_name }}, '[^0-9A-Za-z]', '', 'g')))
{% endmacro %}
'''
    
    macros['standardize_date.sql'] = '''{% macro standardize_date(column_name) %}
    -- Standardize date with validation
    case
        when {{ column_name }} is null then null
        when {{ column_name }}::date < '1900-01-01'::date then null
        when {{ column_name }}::date > current_date + interval '10 years' then null
        else {{ column_name }}::date
    end
{% endmacro %}
'''
    
    macros['get_incremental_filter.sql'] = '''{% macro get_incremental_filter(timestamp_column='modified_timestamp', lookback_hours=24) %}
    -- Standard incremental filter with lookback
    {% if is_incremental() %}
        where {{ timestamp_column }} > (
            select coalesce(
                max({{ timestamp_column }}),
                '1900-01-01'::timestamp
            ) - interval '{{ lookback_hours }} hours'
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
'''
    
    macros['map_status_code.sql'] = '''{% macro map_status_code(column_name, mapping_seed='ref_status_mapping') %}
    -- Map source status code to canonical value
    coalesce(
        (select target_code from {{ ref(mapping_seed) }} m 
         where m.source_code = {{ column_name }}),
        'UNKNOWN'
    )
{% endmacro %}
'''
    
    return macros


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Generate dbt models from MTCA form YAML specifications'
    )
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to directory containing form YAML files'
    )
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Path to dbt models directory'
    )
    parser.add_argument(
        '--generate-macros',
        action='store_true',
        help='Also generate standard macros'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        return 1
    
    generator = DBTModelGenerator(str(output_path))
    
    # Collect all mappings from all YAML files
    all_mappings = []
    all_forms_metadata = []
    
    yaml_files = list(input_path.glob('*.yaml')) + list(input_path.glob('*.yml'))
    
    if not yaml_files:
        logger.error(f"No YAML files found in: {input_path}")
        return 1
    
    logger.info(f"Found {len(yaml_files)} YAML files to process")
    
    for yaml_file in sorted(yaml_files):
        logger.info(f"Processing: {yaml_file.name}")
        try:
            parser = FormYAMLParser(str(yaml_file))
            mappings = parser.extract_mappings()
            metadata = parser.get_form_metadata()
            all_mappings.extend(mappings)
            all_forms_metadata.extend(metadata)
            logger.info(f"  Found {len(mappings)} field mappings from {len(metadata)} forms")
        except Exception as e:
            logger.error(f"  Error processing {yaml_file.name}: {e}")
            continue
    
    if not all_mappings:
        logger.error("No field mappings found in any YAML file")
        return 1
    
    # Group mappings by source table
    source_parser = FormYAMLParser.__new__(FormYAMLParser)
    by_source = defaultdict(list)
    for m in all_mappings:
        key = (m.source_database, m.source_table)
        by_source[key].append(m)
    
    # Group mappings by form/section
    by_form = defaultdict(list)
    for m in all_mappings:
        key = (m.form_code, m.section_id)
        by_form[key].append(m)
    
    # Generate source YML files
    sources_by_db = defaultdict(set)
    for (db, table), _ in by_source.items():
        sources_by_db[db].add(table)
    
    for db, tables in sources_by_db.items():
        db_safe = generator._sanitize_name(db)
        source_yml = generator.generate_source_yml(db, list(tables))
        output_file = output_path / 'staging' / db_safe / f'_{db_safe}__sources.yml'
        generator.write_file(output_file, source_yml)
    
    # Generate staging models
    for (db, table), mappings in by_source.items():
        db_safe = generator._sanitize_name(db)
        table_safe = generator._sanitize_name(table)
        model_sql = generator.generate_staging_model(db, table, mappings)
        output_file = output_path / 'staging' / db_safe / f'stg_{db_safe}__{table_safe}.sql'
        generator.write_file(output_file, model_sql)
    
    # Generate intermediate models
    for (form, section), mappings in by_form.items():
        form_safe = generator._sanitize_name(form)
        section_safe = generator._sanitize_name(section)
        model_sql = generator.generate_intermediate_model(form, section, mappings)
        
        # Determine subdirectory based on form type
        if 'VAT' in form.upper():
            subdir = 'vat'
        elif 'FSS' in form.upper():
            subdir = 'fss'
        elif 'TRA' in form.upper() or 'CO' in form.upper():
            subdir = 'corporate_tax'
        else:
            subdir = 'other'
        
        output_file = output_path / 'intermediate' / subdir / f'int_{form_safe}__{section_safe}.sql'
        generator.write_file(output_file, model_sql)
    
    # Generate seed files from allowed_values
    seeds = generator.generate_seed_from_allowed_values(all_mappings)
    for seed_name, csv_content in seeds.items():
        output_file = output_path.parent / 'seeds' / f'{seed_name}.csv'
        generator.write_file(output_file, csv_content)
    
    # Generate macros if requested
    if args.generate_macros:
        macros = generate_macros()
        for macro_name, macro_content in macros.items():
            output_file = output_path.parent / 'macros' / 'transforms' / macro_name
            generator.write_file(output_file, macro_content)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("GENERATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total YAML files processed: {len(yaml_files)}")
    logger.info(f"Total forms found: {len(all_forms_metadata)}")
    logger.info(f"Total field mappings: {len(all_mappings)}")
    logger.info(f"Source tables: {len(by_source)}")
    logger.info(f"Form sections: {len(by_form)}")
    logger.info(f"Seed files: {len(seeds)}")
    logger.info(f"Total files generated: {len(generator.generated_files)}")
    logger.info("=" * 60)
    
    return 0


if __name__ == '__main__':
    exit(main())
