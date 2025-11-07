import re
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
import logging

from core.models import Constraint, Column, Table, Schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SchemaParser(ABC):
    """Abstract base class for schema parsers"""

    @abstractmethod
    def parse_file(self, file_path: str) -> Schema:
        """Parse schema from file"""
        pass

    @abstractmethod
    def parse_string(self, content: str) -> Schema:
        """Parse schema from string"""
        pass


class InformixSchemaParser(SchemaParser):
    """Parser for Informix DDL"""

    def __init__(self):
        self.current_schema = "default"

    def parse_file(self, file_path: str) -> Schema:
        """Parse Informix DDL from file"""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        return self.parse_string(content)

    def parse_string(self, content: str) -> Schema:
        """Parse Informix DDL from string"""
        tables = []

        # Remove comments
        content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)

        # Find all CREATE TABLE statements
        # Handle both permanent and temporary tables
        create_table_pattern = r'CREATE\s+(?:TEMP\s+)?TABLE\s+(\w+)\s*\((.*?)\)(?:\s+WITH\s+NO\s+LOG)?'

        for match in re.finditer(create_table_pattern, content, re.IGNORECASE | re.DOTALL):
            table_name = match.group(1)
            table_content = match.group(2)

            # Skip if it's a temp table (contains "INTO TEMP" pattern)
            if re.search(r'INTO\s+TEMP\s+' + table_name, content, re.IGNORECASE):
                logger.debug(f"Skipping temporary table: {table_name}")
                continue

            table = self._parse_table(table_name, table_content)
            if table:
                tables.append(table)
                logger.info(f"Parsed table: {table_name} with {len(table.columns)} columns")

        return Schema(name=self.current_schema, tables=tables)

    def _parse_table(self, table_name: str, content: str) -> Optional[Table]:
        """Parse a single CREATE TABLE statement"""
        columns = []
        constraints = []

        # Split into lines and parse each
        lines = self._split_table_content(content)

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if it's a constraint
            if self._is_constraint(line):
                constraint = self._parse_constraint(line)
                if constraint:
                    constraints.append(constraint)
            else:
                # It's a column definition
                column = self._parse_column(line)
                if column:
                    columns.append(column)

        if not columns:
            return None

        return Table(name=table_name, columns=columns, constraints=constraints)

    def _split_table_content(self, content: str) -> List[str]:
        """Split table content into individual column/constraint definitions"""
        # This is a simplified splitter - in production, use a proper SQL parser
        lines = []
        current = []
        paren_level = 0

        for char in content:
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            elif char == ',' and paren_level == 0:
                lines.append(''.join(current).strip())
                current = []
                continue
            current.append(char)

        if current:
            lines.append(''.join(current).strip())

        return lines

    def _is_constraint(self, line: str) -> bool:
        """Check if line is a constraint definition"""
        constraint_keywords = ['PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'REFERENCES']
        return any(keyword in line.upper() for keyword in constraint_keywords)

    def _parse_column(self, line: str) -> Optional[Column]:
        """Parse a column definition"""
        # Remove inline constraints for now
        line = re.sub(r'\s+(PRIMARY\s+KEY|UNIQUE|NOT\s+NULL|NULL|DEFAULT\s+\S+|CHECK\s*\([^)]+\))',
                     '', line, flags=re.IGNORECASE)

        # Basic pattern for column name and type
        match = re.match(r'^(\w+)\s+(.+?)(?:\s|$)', line)
        if not match:
            return None

        col_name = match.group(1)
        col_type = match.group(2).strip()

        # Parse type details
        type_info = self._parse_data_type(col_type)

        # Check for NOT NULL
        nullable = 'NOT NULL' not in line.upper()

        # Check for DEFAULT
        default_match = re.search(r'DEFAULT\s+(\S+)', line, re.IGNORECASE)
        default = default_match.group(1) if default_match else None

        return Column(
            name=col_name,
            data_type=type_info['base_type'],
            nullable=nullable,
            default=default,
            length=type_info.get('length'),
            precision=type_info.get('precision'),
            scale=type_info.get('scale')
        )

    def _parse_data_type(self, type_str: str) -> Dict[str, Any]:
        """Parse Informix data type string"""
        type_str = type_str.strip()

        # Handle datetime with qualifiers
        if type_str.upper().startswith('DATETIME'):
            return {'base_type': 'datetime'}

        # Extract base type and parameters
        match = re.match(r'(\w+(?:\s+\w+)?)\s*(?:\(([^)]+)\))?', type_str)
        if not match:
            return {'base_type': type_str}

        base_type = match.group(1)
        params = match.group(2)

        result = {'base_type': base_type}

        if params:
            parts = [p.strip() for p in params.split(',')]
            if len(parts) == 1:
                # Could be length or precision
                if base_type.lower() in ['char', 'varchar', 'lvarchar']:
                    result['length'] = int(parts[0])
                else:
                    result['precision'] = int(parts[0])
            elif len(parts) == 2:
                # Precision and scale
                result['precision'] = int(parts[0])
                result['scale'] = int(parts[1])

        return result

    def _parse_constraint(self, line: str) -> Optional[Constraint]:
        """Parse a constraint definition"""
        # PRIMARY KEY
        pk_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', line, re.IGNORECASE)
        if pk_match:
            columns = [col.strip() for col in pk_match.group(1).split(',')]
            return Constraint(name=None, type='PRIMARY KEY', columns=columns)

        # FOREIGN KEY
        fk_match = re.search(r'FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(\w+)\s*\(([^)]+)\)',
                            line, re.IGNORECASE)
        if fk_match:
            columns = [col.strip() for col in fk_match.group(1).split(',')]
            ref_table = fk_match.group(2)
            ref_columns = [col.strip() for col in fk_match.group(3).split(',')]
            return Constraint(
                name=None,
                type='FOREIGN KEY',
                columns=columns,
                referenced_table=ref_table,
                referenced_columns=ref_columns
            )

        # UNIQUE
        unique_match = re.search(r'UNIQUE\s*\(([^)]+)\)', line, re.IGNORECASE)
        if unique_match:
            columns = [col.strip() for col in unique_match.group(1).split(',')]
            return Constraint(name=None, type='UNIQUE', columns=columns)

        return None