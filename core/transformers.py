import logging
from abc import ABC, abstractmethod
from typing import Optional

from core.models import Column, Table, Schema


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SchemaTransformer(ABC):
    """Abstract base class for schema transformation"""

    @abstractmethod
    def transform(self, source_schema: Schema) -> Schema:
        """Transform schema to target format"""
        pass


class InformixToClickHouseTransformer(SchemaTransformer):
    """Transform Informix schema to ClickHouse schema"""

    def __init__(self):
        self.type_mappings = {
            'smallint': 'Int16',
            'integer': 'Int32',
            'int': 'Int32',
            'serial': 'Int32',
            'bigint': 'Int64',
            'bigserial': 'Int64',
            'decimal': self._map_decimal,
            'numeric': self._map_decimal,
            'money': 'Decimal64(2)',
            'float': 'Float32',
            'smallfloat': 'Float32',
            'double precision': 'Float64',
            'real': 'Float32',
            'char': self._map_char,
            'character': self._map_char,
            'varchar': 'String',
            'lvarchar': 'String',
            'text': 'String',
            'date': 'Date',
            'datetime': 'DateTime',
            'boolean': 'Bool',
            'byte': 'String',
            'blob': 'String',
            'clob': 'String'
        }

    def transform(self, source_schema: Schema) -> Schema:
        """Transform Informix schema to ClickHouse"""
        transformed_tables = []

        for table in source_schema.tables:
            transformed_table = self._transform_table(table)
            transformed_tables.append(transformed_table)

        return Schema(name=source_schema.name, tables=transformed_tables)

    def _transform_table(self, table: Table) -> Table:
        """Transform a single table"""
        transformed_columns = []

        for column in table.columns:
            transformed_column = self._transform_column(column)
            transformed_columns.append(transformed_column)

        # ClickHouse doesn't use the same constraint system
        # We'll keep primary key info for ORDER BY clause
        constraints = [c for c in table.constraints if c.type == 'PRIMARY KEY']

        return Table(
            name=table.name,
            columns=transformed_columns,
            constraints=constraints,
            comment=table.comment
        )

    def _transform_column(self, column: Column) -> Column:
        """Transform a single column"""
        ch_type = self._map_type(column)

        # Handle nullable in ClickHouse
        if column.nullable and ch_type not in ['String', 'Array', 'JSON']:
            ch_type = f'Nullable({ch_type})'

        return Column(
            name=column.name,
            data_type=ch_type,
            nullable=column.nullable,
            default=self._transform_default(column.default),
            comment=f"Source type: {column.data_type}"
        )

    def _map_type(self, column: Column) -> str:
        """Map Informix type to ClickHouse type"""
        base_type = column.data_type.lower()

        if base_type in self.type_mappings:
            mapping = self.type_mappings[base_type]
            if callable(mapping):
                return mapping(column)
            return mapping

        logger.warning(f"Unknown type {column.data_type} for column {column.name}, using String")
        return 'String'

    def _map_decimal(self, column: Column) -> str:
        """Map decimal/numeric types"""
        precision = column.precision or 18
        scale = column.scale or 2

        if precision <= 9:
            return f'Decimal32({scale})'
        elif precision <= 18:
            return f'Decimal64({scale})'
        else:
            return f'Decimal128({scale})'

    def _map_char(self, column: Column) -> str:
        """Map char types"""
        if column.length and column.length <= 255:
            return f'FixedString({column.length})'
        return 'String'

    def _transform_default(self, default: Optional[str]) -> Optional[str]:
        """Transform default values"""
        if not default:
            return None

        if default.upper() in ['CURRENT', 'TODAY']:
            return 'today()'
        elif 'CURRENT' in default.upper():
            return 'now()'

        return default