from dataclasses import asdict, dataclass
from typing import Dict, Optional, List, Any


@dataclass
class Column:
    """Represents a database column"""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    comment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class Constraint:
    """Represents a database constraint"""
    name: Optional[str]
    type: str  # PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
    columns: List[str]
    referenced_table: Optional[str] = None
    referenced_columns: Optional[List[str]] = None
    check_expression: Optional[str] = None


@dataclass
class Table:
    """Represents a database table"""
    name: str
    columns: List[Column]
    constraints: List[Constraint]
    comment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns],
            'constraints': [asdict(con) for con in self.constraints],
            'comment': self.comment
        }


@dataclass
class Schema:
    """Represents a database schema"""
    name: str
    tables: List[Table]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'tables': [table.to_dict() for table in self.tables]
        }
