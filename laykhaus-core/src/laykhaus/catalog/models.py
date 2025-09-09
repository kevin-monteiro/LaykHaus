"""
Metadata Catalog data models.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4
from enum import Enum


class SourceType(Enum):
    """Types of data sources."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    KAFKA = "kafka"
    S3 = "s3"
    REDIS = "redis"
    ELASTICSEARCH = "elasticsearch"


class DataType(Enum):
    """Supported data types."""
    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    JSON = "json"
    BINARY = "binary"


@dataclass
class Column:
    """Represents a table column."""
    name: str
    data_type: DataType
    nullable: bool = True
    primary_key: bool = False
    description: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "nullable": self.nullable,
            "primary_key": self.primary_key,
            "description": self.description,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Column":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            data_type=DataType(data["data_type"]),
            nullable=data.get("nullable", True),
            primary_key=data.get("primary_key", False),
            description=data.get("description"),
            metadata=data.get("metadata", {})
        )


@dataclass
class Table:
    """Represents a database table."""
    name: str
    columns: List[Column]
    schema_name: Optional[str] = None
    description: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "columns": [col.to_dict() for col in self.columns],
            "description": self.description,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Table":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            columns=[Column.from_dict(col) for col in data.get("columns", [])],
            schema_name=data.get("schema_name"),
            description=data.get("description"),
            row_count=data.get("row_count"),
            size_bytes=data.get("size_bytes"),
            metadata=data.get("metadata", {})
        )
    
    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


@dataclass
class Schema:
    """Represents a database schema."""
    id: UUID = field(default_factory=uuid4)
    name: str = "default"
    tables: List[Table] = field(default_factory=list)
    version: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "id": str(self.id),
            "name": self.name,
            "tables": [table.to_dict() for table in self.tables],
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Schema":
        """Create from dictionary."""
        return cls(
            id=UUID(data["id"]) if "id" in data else uuid4(),
            name=data.get("name", "default"),
            tables=[Table.from_dict(t) for t in data.get("tables", [])],
            version=data.get("version", 1),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.utcnow(),
            metadata=data.get("metadata", {})
        )
    
    def get_table(self, name: str) -> Optional[Table]:
        """Get table by name."""
        for table in self.tables:
            if table.name == name:
                return table
        return None
    
    def add_table(self, table: Table):
        """Add or update table."""
        existing = self.get_table(table.name)
        if existing:
            self.tables.remove(existing)
        self.tables.append(table)
        self.updated_at = datetime.utcnow()


@dataclass
class DataSource:
    """Represents a data source."""
    id: UUID = field(default_factory=uuid4)
    name: str = ""
    source_type: SourceType = SourceType.POSTGRESQL
    connection_params: Dict = field(default_factory=dict)
    schema: Optional[Schema] = None
    capabilities: Dict = field(default_factory=dict)
    active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "id": str(self.id),
            "name": self.name,
            "source_type": self.source_type.value,
            "connection_params": self.connection_params,
            "schema": self.schema.to_dict() if self.schema else None,
            "capabilities": self.capabilities,
            "active": self.active,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "DataSource":
        """Create from dictionary."""
        return cls(
            id=UUID(data["id"]) if "id" in data else uuid4(),
            name=data.get("name", ""),
            source_type=SourceType(data.get("source_type", "postgresql")),
            connection_params=data.get("connection_params", {}),
            schema=Schema.from_dict(data["schema"]) if data.get("schema") else None,
            capabilities=data.get("capabilities", {}),
            active=data.get("active", True),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.utcnow(),
            metadata=data.get("metadata", {})
        )