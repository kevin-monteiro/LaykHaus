"""
Catalog Service for managing metadata.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID
from pathlib import Path

from laykhaus.catalog.models import DataSource, Schema, Table, Column, SourceType, DataType
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class SchemaVersion:
    """Manages schema versioning."""
    
    def __init__(self, schema: Schema):
        """Initialize with a schema."""
        self.current = schema
        self.versions: List[Schema] = [schema]
    
    def add_version(self, new_schema: Schema):
        """Add a new schema version."""
        new_schema.version = self.current.version + 1
        self.versions.append(new_schema)
        self.current = new_schema
    
    def get_version(self, version: int) -> Optional[Schema]:
        """Get a specific version."""
        for schema in self.versions:
            if schema.version == version:
                return schema
        return None
    
    def get_changes(self, from_version: int, to_version: int) -> Dict:
        """Get changes between versions."""
        from_schema = self.get_version(from_version)
        to_schema = self.get_version(to_version)
        
        if not from_schema or not to_schema:
            return {}
        
        changes = {
            "added_tables": [],
            "removed_tables": [],
            "modified_tables": []
        }
        
        from_tables = {t.name: t for t in from_schema.tables}
        to_tables = {t.name: t for t in to_schema.tables}
        
        # Find added tables
        for name in to_tables:
            if name not in from_tables:
                changes["added_tables"].append(name)
        
        # Find removed tables
        for name in from_tables:
            if name not in to_tables:
                changes["removed_tables"].append(name)
        
        # Find modified tables
        for name in from_tables:
            if name in to_tables:
                from_cols = {c.name for c in from_tables[name].columns}
                to_cols = {c.name for c in to_tables[name].columns}
                if from_cols != to_cols:
                    changes["modified_tables"].append(name)
        
        return changes


class CatalogService:
    """
    Service for managing the metadata catalog.
    
    Handles data source registration, schema discovery, and versioning.
    """
    
    def __init__(self, storage_path: str = "/tmp/laykhaus_catalog"):
        """Initialize catalog service."""
        self.logger = get_logger(__name__)
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.data_sources: Dict[UUID, DataSource] = {}
        self.schema_versions: Dict[UUID, SchemaVersion] = {}
        
        # Load existing catalog
        self._load_catalog()
    
    def register_data_source(self, data_source: DataSource) -> UUID:
        """
        Register a new data source.
        
        Args:
            data_source: DataSource to register
            
        Returns:
            UUID of registered data source
        """
        self.data_sources[data_source.id] = data_source
        
        # Discover schema if connector available
        if data_source.name:
            schema = self.discover_schema(data_source.id)
            if schema:
                data_source.schema = schema
                self.schema_versions[data_source.id] = SchemaVersion(schema)
        
        self._save_catalog()
        self.logger.info(f"Registered data source: {data_source.name}")
        
        return data_source.id
    
    def unregister_data_source(self, source_id: UUID) -> bool:
        """
        Unregister a data source.
        
        Args:
            source_id: UUID of data source
            
        Returns:
            True if unregistered, False if not found
        """
        if source_id in self.data_sources:
            source_name = self.data_sources[source_id].name
            del self.data_sources[source_id]
            
            if source_id in self.schema_versions:
                del self.schema_versions[source_id]
            
            self._save_catalog()
            self.logger.info(f"Unregistered data source: {source_name}")
            return True
        
        return False
    
    def get_data_source(self, source_id: UUID) -> Optional[DataSource]:
        """Get data source by ID."""
        return self.data_sources.get(source_id)
    
    def get_data_source_by_name(self, name: str) -> Optional[DataSource]:
        """Get data source by name."""
        for source in self.data_sources.values():
            if source.name == name:
                return source
        return None
    
    def list_data_sources(self) -> List[DataSource]:
        """List all data sources."""
        return list(self.data_sources.values())
    
    async def discover_schema(self, source_id: UUID) -> Optional[Schema]:
        """
        Discover schema from a data source.
        
        Args:
            source_id: UUID of data source
            
        Returns:
            Discovered schema or None
        """
        data_source = self.data_sources.get(source_id)
        if not data_source:
            return None
        
        # Get connector
        connector = connection_manager.get_connector(data_source.name)
        if not connector:
            self.logger.warning(f"No connector found for {data_source.name}")
            return None
        
        try:
            # Get schema from connector
            raw_schema = await connector.get_schema()
            
            # Convert to catalog schema
            schema = Schema(name=data_source.name)
            
            for table_info in raw_schema:
                # Parse table information
                table_name = table_info.get("name", "")
                columns_info = table_info.get("columns", [])
                
                columns = []
                for col_info in columns_info:
                    column = Column(
                        name=col_info.get("name", ""),
                        data_type=self._map_data_type(col_info.get("type", "")),
                        nullable=col_info.get("nullable", True),
                        primary_key=col_info.get("primary_key", False),
                        description=col_info.get("description")
                    )
                    columns.append(column)
                
                table = Table(
                    name=table_name,
                    columns=columns,
                    schema_name=table_info.get("schema"),
                    row_count=table_info.get("row_count"),
                    size_bytes=table_info.get("size_bytes")
                )
                
                schema.add_table(table)
            
            self.logger.info(f"Discovered schema for {data_source.name}: {len(schema.tables)} tables")
            return schema
            
        except Exception as e:
            self.logger.error(f"Schema discovery failed for {data_source.name}: {e}")
            return None
    
    def update_schema(self, source_id: UUID, new_schema: Schema) -> bool:
        """
        Update schema for a data source.
        
        Args:
            source_id: UUID of data source
            new_schema: New schema
            
        Returns:
            True if updated
        """
        data_source = self.data_sources.get(source_id)
        if not data_source:
            return False
        
        # Version management
        if source_id in self.schema_versions:
            self.schema_versions[source_id].add_version(new_schema)
        else:
            self.schema_versions[source_id] = SchemaVersion(new_schema)
        
        data_source.schema = new_schema
        data_source.updated_at = datetime.utcnow()
        
        self._save_catalog()
        self.logger.info(f"Updated schema for {data_source.name} to version {new_schema.version}")
        
        return True
    
    def get_schema_version(self, source_id: UUID, version: Optional[int] = None) -> Optional[Schema]:
        """
        Get schema version for a data source.
        
        Args:
            source_id: UUID of data source
            version: Version number (None for current)
            
        Returns:
            Schema or None
        """
        if source_id not in self.schema_versions:
            return None
        
        version_mgr = self.schema_versions[source_id]
        
        if version is None:
            return version_mgr.current
        else:
            return version_mgr.get_version(version)
    
    def get_schema_changes(self, source_id: UUID, from_version: int, to_version: int) -> Dict:
        """
        Get schema changes between versions.
        
        Args:
            source_id: UUID of data source
            from_version: Starting version
            to_version: Ending version
            
        Returns:
            Dictionary of changes
        """
        if source_id not in self.schema_versions:
            return {}
        
        return self.schema_versions[source_id].get_changes(from_version, to_version)
    
    def search_tables(self, query: str) -> List[Dict]:
        """
        Search for tables across all data sources.
        
        Args:
            query: Search query
            
        Returns:
            List of matching tables with source info
        """
        results = []
        query_lower = query.lower()
        
        for source in self.data_sources.values():
            if not source.schema:
                continue
            
            for table in source.schema.tables:
                if query_lower in table.name.lower():
                    results.append({
                        "source": source.name,
                        "source_id": str(source.id),
                        "table": table.name,
                        "schema": table.schema_name,
                        "columns": len(table.columns),
                        "row_count": table.row_count
                    })
        
        return results
    
    def search_columns(self, query: str, data_type: Optional[DataType] = None) -> List[Dict]:
        """
        Search for columns across all data sources.
        
        Args:
            query: Search query
            data_type: Optional data type filter
            
        Returns:
            List of matching columns with source and table info
        """
        results = []
        query_lower = query.lower()
        
        for source in self.data_sources.values():
            if not source.schema:
                continue
            
            for table in source.schema.tables:
                for column in table.columns:
                    if query_lower in column.name.lower():
                        if data_type and column.data_type != data_type:
                            continue
                        
                        results.append({
                            "source": source.name,
                            "source_id": str(source.id),
                            "table": table.name,
                            "column": column.name,
                            "data_type": column.data_type.value,
                            "nullable": column.nullable,
                            "primary_key": column.primary_key
                        })
        
        return results
    
    def get_statistics(self) -> Dict:
        """Get catalog statistics."""
        stats = {
            "total_sources": len(self.data_sources),
            "active_sources": sum(1 for s in self.data_sources.values() if s.active),
            "total_tables": 0,
            "total_columns": 0,
            "total_rows": 0,
            "total_size_bytes": 0
        }
        
        for source in self.data_sources.values():
            if source.schema:
                stats["total_tables"] += len(source.schema.tables)
                
                for table in source.schema.tables:
                    stats["total_columns"] += len(table.columns)
                    if table.row_count:
                        stats["total_rows"] += table.row_count
                    if table.size_bytes:
                        stats["total_size_bytes"] += table.size_bytes
        
        return stats
    
    def _map_data_type(self, type_str: str) -> DataType:
        """Map database type string to DataType enum."""
        type_lower = type_str.lower()
        
        if "int" in type_lower:
            if "big" in type_lower:
                return DataType.BIGINT
            return DataType.INTEGER
        elif "float" in type_lower or "real" in type_lower:
            return DataType.FLOAT
        elif "double" in type_lower or "decimal" in type_lower or "numeric" in type_lower:
            return DataType.DOUBLE
        elif "bool" in type_lower:
            return DataType.BOOLEAN
        elif "date" in type_lower and "time" not in type_lower:
            return DataType.DATE
        elif "time" in type_lower:
            return DataType.TIMESTAMP
        elif "json" in type_lower:
            return DataType.JSON
        elif "binary" in type_lower or "blob" in type_lower:
            return DataType.BINARY
        else:
            return DataType.STRING
    
    def _save_catalog(self):
        """Save catalog to disk."""
        catalog_data = {
            "data_sources": {
                str(id): source.to_dict()
                for id, source in self.data_sources.items()
            },
            "updated_at": datetime.utcnow().isoformat()
        }
        
        catalog_file = self.storage_path / "catalog.json"
        with open(catalog_file, "w") as f:
            json.dump(catalog_data, f, indent=2)
    
    def _load_catalog(self):
        """Load catalog from disk."""
        catalog_file = self.storage_path / "catalog.json"
        
        if not catalog_file.exists():
            return
        
        try:
            with open(catalog_file, "r") as f:
                catalog_data = json.load(f)
            
            for id_str, source_data in catalog_data.get("data_sources", {}).items():
                source = DataSource.from_dict(source_data)
                self.data_sources[source.id] = source
                
                if source.schema:
                    self.schema_versions[source.id] = SchemaVersion(source.schema)
            
            self.logger.info(f"Loaded catalog with {len(self.data_sources)} data sources")
            
        except Exception as e:
            self.logger.error(f"Failed to load catalog: {e}")