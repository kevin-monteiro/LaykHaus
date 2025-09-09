"""
GraphQL Schema Auto-Generation from Metadata Catalog.

Dynamically generates GraphQL types and schemas based on registered data sources.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import strawberry
from strawberry import Schema

from laykhaus.catalog import CatalogService, DataSource, Schema as CatalogSchema, Table, Column
from laykhaus.catalog.models import DataType
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class GraphQLTypeMapping:
    """Maps LaykHaus data types to GraphQL types."""
    
    TYPE_MAP = {
        DataType.STRING: str,
        DataType.INTEGER: int,
        DataType.BIGINT: int,
        DataType.FLOAT: float,
        DataType.DOUBLE: float,
        DataType.BOOLEAN: bool,
        DataType.DATE: str,  # ISO date string
        DataType.TIMESTAMP: str,  # ISO datetime string
        DataType.JSON: str,  # JSON string
        DataType.BINARY: str,  # Base64 string
    }
    
    @classmethod
    def get_graphql_type(cls, data_type: DataType, nullable: bool = True):
        """Get GraphQL type for LaykHaus data type."""
        base_type = cls.TYPE_MAP.get(data_type, str)
        return Optional[base_type] if nullable else base_type


class SchemaGenerator:
    """
    Auto-generates GraphQL schema from metadata catalog.
    
    Creates types, queries, and mutations based on available data sources.
    """
    
    def __init__(self, catalog_service: CatalogService):
        """Initialize with catalog service."""
        self.catalog = catalog_service
        self.logger = get_logger(__name__)
        self._generated_types = {}
        self._generated_queries = {}
    
    def generate_schema(self) -> Schema:
        """
        Generate complete GraphQL schema from catalog metadata.
        
        Returns:
            Strawberry GraphQL schema
        """
        self.logger.info("Generating GraphQL schema from metadata catalog")
        
        # Get all data sources
        data_sources = self.catalog.list_data_sources()
        
        if not data_sources:
            self.logger.warning("No data sources found, generating minimal schema")
            return self._generate_minimal_schema()
        
        # Generate types for each data source
        for source in data_sources:
            if source.schema:
                self._generate_types_for_source(source)
        
        # Create root query type
        query_type = self._generate_query_type()
        
        # Create root mutation type
        mutation_type = self._generate_mutation_type()
        
        # Create subscription type
        subscription_type = self._generate_subscription_type()
        
        # Build schema
        schema = Schema(
            query=query_type,
            mutation=mutation_type,
            subscription=subscription_type
        )
        
        self.logger.info(f"Generated GraphQL schema with {len(self._generated_types)} types")
        return schema
    
    def _generate_types_for_source(self, source: DataSource):
        """Generate GraphQL types for a data source."""
        source_name = source.name.replace('-', '_').replace('.', '_')
        
        for table in source.schema.tables:
            type_name = f"{source_name}_{table.name}".title().replace('_', '')
            
            # Create fields for the type
            fields = {}
            for column in table.columns:
                field_name = column.name
                field_type = GraphQLTypeMapping.get_graphql_type(column.data_type, column.nullable)
                
                # Add description if available
                description = column.description or f"{column.name} from {source.name}.{table.name}"
                
                fields[field_name] = strawberry.field(
                    resolver=None,  # Will be set by resolvers
                    description=description
                )
            
            # Create the type dynamically
            generated_type = strawberry.type(
                type(type_name, (), fields),
                description=f"Table {table.name} from data source {source.name}"
            )
            
            self._generated_types[f"{source_name}.{table.name}"] = generated_type
            
            # Generate query field for this type
            self._generate_query_field_for_table(source, table, generated_type)
    
    def _generate_query_field_for_table(self, source: DataSource, table: Table, graphql_type):
        """Generate query field for a table."""
        source_name = source.name.replace('-', '_').replace('.', '_')
        field_name = f"{source_name}_{table.name}".lower()
        
        # Create query field
        async def table_resolver(
            info,
            limit: Optional[int] = 100,
            offset: Optional[int] = 0,
            where: Optional[str] = None
        ) -> List[graphql_type]:
            # This will be implemented by the resolver
            return []
        
        self._generated_queries[field_name] = strawberry.field(
            table_resolver,
            description=f"Query {table.name} from {source.name}"
        )
    
    def _generate_query_type(self):
        """Generate root Query type."""
        
        @strawberry.type
        class Query:
            # Add health check
            @strawberry.field
            def health(self) -> str:
                return "GraphQL gateway operational"
            
            # Add data sources query
            @strawberry.field
            def data_sources(self) -> List[str]:
                """List available data sources."""
                return [source.name for source in self.catalog.list_data_sources()]
            
            # Add catalog statistics
            @strawberry.field
            def catalog_stats(self) -> str:
                """Get catalog statistics."""
                stats = self.catalog.get_statistics()
                return f"Sources: {stats['total_sources']}, Tables: {stats['total_tables']}"
        
        # Add generated query fields
        for field_name, field in self._generated_queries.items():
            setattr(Query, field_name, field)
        
        return Query
    
    def _generate_mutation_type(self):
        """Generate root Mutation type."""
        
        @strawberry.type
        class Mutation:
            @strawberry.mutation
            def execute_sql(self, query: str) -> str:
                """Execute SQL query through federation engine."""
                # This will be implemented by resolvers
                return f"Would execute: {query}"
        
        return Mutation
    
    def _generate_subscription_type(self):
        """Generate root Subscription type."""
        
        @strawberry.type
        class Subscription:
            @strawberry.subscription
            async def data_updates(self, source: str, table: str):
                """Subscribe to data updates from a table."""
                # This will be implemented with WebSocket support
                yield f"Update from {source}.{table}"
        
        return Subscription
    
    def _generate_minimal_schema(self) -> Schema:
        """Generate minimal schema when no data sources available."""
        
        @strawberry.type
        class Query:
            @strawberry.field
            def health(self) -> str:
                return "GraphQL gateway operational (no data sources)"
            
            @strawberry.field
            def message(self) -> str:
                return "Register data sources to see auto-generated GraphQL types"
        
        @strawberry.type
        class Mutation:
            @strawberry.mutation
            def placeholder(self, input: str) -> str:
                return "No mutations available yet"
        
        return Schema(query=Query, mutation=Mutation)
    
    def get_generated_types(self) -> Dict[str, Any]:
        """Get all generated GraphQL types."""
        return self._generated_types.copy()
    
    def refresh_schema(self) -> Schema:
        """Refresh schema from updated catalog."""
        self._generated_types.clear()
        self._generated_queries.clear()
        return self.generate_schema()