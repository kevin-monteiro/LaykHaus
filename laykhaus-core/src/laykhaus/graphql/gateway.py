"""
GraphQL Gateway for LaykHaus.

Integrates GraphQL schema generation, resolvers, and security into a unified gateway.
"""

from typing import Dict, Any, Optional, List, AsyncGenerator
import asyncio

import strawberry
from strawberry import Schema
from strawberry.fastapi import GraphQLRouter

from laykhaus.catalog import CatalogService
from laykhaus.federation import FederationExecutor
from laykhaus.security import RBACManager, DataMaskingEngine
from laykhaus.core.logging import get_logger
from .schema_generator import SchemaGenerator
from .resolvers import (
    QueryResolver, 
    MutationResolver, 
    SubscriptionResolver,
    QueryResult,
    DataSourceInfo,
    SchemaInfo
)

logger = get_logger(__name__)


class GraphQLGateway:
    """
    Unified GraphQL Gateway for LaykHaus.
    
    Combines auto-generated schema, resolvers, and security into a single interface.
    """
    
    def __init__(self, catalog_service: CatalogService, federation_executor: FederationExecutor):
        """Initialize GraphQL gateway with required services."""
        self.catalog = catalog_service
        self.executor = federation_executor
        self.rbac = RBACManager()
        self.logger = get_logger(__name__)
        
        # Initialize components
        self.schema_generator = SchemaGenerator(catalog_service)
        self.query_resolver = QueryResolver(catalog_service, federation_executor, self.rbac)
        self.mutation_resolver = MutationResolver(catalog_service, federation_executor, self.rbac)
        self.subscription_resolver = SubscriptionResolver(catalog_service, federation_executor)
        
        # Generate initial schema
        self._schema = None
        self._router = None
    
    def get_schema(self) -> Schema:
        """Get or create GraphQL schema."""
        if self._schema is None:
            self._schema = self._create_unified_schema()
        return self._schema
    
    def get_router(self, path: str = "/graphql") -> GraphQLRouter:
        """Get FastAPI GraphQL router."""
        if self._router is None:
            schema = self.get_schema()
            self._router = GraphQLRouter(
                schema,
                path=path,
                graphql_ide="graphiql"  # Enable GraphiQL IDE
            )
        return self._router
    
    def _create_unified_schema(self) -> Schema:
        """Create unified GraphQL schema combining generated and custom types."""
        
        # Capture resolvers in closure for schema access
        query_resolver = self.query_resolver
        mutation_resolver = self.mutation_resolver
        subscription_resolver = self.subscription_resolver
        
        # Create unified Query type
        @strawberry.type
        class Query:
            # Health and system queries
            @strawberry.field
            def health(self) -> str:
                return query_resolver.health()
            
            @strawberry.field
            def data_sources(self, info) -> List[DataSourceInfo]:
                return query_resolver.data_sources(info)
            
            @strawberry.field  
            def catalog_stats(self, info) -> str:
                return query_resolver.catalog_stats(info)
            
            @strawberry.field
            def schemas(self, info) -> List[SchemaInfo]:
                return query_resolver.schemas(info)
            
            # SQL execution  
            @strawberry.field
            async def execute_sql(
                self,
                info,
                query: str,
                limit: Optional[int] = 100,
                user_id: Optional[str] = None
            ) -> QueryResult:
                return await query_resolver.execute_sql(info, query, limit, user_id)
        
        # Create unified Mutation type
        @strawberry.type
        class Mutation:
            @strawberry.mutation
            async def execute_sql_mutation(
                self,
                info,
                query: str,
                user_id: Optional[str] = None
            ) -> str:
                return await mutation_resolver.execute_sql_mutation(info, query, user_id)
            
            @strawberry.mutation
            async def register_data_source(
                self,
                info,
                name: str,
                connection_url: str,
                source_type: str,
                user_id: Optional[str] = None
            ) -> str:
                return await mutation_resolver.register_data_source(
                    info, name, connection_url, source_type, user_id
                )
        
        # Create unified Subscription type
        @strawberry.type
        class Subscription:
            @strawberry.subscription
            async def data_updates(
                self,
                info,
                source: str,
                table: str,
                user_id: Optional[str] = None
            ) -> AsyncGenerator[str, None]:
                async for update in subscription_resolver.data_updates(info, source, table, user_id):
                    yield update
            
            @strawberry.subscription
            async def query_performance_metrics(
                self,
                info,
                user_id: Optional[str] = None
            ) -> AsyncGenerator[str, None]:
                async for metrics in subscription_resolver.query_performance_metrics(info, user_id):
                    yield metrics
        
        # Create schema
        schema = Schema(
            query=Query,
            mutation=Mutation,
            subscription=Subscription
        )
        
        self.logger.info("Unified GraphQL schema created successfully")
        return schema
    
    async def refresh_schema(self) -> Schema:
        """Refresh schema from updated catalog metadata."""
        self.logger.info("Refreshing GraphQL schema from catalog updates")
        
        # Clear cached schema
        self._schema = None
        self._router = None
        
        # Regenerate schema
        new_schema = self.get_schema()
        
        self.logger.info("GraphQL schema refreshed successfully")
        return new_schema
    
    def get_introspection_query(self) -> str:
        """Get GraphQL introspection query for schema exploration."""
        return """
        query IntrospectionQuery {
          __schema {
            queryType { name }
            mutationType { name }
            subscriptionType { name }
            types {
              ...FullType
            }
          }
        }
        
        fragment FullType on __Type {
          kind
          name
          description
          fields(includeDeprecated: true) {
            name
            description
            args {
              ...InputValue
            }
            type {
              ...TypeRef
            }
            isDeprecated
            deprecationReason
          }
          inputFields {
            ...InputValue
          }
          interfaces {
            ...TypeRef
          }
          enumValues(includeDeprecated: true) {
            name
            description
            isDeprecated
            deprecationReason
          }
          possibleTypes {
            ...TypeRef
          }
        }
        
        fragment InputValue on __InputValue {
          name
          description
          type { ...TypeRef }
          defaultValue
        }
        
        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
        """
    
    async def notify_data_change(self, source: str, table: str, change_type: str, data: Dict[str, Any]):
        """Notify GraphQL subscriptions of data changes."""
        await self.subscription_resolver.notify_data_change(source, table, change_type, data)
    
    def get_playground_html(self, endpoint: str = "/graphql") -> str:
        """Generate GraphQL Playground HTML page."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="user-scalable=no, initial-scale=1.0, minimum-scale=1.0, maximum-scale=1.0, minimal-ui">
            <title>LaykHaus GraphQL Playground</title>
            <link rel="stylesheet" href="//cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
            <link rel="shortcut icon" href="//cdn.jsdelivr.net/npm/graphql-playground-react/build/favicon.png" />
            <script src="//cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>
        </head>
        <body>
            <div id="root">
                <style>
                    body {{ background: rgb(23, 42, 58); font-family: "Open Sans", sans-serif; height: 90vh; }}
                    #root {{ height: 100%; width: 100%; display: flex; align-items: center; justify-content: center; }}
                    .loading {{ font-size: 32px; font-weight: 200; color: rgba(255, 255, 255, .6); margin-left: 20px; }}
                    img {{ width: 78px; height: 78px; }}
                    .title {{ font-weight: 400; }}
                </style>
                <img src="//cdn.jsdelivr.net/npm/graphql-playground-react/build/logo.png" alt="">
                <div class="loading"> Loading
                    <span class="title">LaykHaus GraphQL Playground</span>
                </div>
            </div>
            <script>
                window.addEventListener('load', function (event) {{
                    GraphQLPlayground.init(document.getElementById('root'), {{
                        endpoint: '{endpoint}',
                        settings: {{
                            'request.credentials': 'same-origin',
                        }},
                        tabs: [
                            {{
                                endpoint: '{endpoint}',
                                query: `# Welcome to LaykHaus GraphQL Playground
# 
# LaykHaus is a federated data lakehouse platform that automatically
# generates GraphQL APIs from your data sources.
#
# Example queries:

# Get system health
query HealthCheck {{
  health
  catalogStats
}}

# List all data sources
query DataSources {{
  dataSources {{
    name
    type
    status
    tableCount
    lastUpdated
  }}
}}

# Execute SQL through GraphQL
query ExecuteSQL {{
  executeSql(query: "SELECT COUNT(*) as total FROM sample_table") {{
    columns
    data
    totalRows
    executionTimeMs
    pushdownCoverage
  }}
}}

# Subscribe to real-time data updates (requires WebSocket)
subscription DataUpdates {{
  dataUpdates(source: "sample_db", table: "users") 
}}`
                            }}
                        ]
                    }})
                }})
            </script>
        </body>
        </html>
        """
    
    def get_health_info(self) -> Dict[str, Any]:
        """Get GraphQL gateway health information."""
        try:
            sources = self.catalog.list_data_sources()
            return {
                "status": "healthy",
                "version": "1.0.0",
                "schema_version": getattr(self._schema, 'version', '1.0'),
                "data_sources": len(sources),
                "endpoints": {
                    "graphql": "/graphql",
                    "playground": "/graphql",
                    "introspection": "/graphql?query=query+IntrospectionQuery{__schema{types{name}}}"
                },
                "features": [
                    "Auto-generated schema from metadata catalog",
                    "Real-time subscriptions via WebSockets", 
                    "Field-level security with RBAC",
                    "Dynamic data masking",
                    "Federated query execution",
                    "Query optimization with pushdown"
                ]
            }
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }