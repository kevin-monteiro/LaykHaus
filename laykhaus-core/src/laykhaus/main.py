"""
Main application entry point for LaykHaus platform.
"""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from prometheus_client import make_asgi_app

from laykhaus.catalog import CatalogService, DataSource, Schema, Table, Column
from laykhaus.catalog.models import SourceType, DataType
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.config import settings
from laykhaus.core.logging import get_logger
from laykhaus.federation import FederationExecutor, QueryParser, QueryPlanner, QueryOptimizer
from laykhaus.federation.spark_federated_executor import SparkFederatedExecutor
from laykhaus.gateway import PGWireGateway
from laykhaus.graphql import GraphQLGateway
from laykhaus.graphql.websocket import websocket_manager
from laykhaus.security import RBACManager, DataMaskingEngine, SecurityContext
from laykhaus.ml import MLFramework
from laykhaus.streaming.kafka_engine import KafkaStreamingEngine, StreamConfig, ConsumerConfig
from laykhaus.streaming.spark_engine import SparkAnalyticsEngine, SparkConfig, SparkJob, SparkJobType

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager.
    Handles startup and shutdown operations.
    """
    # Startup
    logger.info("Starting LaykHaus platform")
    
    try:
        # Initialize connection manager
        await connection_manager.initialize()
        logger.info("Connection manager initialized")
        
        # Initialize Spark-based federation engine
        app.state.spark_federated_executor = SparkFederatedExecutor()
        app.state.federation_executor = app.state.spark_federated_executor
        logger.info("Spark-based federation engine initialized")
        
        # Initialize catalog service
        app.state.catalog_service = CatalogService()
        logger.info("Catalog service initialized")
        
        # Initialize security components
        app.state.rbac_manager = RBACManager()
        app.state.masking_engine = DataMaskingEngine()
        logger.info("Security components initialized")
        
        # Initialize basic ML components (simplified)
        app.state.ml_framework = MLFramework()
        logger.info("ML framework initialized")
        
        # Initialize Kafka Streaming Engine - THE HERO!
        app.state.kafka_engine = KafkaStreamingEngine()
        await app.state.kafka_engine.initialize()
        
        # Create essential streaming topics
        core_streams = [
            StreamConfig("laykhaus-data", partition_count=4, retention_hours=24),
            StreamConfig("stoksenti-stream", partition_count=6, retention_hours=48)
        ]
        
        for stream_config in core_streams:
            await app.state.kafka_engine.create_stream(stream_config)
        
        logger.info("🚀 Kafka Streaming Engine initialized - THE DATA STREAMING HERO!")
        
        # Initialize Spark Analytics Engine - THE OTHER HERO!
        spark_config = SparkConfig(
            app_name="LaykHaus-Analytics",
            max_cores=4,
            executor_memory="2g",
            driver_memory="1g"
        )
        app.state.spark_engine = SparkAnalyticsEngine(spark_config)
        await app.state.spark_engine.initialize()
        logger.info("⚡ Spark Analytics Engine initialized - THE BIG DATA ANALYTICS HERO!")
        
        # Initialize GraphQL gateway
        app.state.graphql_gateway = GraphQLGateway(
            catalog_service=app.state.catalog_service,
            federation_executor=app.state.federation_executor
        )
        logger.info("GraphQL gateway initialized")
        
        # Start PostgreSQL wire protocol gateway
        app.state.pg_gateway = PGWireGateway(
            executor=app.state.federation_executor,
            host="0.0.0.0",
            port=5434  # Use port 5434 to avoid conflicts
        )
        # Start gateway in background task
        asyncio.create_task(app.state.pg_gateway.start())
        logger.info("PostgreSQL wire protocol gateway started on port 5434")
        
        logger.info("LaykHaus platform started successfully")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down LaykHaus platform")
    
    try:
        # Shutdown connection manager
        await connection_manager.shutdown()
        
        # Cleanup catalog
        if hasattr(app.state, "catalog_service"):
            app.state.catalog_service._save_catalog()
        
        # Stop PostgreSQL wire protocol gateway
        if hasattr(app.state, "pg_gateway"):
            await app.state.pg_gateway.stop()
        
        logger.info("LaykHaus platform shut down successfully")
        
    except Exception as e:
        logger.error(f"Shutdown error: {e}")


# Create FastAPI application
app = FastAPI(
    title="LaykHaus",
    description="Federated Data Lakehouse Platform",
    version=settings.app_version,
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Register Spark federation endpoints
from laykhaus.api.spark_endpoints import router as spark_router
app.include_router(spark_router)

# GraphQL endpoint will be mounted after startup


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "LaykHaus",
        "version": settings.app_version,
        "status": "running",
        "environment": settings.environment,
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    Returns overall system health and component statuses.
    """
    try:
        # Get connector health statuses
        connector_health = await connection_manager.get_health_status()
        
        # Determine overall health
        all_healthy = all(status.healthy for status in connector_health.values())
        
        health_status = {
            "status": "healthy" if all_healthy else "degraded",
            "version": settings.app_version,
            "environment": settings.environment,
            "components": {
                "connectors": {
                    name: {
                        "healthy": status.healthy,
                        "latency_ms": status.latency_ms,
                        "message": status.message,
                    }
                    for name, status in connector_health.items()
                }
            }
        }
        
        if not all_healthy:
            return JSONResponse(status_code=503, content=health_status)
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )


@app.get("/api/v1/connectors")
async def list_connectors():
    """
    List all available connectors.
    """
    connectors = []
    
    for connector_id in connection_manager.list_connectors():
        connector = connection_manager.get_connector(connector_id)
        if connector:
            capabilities = connector.get_capabilities()
            
            # Get health status for each connector
            try:
                health = await connector.health_check()
                status = "active" if health.healthy else "error"
            except:
                status = "error"
            
            # Extract name from connector_id or use a default
            name = connector_id.replace("_", " ").title()
            if connector_id.startswith("postgres_"):
                name = connector_id[9:].replace("_", " ").title()
            elif connector_id.startswith("kafka_"):
                name = connector_id[6:].replace("_", " ").title()
            elif connector_id.startswith("rest_"):
                name = connector_id[5:].replace("_", " ").title()
            
            # Build config structure for UI compatibility
            config = {
                "connection": {},
                "authentication": {}
            }
            
            if hasattr(connector, "config"):
                if capabilities.connector_type.value == "postgresql":
                    config["connection"] = {
                        "host": connector.config.host,
                        "port": connector.config.port,
                        "database": connector.config.database,
                        "username": connector.config.username,
                        "schema": connector.config.extra_params.get("schema", "public")
                    }
                elif capabilities.connector_type.value == "kafka":
                    config["connection"] = {
                        "brokers": connector.config.extra_params.get("bootstrap_servers", ""),
                        "topics": connector.config.extra_params.get("topics", []),
                        "group_id": connector.config.extra_params.get("consumer_group", "")
                    }
                elif capabilities.connector_type.value == "rest_api":
                    config["connection"] = {
                        "base_url": connector.config.extra_params.get("base_url", ""),
                        "auth_type": connector.config.extra_params.get("auth_type", "none")
                    }
            
            connectors.append({
                "id": connector_id,
                "name": name,
                "type": capabilities.connector_type.value,
                "connected": connector.is_connected,
                "status": status,
                "config": config,
                "metadata": {
                    "createdAt": datetime.now().isoformat(),
                    "updatedAt": datetime.now().isoformat()
                },
                "sql_features": [f.value for f in capabilities.sql_features],
                "pushdown_capabilities": [p.value for p in capabilities.pushdown_capabilities],
                "supports_streaming": capabilities.supports_streaming,
            })
    
    return {"connectors": connectors}


@app.get("/api/v1/connectors/stats")
async def get_connector_stats():
    """
    Get connector statistics.
    """
    connectors = []
    total = 0
    active = 0
    inactive = 0
    error = 0
    
    for connector_id in connection_manager.list_connectors():
        connector = connection_manager.get_connector(connector_id)
        if connector:
            total += 1
            
            # Check health status
            try:
                health = await connector.health_check()
                if health.healthy:
                    active += 1
                else:
                    error += 1
            except:
                error += 1
            
            if not connector.is_connected:
                inactive += 1
                active -= 1 if connector.is_connected else 0
    
    return {
        "total": total,
        "active": active,
        "inactive": inactive,
        "error": error,
        "by_type": {
            "postgresql": sum(1 for cid in connection_manager.list_connectors() 
                            if "postgres" in cid.lower()),
            "kafka": sum(1 for cid in connection_manager.list_connectors() 
                        if "kafka" in cid.lower()),
            "rest_api": sum(1 for cid in connection_manager.list_connectors() 
                           if "rest" in cid.lower()),
        }
    }


@app.get("/api/v1/connectors/{connector_id}")
async def get_connector_details(connector_id: str):
    """
    Get detailed information about a specific connector.
    """
    connector = connection_manager.get_connector(connector_id)
    
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    capabilities = connector.get_capabilities()
    health = await connector.health_check()
    
    try:
        statistics = await connector.get_statistics()
    except:
        statistics = None
    
    return {
        "id": connector_id,
        "type": capabilities.connector_type.value,
        "connected": connector.is_connected,
        "health": {
            "healthy": health.healthy,
            "latency_ms": health.latency_ms,
            "message": health.message,
        },
        "capabilities": {
            "sql_features": [f.value for f in capabilities.sql_features],
            "data_types": [d.value for d in capabilities.data_types],
            "aggregate_functions": [a.value for a in capabilities.aggregate_functions],
            "join_types": [j.value for j in capabilities.join_types],
            "pushdown_capabilities": [p.value for p in capabilities.pushdown_capabilities],
            "transaction_support": capabilities.transaction_support.value,
            "consistency_guarantee": capabilities.consistency_guarantee.value,
            "supports_streaming": capabilities.supports_streaming,
            "supports_schema_discovery": capabilities.supports_schema_discovery,
            "supports_statistics": capabilities.supports_statistics,
            "supports_explain_plan": capabilities.supports_explain_plan,
        },
        "statistics": {
            "table_count": statistics.table_count if statistics else 0,
            "total_rows": statistics.total_rows if statistics else 0,
            "total_size_bytes": statistics.total_size_bytes if statistics else 0,
        } if statistics else None,
    }


@app.get("/api/v1/connectors/{connector_id}/schema")
async def get_connector_schema(connector_id: str):
    """
    Get schema information from a connector.
    """
    connector = connection_manager.get_connector(connector_id)
    
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    if not connector.is_connected:
        raise HTTPException(status_code=503, detail=f"Connector {connector_id} not connected")
    
    try:
        schema = await connector.get_schema()
        return {"connector_id": connector_id, "schema": schema}
    except Exception as e:
        logger.error(f"Failed to get schema from {connector_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/connectors/{connector_id}/test")
async def test_connector(connector_id: str):
    """
    Test a connector's connection and basic functionality.
    """
    connector = connection_manager.get_connector(connector_id)
    
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    # Test health
    health = await connector.health_check()
    
    # Test basic query
    test_results = {
        "health_check": {
            "passed": health.healthy,
            "latency_ms": health.latency_ms,
            "message": health.message,
        }
    }
    
    # Try a simple query
    try:
        if connector.get_capabilities().connector_type.value == "postgresql":
            result = await connector.execute_query("SELECT 1 as test")
            test_results["query_test"] = {
                "passed": True,
                "rows_returned": result.row_count,
                "execution_time_ms": result.execution_time_ms,
            }
        elif connector.get_capabilities().connector_type.value == "kafka":
            # For Kafka, just check if we can get schema
            schema = await connector.get_schema()
            test_results["schema_test"] = {
                "passed": True,
                "topics_found": len(schema),
            }
    except Exception as e:
        test_results["query_test"] = {
            "passed": False,
            "error": str(e),
        }
    
    return {
        "connector_id": connector_id,
        "all_tests_passed": all(t.get("passed", False) for t in test_results.values()),
        "test_results": test_results,
    }


@app.post("/api/v1/connectors")
async def create_connector(request: Dict):
    """
    Create a new connector configuration.
    """
    try:
        # Ignore any connector_id from the request - always generate our own
        if "id" in request or "connector_id" in request:
            logger.warning("Ignoring connector_id from request - will generate new ID")
        
        connector_type = request.get("type", "").lower()
        config = request.get("config", {})
        name = request.get("name", f"connector_{connector_type}")
        
        # Log the received config for debugging
        logger.info(f"Creating connector with type: {connector_type}, config: {config}")
        
        if connector_type == "postgresql":
            from laykhaus.connectors.postgres_connector import PostgreSQLConnector
            from laykhaus.connectors.base import ConnectionConfig
            import secrets
            
            # Generate unique ID with pgsql_ prefix
            hex_suffix = secrets.token_hex(8)
            connector_id = f"pgsql_{hex_suffix}"
            # Support both flat and nested config
            connection = config.get("connection", config)
            conn_config = ConnectionConfig(
                host=connection.get("host", "localhost"),
                port=connection.get("port", 5432),
                database=connection.get("database"),
                username=connection.get("username"),
                password=connection.get("password"),
                extra_params={"schema": connection.get("schema", "public")}
            )
            connector = PostgreSQLConnector(connector_id, conn_config)
            await connector.connect()
            await connection_manager.add_connector(connector_id, connector)
            
        elif connector_type == "kafka":
            from laykhaus.connectors.kafka_connector import KafkaConnector
            from laykhaus.connectors.base import ConnectionConfig
            import secrets
            
            # Generate unique ID with kafka_ prefix
            hex_suffix = secrets.token_hex(8)
            connector_id = f"kafka_{hex_suffix}"
            # Kafka uses host:port format in brokers
            brokers = config.get("brokers", "localhost:9092")
            
            # Parse first broker for base connection config
            first_broker = brokers.split(",")[0]
            host, port = first_broker.split(":")
            conn_config = ConnectionConfig(
                host=host,
                port=int(port),
                extra_params={
                    "topics": config.get("topics", []),
                    "consumer_group": config.get("group_id", "laykhaus-consumer"),
                    "bootstrap_servers": brokers
                }
            )
            connector = KafkaConnector(connector_id, conn_config)
            await connector.connect()
            await connection_manager.add_connector(connector_id, connector)
            
        elif connector_type == "rest" or connector_type == "rest_api":
            from laykhaus.connectors.rest_api_connector import RESTAPIConnector
            from laykhaus.connectors.base import ConnectionConfig
            import secrets
            
            # Generate unique ID with rest_ prefix
            hex_suffix = secrets.token_hex(8)
            connector_id = f"rest_{hex_suffix}"
            
            # Parse base URL or use host:port
            base_url = config.get("base_url")
            if base_url:
                # Extract host and port from URL if possible
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                host = parsed.hostname or "localhost"
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
            else:
                host = config.get("host", "localhost")
                port = config.get("port", 80)
                base_url = f"http://{host}:{port}"
            
            conn_config = ConnectionConfig(
                host=host,
                port=port,
                extra_params={
                    "base_url": base_url,
                    "auth_type": config.get("auth_type", "none"),
                    "auth_config": config.get("auth_config", {})
                }
            )
            connector = RESTAPIConnector(connector_id, conn_config)
            await connector.connect()
            await connection_manager.add_connector(connector_id, connector)
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported connector type: {connector_type}")
        
        return {
            "id": connector_id,
            "name": name,
            "type": connector_type,
            "config": config,
            "connected": connector.is_connected,
            "message": f"Connector {connector_id} created successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create connector: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/connectors/{connector_id}")
async def update_connector(connector_id: str, request: Dict):
    """
    Update an existing connector configuration.
    """
    try:
        # Ignore any attempt to change the connector_id
        if "id" in request or "connector_id" in request:
            logger.warning("Ignoring connector_id in update request - ID cannot be changed")
        
        # Remove the old connector
        old_connector = connection_manager.get_connector(connector_id)
        if not old_connector:
            raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
        
        await old_connector.disconnect()
        await connection_manager.remove_connector(connector_id)
        
        # Recreate connector with same ID but updated config
        connector_type = request.get("type", "").lower()
        config = request.get("config", {})
        name = request.get("name", connector_id)
        
        # Log the received config for debugging
        logger.info(f"Updating connector {connector_id} with config: {config}")
        
        # IMPORTANT: Keep the existing connector_id for all updates
        # The connector_id is immutable and cannot be changed
        
        if connector_type == "postgresql":
            from laykhaus.connectors.postgres_connector import PostgreSQLConnector
            from laykhaus.connectors.base import ConnectionConfig
            
            conn_config = ConnectionConfig(
                host=config.get("host", "localhost"),
                port=config.get("port", 5432),
                database=config.get("database"),
                username=config.get("username"),
                password=config.get("password"),
                extra_params={"schema": config.get("schema", "public")}
            )
            connector = PostgreSQLConnector(connector_id, conn_config)
            await connector.connect()
            await connection_manager.add_connector(connector_id, connector)
            
        elif connector_type == "kafka":
            from laykhaus.connectors.kafka_connector import KafkaConnector
            from laykhaus.connectors.base import ConnectionConfig
            
            # Kafka uses host:port format in brokers
            brokers = config.get("brokers", "localhost:9092")
            
            # If connecting from within container network, use internal address
            if "localhost" in brokers or "127.0.0.1" in brokers:
                brokers = "kafka:29092"  # Use internal listener for container-to-container
            
            # Parse first broker for base connection config
            first_broker = brokers.split(",")[0]
            host, port = first_broker.split(":")
            conn_config = ConnectionConfig(
                host=host,
                port=int(port),
                extra_params={
                    "topics": config.get("topics", []),
                    "consumer_group": config.get("group_id", "laykhaus-consumer"),
                    "bootstrap_servers": brokers
                }
            )
            connector = KafkaConnector(connector_id, conn_config)
            await connector.connect()
            await connection_manager.add_connector(connector_id, connector)
            
        elif connector_type == "rest" or connector_type == "rest_api":
            from laykhaus.connectors.rest_api_connector import RESTAPIConnector
            from laykhaus.connectors.base import ConnectionConfig
            
            # Keep the existing connector_id - don't regenerate for updates
            
            # Parse base URL or use host:port
            base_url = config.get("base_url")
            if base_url:
                # Extract host and port from URL if possible
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                host = parsed.hostname or "localhost"
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
            else:
                host = config.get("host", "localhost")
                port = config.get("port", 80)
                base_url = f"http://{host}:{port}"
            
            conn_config = ConnectionConfig(
                host=host,
                port=port,
                extra_params={
                    "base_url": base_url,
                    "auth_type": config.get("auth_type", "none"),
                    "auth_config": config.get("auth_config", {})
                }
            )
            connector = RESTAPIConnector(connector_id, conn_config)
            await connector.connect()
            await connection_manager.add_connector(connector_id, connector)
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported connector type: {connector_type}")
        
        return {
            "id": connector_id,
            "name": name,
            "type": connector_type,
            "config": config,
            "connected": connector.is_connected,
            "message": f"Connector {connector_id} updated successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to update connector: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/connectors/{connector_id}")
async def delete_connector(connector_id: str):
    """
    Delete a connector configuration.
    """
    try:
        connector = connection_manager.get_connector(connector_id)
        if not connector:
            raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
        
        await connector.disconnect()
        await connection_manager.remove_connector(connector_id)
        
        return {
            "message": f"Connector {connector_id} deleted successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to delete connector: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/connectors/{connector_id}/schema")
async def get_connector_schema(connector_id: str):
    """
    Get schema information for a specific connector.
    """
    try:
        connector = connection_manager.get_connector(connector_id)
        if not connector:
            raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
        
        # Get schema from connector
        schema = await connector.get_schema()
        
        return {
            "connector_id": connector_id,
            "schema": schema,
            "tables": schema.get("tables", []) if isinstance(schema, dict) else []
        }
        
    except Exception as e:
        logger.error(f"Failed to get schema for connector {connector_id}: {e}")
        return {
            "connector_id": connector_id,
            "schema": {},
            "tables": []
        }


@app.post("/api/v1/connectors/{connector_id}/test")
async def test_connector(connector_id: str):
    """
    Test a connector's connection.
    """
    try:
        connector = connection_manager.get_connector(connector_id)
        if not connector:
            raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
        
        # Test the connection
        start_time = time.time()
        is_connected = await connector.test_connection()
        latency = (time.time() - start_time) * 1000  # Convert to ms
        
        return {
            "success": is_connected,
            "message": "Connection successful" if is_connected else "Connection failed",
            "latency": round(latency, 2),
            "timestamp": datetime.utcnow().isoformat(),
            "connector_id": connector_id
        }
        
    except Exception as e:
        error_msg = str(e) if str(e) else "Unknown error occurred"
        logger.error(f"Failed to test connector {connector_id}: {error_msg}")
        return {
            "success": False,
            "message": error_msg,
            "timestamp": datetime.utcnow().isoformat(),
            "connector_id": connector_id
        }


@app.post("/api/v1/query")
async def execute_federated_query(request: Dict):
    """
    Execute a federated SQL query across multiple data sources.
    
    This is the main endpoint for federated query execution.
    """
    query = request.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
    
    try:
        # Get federation executor from app state
        executor = app.state.federation_executor
        
        # Execute the federated query
        result = executor.execute_federated_query(query)
        
        return {
            "success": True,
            "data": result.data,
            "schema": result.columns,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "metadata": {
                "execution_plan": result.execution_plan,
                "statistics": result.statistics
            }
        }
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/query/explain")
async def explain_query_plan(request: Dict):
    """
    Get the execution plan for a query without executing it.
    """
    query = request.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
    
    try:
        # Parse the query
        parser = QueryParser()
        parsed = parser.parse(query)
        
        # Create execution plan
        planner = QueryPlanner()
        plan = planner.create_plan(parsed)
        
        # Optimize the plan
        optimizer = QueryOptimizer()
        optimized_plan = optimizer.optimize(plan)
        
        return {
            "query": query,
            "plan": optimized_plan.to_dict(),
            "explanation": optimized_plan.explain(),
            "requires_federation": optimized_plan.requires_federation,
            "pushdown_coverage": f"{optimized_plan.pushdown_coverage:.1%}",
            "estimated_cost": optimized_plan.total_estimated_cost,
            "estimated_time_ms": optimized_plan.total_estimated_time_ms
        }
        
    except Exception as e:
        logger.error(f"Query planning failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/query/parse")
async def parse_query(request: Dict):
    """
    Parse a SQL query and return its structure.
    Useful for debugging and understanding query decomposition.
    """
    query = request.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
    
    try:
        parser = QueryParser()
        parsed = parser.parse(query)
        
        return {
            "query": query,
            "query_type": parsed.query_type.value,
            "tables": [str(t) for t in parsed.tables],
            "columns": parsed.columns,
            "where_clause": parsed.where_clause,
            "group_by": parsed.group_by,
            "order_by": parsed.order_by,
            "limit": parsed.limit,
            "joins": [str(j) for j in parsed.joins],
            "cross_source": parsed.cross_source,
            "required_features": [f.value for f in parsed.required_features],
            "pushdown_opportunities": [p.value for p in parsed.pushdown_opportunities]
        }
        
    except Exception as e:
        logger.error(f"Query parsing failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/v1/catalog/sources")
async def list_catalog_sources():
    """
    List all registered data sources in the catalog.
    """
    catalog = app.state.catalog_service
    sources = catalog.list_data_sources()
    
    return {
        "sources": [
            {
                "id": str(source.id),
                "name": source.name,
                "type": source.source_type.value,
                "active": source.active,
                "table_count": len(source.schema.tables) if source.schema else 0,
                "created_at": source.created_at.isoformat(),
                "updated_at": source.updated_at.isoformat()
            }
            for source in sources
        ]
    }


@app.post("/api/v1/catalog/sources")
async def register_catalog_source(request: Dict):
    """
    Register a new data source in the catalog.
    """
    catalog = app.state.catalog_service
    
    try:
        # Create data source
        source = DataSource(
            name=request["name"],
            source_type=SourceType(request.get("type", "postgresql")),
            connection_params=request.get("connection_params", {}),
            metadata=request.get("metadata", {})
        )
        
        # Register in catalog
        source_id = catalog.register_data_source(source)
        
        # Try to discover schema
        schema = await catalog.discover_schema(source_id)
        if schema:
            catalog.update_schema(source_id, schema)
        
        return {
            "id": str(source_id),
            "name": source.name,
            "message": f"Data source '{source.name}' registered successfully",
            "schema_discovered": schema is not None
        }
        
    except Exception as e:
        logger.error(f"Failed to register data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/catalog/sources/{source_id}")
async def unregister_catalog_source(source_id: str):
    """
    Unregister a data source from the catalog.
    """
    catalog = app.state.catalog_service
    
    try:
        from uuid import UUID
        uuid_id = UUID(source_id)
        
        if catalog.unregister_data_source(uuid_id):
            return {"message": f"Data source {source_id} unregistered"}
        else:
            raise HTTPException(status_code=404, detail=f"Data source {source_id} not found")
            
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/catalog/sources/{source_id}/schema")
async def get_source_schema(source_id: str, version: Optional[int] = None):
    """
    Get schema for a data source.
    
    Args:
        source_id: UUID of data source
        version: Optional schema version (latest if not specified)
    """
    catalog = app.state.catalog_service
    
    try:
        from uuid import UUID
        uuid_id = UUID(source_id)
        
        schema = catalog.get_schema_version(uuid_id, version)
        
        if not schema:
            raise HTTPException(status_code=404, detail="Schema not found")
        
        return {
            "source_id": source_id,
            "schema": schema.to_dict()
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/catalog/sources/{source_id}/discover")
async def discover_source_schema(source_id: str):
    """
    Trigger schema discovery for a data source.
    """
    catalog = app.state.catalog_service
    
    try:
        from uuid import UUID
        uuid_id = UUID(source_id)
        
        schema = await catalog.discover_schema(uuid_id)
        
        if not schema:
            raise HTTPException(status_code=500, detail="Schema discovery failed")
        
        # Update in catalog
        catalog.update_schema(uuid_id, schema)
        
        return {
            "source_id": source_id,
            "tables_discovered": len(schema.tables),
            "version": schema.version,
            "schema": schema.to_dict()
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/catalog/sources/{source_id}/changes")
async def get_schema_changes(source_id: str, from_version: int, to_version: int):
    """
    Get schema changes between versions.
    """
    catalog = app.state.catalog_service
    
    try:
        from uuid import UUID
        uuid_id = UUID(source_id)
        
        changes = catalog.get_schema_changes(uuid_id, from_version, to_version)
        
        return {
            "source_id": source_id,
            "from_version": from_version,
            "to_version": to_version,
            "changes": changes
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/catalog/search/tables")
async def search_tables(q: str):
    """
    Search for tables across all data sources.
    
    Args:
        q: Search query
    """
    catalog = app.state.catalog_service
    results = catalog.search_tables(q)
    
    return {
        "query": q,
        "count": len(results),
        "results": results
    }


@app.get("/api/v1/catalog/search/columns")
async def search_columns(q: str, data_type: Optional[str] = None):
    """
    Search for columns across all data sources.
    
    Args:
        q: Search query
        data_type: Optional data type filter
    """
    catalog = app.state.catalog_service
    
    dt = DataType(data_type) if data_type else None
    results = catalog.search_columns(q, dt)
    
    return {
        "query": q,
        "data_type": data_type,
        "count": len(results),
        "results": results
    }


@app.get("/api/v1/catalog/statistics")
async def get_catalog_statistics():
    """
    Get catalog statistics.
    """
    catalog = app.state.catalog_service
    stats = catalog.get_statistics()
    
    return stats


@app.get("/graphql/health")
async def graphql_health():
    """
    GraphQL gateway health check endpoint.
    """
    try:
        if hasattr(app.state, 'graphql_gateway'):
            health_info = app.state.graphql_gateway.get_health_info()
            return health_info
        else:
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable", "error": "GraphQL gateway not initialized"}
            )
    except Exception as e:
        logger.error(f"GraphQL health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


@app.get("/graphql/playground", response_class=HTMLResponse)
async def graphql_playground():
    """
    GraphQL Playground IDE.
    """
    try:
        if hasattr(app.state, 'graphql_gateway'):
            playground_html = app.state.graphql_gateway.get_playground_html("/graphql")
            return HTMLResponse(playground_html)
        else:
            return HTMLResponse(
                "<h1>GraphQL Playground</h1><p>GraphQL gateway not available</p>",
                status_code=503
            )
    except Exception as e:
        logger.error(f"GraphQL playground error: {e}")
        return HTMLResponse(
            f"<h1>GraphQL Playground</h1><p>Error: {str(e)}</p>",
            status_code=500
        )


@app.post("/graphql")
async def graphql_endpoint(request: Request):
    """
    GraphQL query endpoint.
    """
    try:
        if not hasattr(app.state, 'graphql_gateway'):
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        # Get the GraphQL schema and execute query
        schema = app.state.graphql_gateway.get_schema()
        
        # Parse request
        data = await request.json()
        query = data.get("query")
        variables = data.get("variables")
        operation_name = data.get("operationName")
        
        if not query:
            raise HTTPException(status_code=400, detail="Query is required")
        
        # Execute GraphQL query using strawberry
        import strawberry
        result = await schema.execute(
            query,
            variable_values=variables,
            operation_name=operation_name,
            context_value={"user_id": "guest"}  # Simplified auth context
        )
        
        response_data = {"data": result.data}
        if result.errors:
            response_data["errors"] = [{"message": str(error)} for error in result.errors]
        
        return response_data
        
    except Exception as e:
        logger.error(f"GraphQL endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/graphql/ws")
async def graphql_websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for GraphQL subscriptions.
    Implements GraphQL over WebSocket Protocol.
    """
    connection_id = None
    try:
        # Connect to WebSocket manager
        connection_id = await websocket_manager.connect(websocket, user_id="guest")
        
        # Handle WebSocket messages
        while True:
            try:
                # Receive message from client
                message = await websocket.receive_text()
                data = json.loads(message)
                
                message_type = data.get("type")
                message_id = data.get("id")
                payload = data.get("payload", {})
                
                if message_type == "connection_init":
                    # Connection initialization
                    await websocket.send_text(json.dumps({
                        "type": "connection_ack"
                    }))
                
                elif message_type == "start":
                    # Start subscription
                    query = payload.get("query")
                    variables = payload.get("variables")
                    operation_name = payload.get("operationName")
                    
                    if not query:
                        await websocket_manager.send_error(
                            connection_id, message_id, "Query is required"
                        )
                        continue
                    
                    # Add subscription to manager
                    success = await websocket_manager.add_subscription(
                        connection_id, message_id, query, variables, operation_name
                    )
                    
                    if success:
                        # Execute subscription using GraphQL gateway
                        if hasattr(app.state, 'graphql_gateway'):
                            schema = app.state.graphql_gateway.get_schema()
                            
                            # Start subscription execution
                            asyncio.create_task(
                                _handle_subscription_execution(
                                    schema, connection_id, message_id, 
                                    query, variables, operation_name
                                )
                            )
                        else:
                            await websocket_manager.send_error(
                                connection_id, message_id, "GraphQL gateway not available"
                            )
                    else:
                        await websocket_manager.send_error(
                            connection_id, message_id, "Failed to start subscription"
                        )
                
                elif message_type == "stop":
                    # Stop subscription
                    await websocket_manager.remove_subscription(message_id)
                    await websocket_manager.send_complete(connection_id, message_id)
                
                elif message_type == "pong":
                    # Pong response to ping
                    pass
                
                else:
                    logger.warning(f"Unknown WebSocket message type: {message_type}")
                    
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in WebSocket message: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "payload": [{"message": "Invalid JSON"}]
                }))
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error", 
                    "payload": [{"message": str(e)}]
                }))
                
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Cleanup connection
        if connection_id:
            await websocket_manager.disconnect(connection_id)


async def _handle_subscription_execution(
    schema, connection_id: str, subscription_id: str,
    query: str, variables: Optional[Dict] = None, operation_name: Optional[str] = None
):
    """Handle execution of a GraphQL subscription."""
    try:
        # Execute subscription
        result = await schema.subscribe(
            query,
            variable_values=variables,
            operation_name=operation_name,
            context_value={"user_id": "guest", "connection_id": connection_id}
        )
        
        # Stream results
        if hasattr(result, '__aiter__'):
            async for data in result:
                response_data = {"data": data.data}
                if data.errors:
                    response_data["errors"] = [{"message": str(error)} for error in data.errors]
                
                success = await websocket_manager.send_to_subscription(
                    subscription_id, response_data
                )
                if not success:
                    break
        else:
            # Single result (error case)
            response_data = {"data": result.data}
            if result.errors:
                response_data["errors"] = [{"message": str(error)} for error in result.errors]
            
            await websocket_manager.send_to_subscription(subscription_id, response_data)
        
        # Send completion
        await websocket_manager.send_complete(connection_id, subscription_id)
        
    except Exception as e:
        logger.error(f"Subscription execution error: {e}")
        await websocket_manager.send_error(connection_id, subscription_id, str(e))


# =============================================================================
# Machine Learning & Advanced Analytics API Endpoints
# =============================================================================

@app.get("/api/v1/ml/health")
async def ml_health():
    """Get ML framework health status."""
    try:
        ml_framework = app.state.ml_framework
        analytics = app.state.advanced_analytics
        performance_predictor = app.state.performance_predictor
        
        return {
            "status": "healthy",
            "components": {
                "ml_framework": ml_framework.get_health_status(),
                "advanced_analytics": analytics.get_health_status(),
                "performance_predictor": performance_predictor.get_health_status()
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"ML health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ml/analyze")
async def analyze_data(request: Dict):
    """Perform advanced data analysis."""
    try:
        # Extract parameters
        data_source = request.get("data_source")
        target_column = request.get("target_column")
        analysis_type = request.get("analysis_type", "comprehensive")
        
        if not data_source:
            raise HTTPException(status_code=400, detail="data_source is required")
        
        # For demo purposes, generate sample data
        # In production, this would fetch from the specified data source
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        
        # Generate sample time series data
        dates = pd.date_range(
            start=datetime.now() - timedelta(days=30),
            end=datetime.now(),
            freq='H'
        )
        
        sample_data = pd.DataFrame({
            'timestamp': dates,
            'value': np.random.normal(100, 15, len(dates)) + 5 * np.sin(np.arange(len(dates)) * 2 * np.pi / 24),
            'volume': np.random.poisson(1000, len(dates)),
            'category': np.random.choice(['A', 'B', 'C'], len(dates))
        })
        
        # Perform analysis
        analytics = app.state.advanced_analytics
        results = analytics.analyze_data(sample_data, target_column or 'value')
        
        return {
            "success": True,
            "analysis_type": analysis_type,
            "data_source": data_source,
            "target_column": target_column,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Data analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ml/forecast")
async def create_forecast(request: Dict):
    """Create time series forecast."""
    try:
        data_source = request.get("data_source")
        target_column = request.get("target_column", "value")
        periods = request.get("periods", 24)
        confidence_level = request.get("confidence_level", 0.95)
        
        if not data_source:
            raise HTTPException(status_code=400, detail="data_source is required")
        
        # Generate sample historical data for demo
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        
        dates = pd.date_range(
            start=datetime.now() - timedelta(days=7),
            end=datetime.now(),
            freq='H'
        )
        
        historical_data = pd.Series(
            np.random.normal(100, 10, len(dates)) + 3 * np.sin(np.arange(len(dates)) * 2 * np.pi / 24),
            index=dates
        )
        
        # Create forecast
        analytics = app.state.advanced_analytics
        forecast = analytics.time_series_analyzer.forecast(
            historical_data,
            periods=periods,
            confidence_level=confidence_level
        )
        
        return {
            "success": True,
            "data_source": data_source,
            "target_column": target_column,
            "periods": periods,
            "confidence_level": confidence_level,
            "forecast": {
                "timestamps": [str(ts) for ts in forecast.timestamps],
                "values": forecast.values,
                "confidence_lower": forecast.confidence_lower,
                "confidence_upper": forecast.confidence_upper,
                "model_info": forecast.model_info
            }
        }
        
    except Exception as e:
        logger.error(f"Forecasting failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ml/anomalies")
async def detect_anomalies(request: Dict):
    """Detect anomalies in data."""
    try:
        data_source = request.get("data_source")
        target_column = request.get("target_column", "value")
        method = request.get("method", "zscore")
        threshold = request.get("threshold", 3.0)
        
        if not data_source:
            raise HTTPException(status_code=400, detail="data_source is required")
        
        # Generate sample data with anomalies for demo
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        
        dates = pd.date_range(
            start=datetime.now() - timedelta(days=7),
            end=datetime.now(),
            freq='H'
        )
        
        # Normal data with some injected anomalies
        normal_data = np.random.normal(100, 10, len(dates))
        # Inject some anomalies
        anomaly_indices = np.random.choice(len(dates), size=5, replace=False)
        normal_data[anomaly_indices] += np.random.choice([-50, 50], size=5)
        
        data_series = pd.Series(normal_data, index=dates)
        
        # Detect anomalies
        analytics = app.state.advanced_analytics
        anomalies = analytics.anomaly_detector.detect_point_anomalies(
            data_series,
            method=method,
            threshold=threshold
        )
        
        return {
            "success": True,
            "data_source": data_source,
            "target_column": target_column,
            "method": method,
            "threshold": threshold,
            "anomalies_detected": len(anomalies),
            "anomalies": [
                {
                    "timestamp": str(anomaly.timestamp),
                    "value": anomaly.value,
                    "anomaly_score": anomaly.anomaly_score,
                    "type": anomaly.anomaly_type.value,
                    "confidence": anomaly.confidence,
                    "context": anomaly.context
                }
                for anomaly in anomalies
            ]
        }
        
    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ml/optimize")
async def optimize_query(request: Dict):
    """Optimize query performance using ML."""
    try:
        query = request.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="query is required")
        
        # Get performance predictor
        performance_predictor = app.state.performance_predictor
        
        # Analyze and optimize query
        optimization_results = performance_predictor.query_optimizer.optimize_query(query)
        
        return {
            "success": True,
            "query": query,
            "optimization": optimization_results
        }
        
    except Exception as e:
        logger.error(f"Query optimization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/ml/models")
async def list_models():
    """List all registered ML models."""
    try:
        ml_framework = app.state.ml_framework
        registry = ml_framework.get_registry()
        
        models = registry.list_models()
        
        return {
            "success": True,
            "total_models": len(models),
            "models": [model.to_dict() for model in models]
        }
        
    except Exception as e:
        logger.error(f"Failed to list models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/ml/performance/insights")
async def get_performance_insights():
    """Get comprehensive performance insights."""
    try:
        performance_predictor = app.state.performance_predictor
        insights = performance_predictor.get_performance_insights()
        
        return {
            "success": True,
            "insights": insights
        }
        
    except Exception as e:
        logger.error(f"Failed to get performance insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ml/performance/predict")
async def predict_system_load(request: Dict):
    """Predict system load for future time period."""
    try:
        time_horizon_hours = request.get("time_horizon_hours", 24)
        
        performance_predictor = app.state.performance_predictor
        prediction = performance_predictor.predict_system_load(time_horizon_hours)
        
        return {
            "success": True,
            "prediction": prediction
        }
        
    except Exception as e:
        logger.error(f"Load prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/graphql")
async def graphql_playground():
    """
    GraphQL Playground IDE for GET requests.
    """
    try:
        if hasattr(app.state, 'graphql_gateway'):
            playground_html = app.state.graphql_gateway.get_playground_html("/graphql")
            return HTMLResponse(playground_html)
        else:
            return HTMLResponse(
                "<h1>GraphQL Playground</h1><p>GraphQL gateway not available</p>",
                status_code=503
            )
    except Exception as e:
        logger.error(f"GraphQL playground error: {e}")
        return HTMLResponse(
            f"<h1>GraphQL Playground</h1><p>Error: {str(e)}</p>",
            status_code=500
        )


# =============================================================================
# KAFKA STREAMING ENDPOINTS - THE DATA STREAMING HERO!
# =============================================================================

@app.get("/api/v1/kafka/health")
async def kafka_health():
    """Get Kafka streaming engine health status"""
    try:
        if not hasattr(app.state, 'kafka_engine'):
            return {"status": "unavailable", "error": "Kafka engine not initialized"}
        
        streams = await app.state.kafka_engine.list_streams()
        
        return {
            "status": "healthy",
            "kafka_engine": "operational",
            "bootstrap_servers": app.state.kafka_engine.bootstrap_servers,
            "active_streams": len(streams),
            "streams": streams,
            "hero_status": "🚀 KAFKA STREAMING HERO READY!"
        }
        
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/api/v1/kafka/streams/create")
async def create_kafka_stream(request: Dict):
    """Create a new Kafka stream"""
    try:
        stream_name = request.get("stream_name")
        partitions = request.get("partitions", 3)
        retention_hours = request.get("retention_hours", 168)
        
        if not stream_name:
            raise HTTPException(status_code=400, detail="stream_name is required")
        
        config = StreamConfig(
            topic_name=stream_name,
            partition_count=partitions,
            retention_hours=retention_hours
        )
        
        success = await app.state.kafka_engine.create_stream(config)
        
        return {
            "success": success,
            "stream_name": stream_name,
            "partitions": partitions,
            "retention_hours": retention_hours,
            "message": f"🚀 Kafka stream '{stream_name}' created successfully!"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/kafka/produce")
async def produce_to_kafka(request: Dict):
    """Produce messages to a Kafka stream"""
    try:
        stream_name = request.get("stream_name")
        messages = request.get("messages", [])
        
        if not stream_name or not messages:
            raise HTTPException(status_code=400, detail="stream_name and messages are required")
        
        producer = await app.state.kafka_engine.get_producer(stream_name)
        
        # Convert messages to StreamMessage objects
        from laykhaus.streaming.kafka_engine import StreamMessage
        stream_messages = []
        for msg in messages:
            stream_msg = StreamMessage(
                key=msg.get("key", "default"),
                value=msg.get("value"),
                headers=msg.get("headers", {})
            )
            stream_messages.append(stream_msg)
        
        # Send batch of messages
        result = await producer.send_batch(stream_messages)
        
        return {
            "stream_name": stream_name,
            "total_messages": len(messages),
            "sent_count": result.get("sent_count", 0),
            "failed_count": result.get("failed_count", 0),
            "success_rate": result.get("success_rate", 0),
            "hero_message": f"🚀 Kafka processed {result.get('sent_count', 0)} messages at high speed!"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/kafka/streams/{stream_name}/metrics")
async def get_kafka_stream_metrics(stream_name: str):
    """Get comprehensive metrics for a specific Kafka stream"""
    try:
        metrics = await app.state.kafka_engine.get_stream_metrics(stream_name)
        
        return {
            "stream_name": stream_name,
            "metrics": metrics,
            "kafka_power": "🚀 Real-time streaming at massive scale!"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# SPARK ANALYTICS ENDPOINTS - THE BIG DATA HERO!
# =============================================================================

@app.get("/api/v1/spark/health")
async def spark_health():
    """Get Spark analytics engine health status"""
    try:
        if not hasattr(app.state, 'spark_engine'):
            return {"status": "unavailable", "error": "Spark engine not initialized"}
        
        metrics = app.state.spark_engine.get_engine_metrics()
        
        return {
            "status": "healthy",
            "spark_engine": "operational",
            "metrics": metrics,
            "hero_status": "⚡ SPARK BIG DATA ANALYTICS HERO READY!"
        }
        
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/api/v1/spark/jobs/sql")
async def submit_spark_sql_job(request: Dict):
    """Submit a Spark SQL analytics job"""
    try:
        sql_query = request.get("sql_query")
        job_name = request.get("job_name", "LaykHaus SQL Analytics")
        
        if not sql_query:
            raise HTTPException(status_code=400, detail="sql_query is required")
        
        # Create Spark job
        job = SparkJob(
            id=f"sql_job_{int(time.time())}",
            name=job_name,
            job_type=SparkJobType.BATCH,
            sql_query=sql_query
        )
        
        # Submit job
        job_id = await app.state.spark_engine.submit_job(job)
        
        # Get result
        result_job = app.state.spark_engine.get_job_status(job_id)
        
        return {
            "job_id": job_id,
            "job_name": job_name,
            "status": result_job.status.value if result_job else "unknown",
            "result": result_job.result if result_job else None,
            "hero_message": f"⚡ Spark processed your SQL with big data power!"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/spark/jobs/etl")
async def submit_spark_etl_job(request: Dict):
    """Submit a Spark ETL pipeline job"""
    try:
        input_sources = request.get("input_sources", [])
        output_destination = request.get("output_destination")
        transformation_sql = request.get("transformation_sql")
        job_name = request.get("job_name", "LaykHaus ETL Pipeline")
        
        if not input_sources:
            raise HTTPException(status_code=400, detail="input_sources are required")
        
        # Create ETL job
        job = SparkJob(
            id=f"etl_job_{int(time.time())}",
            name=job_name,
            job_type=SparkJobType.ETL,
            sql_query=transformation_sql,
            input_sources=input_sources,
            output_destination=output_destination
        )
        
        # Submit job
        job_id = await app.state.spark_engine.submit_job(job)
        
        # Get result
        result_job = app.state.spark_engine.get_job_status(job_id)
        
        return {
            "job_id": job_id,
            "job_name": job_name,
            "status": result_job.status.value if result_job else "unknown",
            "input_sources": input_sources,
            "output_destination": output_destination,
            "result": result_job.result if result_job else None,
            "hero_message": f"⚡ Spark ETL pipeline processing massive datasets!"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/spark/jobs/ml")
async def submit_spark_ml_job(request: Dict):
    """Submit a Spark ML pipeline job"""
    try:
        job_name = request.get("job_name", "LaykHaus ML Pipeline")
        model_type = request.get("model_type", "classification")
        
        # Create ML job
        job = SparkJob(
            id=f"ml_job_{int(time.time())}",
            name=job_name,
            job_type=SparkJobType.ML_PIPELINE,
            config={"model_type": model_type}
        )
        
        # Submit job
        job_id = await app.state.spark_engine.submit_job(job)
        
        # Get result
        result_job = app.state.spark_engine.get_job_status(job_id)
        
        return {
            "job_id": job_id,
            "job_name": job_name,
            "model_type": model_type,
            "status": result_job.status.value if result_job else "unknown",
            "result": result_job.result if result_job else None,
            "hero_message": f"⚡ Spark ML processing with distributed machine learning power!"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/spark/jobs")
async def list_spark_jobs():
    """List all Spark jobs (active and completed)"""
    try:
        active_jobs = app.state.spark_engine.list_active_jobs()
        completed_jobs = app.state.spark_engine.list_completed_jobs(limit=20)
        
        return {
            "active_jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "type": job.job_type.value,
                    "status": job.status.value,
                    "started_at": job.started_at.isoformat() if job.started_at else None
                }
                for job in active_jobs
            ],
            "completed_jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "type": job.job_type.value,
                    "status": job.status.value,
                    "duration_seconds": (job.completed_at - job.started_at).total_seconds() if job.completed_at and job.started_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None
                }
                for job in completed_jobs
            ],
            "summary": {
                "active_count": len(active_jobs),
                "completed_count": len(completed_jobs),
                "hero_status": "⚡ Spark managing distributed computing workloads!"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/streaming/hero-status")
async def get_hero_status():
    """Get the hero status of Kafka and Spark"""
    try:
        kafka_health = "healthy" if hasattr(app.state, 'kafka_engine') else "unavailable"
        spark_health = "healthy" if hasattr(app.state, 'spark_engine') else "unavailable"
        
        kafka_streams = []
        spark_jobs = []
        
        if kafka_health == "healthy":
            kafka_streams = await app.state.kafka_engine.list_streams()
        
        if spark_health == "healthy":
            active_jobs = app.state.spark_engine.list_active_jobs()
            spark_jobs = [job.name for job in active_jobs]
        
        return {
            "heroes_status": "🚀⚡ KAFKA & SPARK HEROES READY FOR ACTION! ⚡🚀",
            "kafka": {
                "status": kafka_health,
                "hero_power": "🚀 Real-time Data Streaming at Internet Scale",
                "active_streams": len(kafka_streams),
                "stream_names": [s.get("name", "") for s in kafka_streams]
            },
            "spark": {
                "status": spark_health,  
                "hero_power": "⚡ Massive Distributed Analytics & ML Processing",
                "active_jobs": len(spark_jobs),
                "job_names": spark_jobs
            },
            "combined_power": {
                "real_time_analytics": "Kafka → Spark streaming pipelines",
                "big_data_processing": "Petabyte-scale data processing capability",
                "ml_at_scale": "Distributed machine learning across clusters",
                "streaming_etl": "Real-time ETL with sub-second latency"
            }
        }
        
    except Exception as e:
        return {"error": str(e), "status": "heroes need assistance"}

if __name__ == "__main__":
    import uvicorn
    import time
    
    uvicorn.run(
        "laykhaus.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.is_development,
        log_level=settings.log_level.lower(),
    )