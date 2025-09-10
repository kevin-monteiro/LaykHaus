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

from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.config import settings
from laykhaus.core.logging import get_logger
from laykhaus.federation.spark_federated_executor import SparkFederatedExecutor

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
            
            # Build extra_params including any schema provided
            extra_params = {
                "base_url": base_url,
                "auth_type": config.get("auth_type", "none"),
                "auth_config": config.get("auth_config", {})
            }
            
            # Include any extra_params from the config (like schema)
            if "extra_params" in config:
                extra_params.update(config["extra_params"])
            
            conn_config = ConnectionConfig(
                host=host,
                port=port,
                extra_params=extra_params
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











if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "laykhaus.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.is_development,
        log_level=settings.log_level.lower(),
    )
