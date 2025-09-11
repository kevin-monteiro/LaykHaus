"""
Connector management API endpoints.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.connectors.base import ConnectionConfig, ConnectorType
from laykhaus.core.config import settings
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/connectors", tags=["connectors"])


class ConnectorCreateRequest(BaseModel):
    """Request model for creating a connector."""
    name: str
    type: str
    config: Dict[str, Any]


class ConnectorUpdateRequest(BaseModel):
    """Request model for updating a connector."""
    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


@router.get("")
async def list_connectors():
    """List all configured connectors with their status."""
    connectors = []
    
    for connector_id in connection_manager.list_connectors():
        connector = connection_manager.get_connector(connector_id)
        if connector:
            health = await connector.health_check()
            capabilities = connector.get_capabilities()
            # Map health status to frontend status values
            if connector.is_connected and health.healthy:
                status = "active"
            elif connector.is_connected and not health.healthy:
                status = "error"
            else:
                status = "inactive"
                
            connectors.append({
                "id": connector_id,
                "name": connector.connector_id,
                "type": capabilities.connector_type.value,
                "connected": connector.is_connected,
                "status": status,
                "config": {
                    "connection": {
                        "host": connector.config.host,
                        "port": connector.config.port,
                        "database": getattr(connector.config, 'database', None),
                        "username": getattr(connector.config, 'username', None),
                        "schema": getattr(connector.config, 'schema', None),
                    },
                    "authentication": {}
                },
                "metadata": {
                    "createdAt": datetime.utcnow().isoformat(),
                    "updatedAt": datetime.utcnow().isoformat()
                },
                "sql_features": [f.value for f in capabilities.sql_features],
                "pushdown_capabilities": [p.value for p in capabilities.pushdown_capabilities],
                "supports_streaming": capabilities.supports_streaming
            })
    
    return {"connectors": connectors}


@router.get("/stats")
async def get_connector_stats():
    """Get statistics for all connectors."""
    stats = await connection_manager.get_statistics()
    return stats


@router.get("/{connector_id}")
async def get_connector(connector_id: str):
    """Get details of a specific connector."""
    connector = connection_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    health = await connector.health_check()
    capabilities = connector.get_capabilities()
    
    return {
        "id": connector_id,
        "name": connector.connector_id,
        "type": capabilities.connector_type.value,
        "connected": connector.is_connected,
        "status": "healthy" if health.healthy else "unhealthy",
        "config": {
            "host": connector.config.host,
            "port": connector.config.port,
            "database": getattr(connector.config, 'database', None),
            "username": getattr(connector.config, 'username', None),
            "ssl_enabled": connector.config.ssl_enabled,
            "connection_timeout": connector.config.connection_timeout,
            "query_timeout": connector.config.query_timeout,
            "pool_size": connector.config.pool_size,
        },
        "health": {
            "status": "healthy" if health.healthy else "unhealthy",
            "message": health.message,
            "details": health.metadata
        }
    }


@router.post("")
async def create_connector(request: ConnectorCreateRequest):
    """Create a new connector."""
    try:
        # Determine connector type
        connector_type = ConnectorType(request.type)
        
        # Create configuration based on type
        # Handle nested config structure
        conn_config = request.config.get("connection", request.config)
        
        if connector_type == ConnectorType.POSTGRESQL:
            config = ConnectionConfig(
                host=conn_config.get("host", settings.default_postgres_host),
                port=conn_config.get("port", 5432),
                database=conn_config.get("database", "postgres"),
                username=conn_config.get("username", "postgres"),
                password=conn_config.get("password", "")
            )
        elif connector_type == ConnectorType.KAFKA:
            brokers = conn_config.get("brokers", settings.kafka_bootstrap_servers)
            if ":" in brokers:
                host, port = brokers.split(":")
            else:
                host, port = brokers, "9092"
            config = ConnectionConfig(
                host=host,
                port=int(port),
                extra_params={
                    "topics": conn_config.get("topics", []),
                    "consumer_group": conn_config.get("group_id", settings.kafka_consumer_group),
                }
            )
        elif connector_type == ConnectorType.REST_API:
            config = ConnectionConfig(
                host=conn_config.get("host", settings.default_rest_api_host),
                port=conn_config.get("port", 8080),
                extra_params={
                    "base_path": conn_config.get("base_path", "/api"),
                    "auth_type": conn_config.get("auth_type", "none"),
                }
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported connector type: {request.type}")
        
        # Generate connector ID
        import uuid
        type_prefix = {
            ConnectorType.POSTGRESQL: "pgsql",
            ConnectorType.KAFKA: "kafka", 
            ConnectorType.REST_API: "rest"
        }.get(connector_type, "unknown")
        connector_id = f"{type_prefix}_{uuid.uuid4().hex[:16]}"
        
        # Create connector instance
        from laykhaus.connectors.factory import ConnectorFactory
        connector = ConnectorFactory.create_connector(connector_id, connector_type, config)
        
        if not connector:
            raise HTTPException(status_code=500, detail="Failed to create connector")
        
        # Connect and register
        await connector.connect()
        await connection_manager.add_connector(connector_id, connector)
        
        return {
            "id": connector_id,
            "message": f"Connector {request.name} created successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create connector: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{connector_id}")
async def update_connector(connector_id: str, request: ConnectorUpdateRequest):
    """Update an existing connector."""
    connector = connection_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    try:
        # Update configuration
        if request.name:
            # Name updates not supported directly on config
            pass
        
        if request.config:
            # Update specific config fields based on connector type
            for key, value in request.config.items():
                if hasattr(connector.config, key):
                    setattr(connector.config, key, value)
        
        # Reconnect with new config
        await connector.disconnect()
        await connector.connect()
        
        return {
            "id": connector_id,
            "message": f"Connector {connector_id} updated successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to update connector {connector_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{connector_id}")
async def delete_connector(connector_id: str):
    """Delete a connector."""
    if connector_id not in connection_manager.list_connectors():
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    await connection_manager.remove_connector(connector_id)
    return {"message": f"Connector {connector_id} deleted successfully"}


@router.get("/{connector_id}/schema")
async def get_connector_schema(connector_id: str):
    """Get schema information for a connector."""
    connector = connection_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    try:
        schema = await connector.get_schema()
        return {"connector_id": connector_id, "schema": schema}
    except Exception as e:
        logger.error(f"Failed to get schema for {connector_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{connector_id}/test")
async def test_connector(connector_id: str):
    """Test connector connection."""
    connector = connection_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector {connector_id} not found")
    
    try:
        is_connected = await connector.test_connection()
        health = await connector.health_check()
        
        return {
            "connected": is_connected,
            "health": {
                "status": "healthy" if health.healthy else "unhealthy",
                "message": health.message,
                "details": health.metadata
            }
        }
    except Exception as e:
        logger.error(f"Failed to test connector {connector_id}: {e}")
        return {
            "connected": False,
            "error": str(e)
        }