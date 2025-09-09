"""
Connection pool management for all connectors.
Handles connection lifecycle, health monitoring, and failover.
"""

import asyncio
import json
import os
from typing import Dict, List, Optional
from pathlib import Path

from laykhaus.connectors.base import BaseConnector, ConnectionConfig, HealthStatus
from laykhaus.connectors.kafka_connector import KafkaConnector
from laykhaus.connectors.postgres_connector import PostgreSQLConnector
from laykhaus.core.config import settings
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """
    Manages connections to all data sources.
    Provides connection pooling, health monitoring, and automatic recovery.
    """
    
    def __init__(self):
        """Initialize connection manager."""
        self.logger = get_logger(__name__)
        self.connectors: Dict[str, BaseConnector] = {}
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_check_interval = 30  # seconds
        # Persistence file path
        self._persistence_dir = Path("/tmp/laykhaus")
        self._connectors_file = self._persistence_dir / "connectors.json"
    
    async def initialize(self) -> None:
        """Initialize all configured connectors."""
        self.logger.info("Initializing connection manager")
        
        # Load saved connectors from persistence
        await self._load_connectors()
        
        # Don't auto-create connectors - let users create them via API
        # await self._init_postgres_connector()  # Disabled auto-creation
        # await self._init_kafka_connector()     # Disabled auto-creation
        
        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._monitor_health())
        
        self.logger.info(f"Initialized {len(self.connectors)} connectors")
    
    async def _init_postgres_connector(self) -> None:
        """Initialize PostgreSQL connector."""
        try:
            # Use simplified configuration for Phase 1
            config = ConnectionConfig(
                host="postgres",  # Docker service name
                port=5432,
                database="laykhaus_demo",
                username="laykhaus_user",
                password="secure_password",
                pool_size=10,
                query_timeout=30,
            )
            
            connector = PostgreSQLConnector("postgres_main", config)
            await connector.connect()
            
            self.connectors["postgres_main"] = connector
            self.logger.info("PostgreSQL connector initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL connector: {e}")
            # Don't fail completely, continue without this connector
    
    async def _init_kafka_connector(self) -> None:
        """Initialize Kafka connector."""
        try:
            # Skip Kafka for Phase 1 if not available
            self.logger.info("Skipping Kafka connector for Phase 1 (optional)")
            return
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka connector: {e}")
            # Don't fail completely, continue without this connector
    
    async def shutdown(self) -> None:
        """Shutdown all connections."""
        self.logger.info("Shutting down connection manager")
        
        # Stop health monitoring
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        # Disconnect all connectors
        for name, connector in self.connectors.items():
            try:
                await connector.disconnect()
                self.logger.info(f"Disconnected {name}")
            except Exception as e:
                self.logger.error(f"Error disconnecting {name}: {e}")
        
        self.connectors.clear()
    
    def get_connector(self, connector_id: str) -> Optional[BaseConnector]:
        """
        Get a connector by ID.
        
        Args:
            connector_id: Connector identifier
        
        Returns:
            Connector instance or None
        """
        return self.connectors.get(connector_id)
    
    def list_connectors(self) -> List[str]:
        """
        List all available connector IDs.
        
        Returns:
            List of connector IDs
        """
        return list(self.connectors.keys())
    
    async def add_connector(
        self, 
        connector_id: str, 
        connector: BaseConnector
    ) -> None:
        """
        Add a new connector to the manager.
        
        Args:
            connector_id: Unique connector identifier
            connector: Connector instance
        """
        if connector_id in self.connectors:
            raise ValueError(f"Connector {connector_id} already exists")
        
        if not connector.is_connected:
            await connector.connect()
        
        self.connectors[connector_id] = connector
        self.logger.info(f"Added connector {connector_id}")
        
        # Save connectors to persist across restarts
        await self._save_connectors()
    
    async def remove_connector(self, connector_id: str) -> None:
        """
        Remove and disconnect a connector.
        
        Args:
            connector_id: Connector to remove
        """
        if connector_id in self.connectors:
            connector = self.connectors[connector_id]
            await connector.disconnect()
            del self.connectors[connector_id]
            self.logger.info(f"Removed connector {connector_id}")
            
            # Save connectors to persist across restarts
            await self._save_connectors()
    
    async def get_health_status(self) -> Dict[str, HealthStatus]:
        """
        Get health status of all connectors.
        
        Returns:
            Dictionary of connector health statuses
        """
        health_statuses = {}
        
        for name, connector in self.connectors.items():
            try:
                health = await connector.health_check()
                health_statuses[name] = health
            except Exception as e:
                health_statuses[name] = HealthStatus(
                    healthy=False,
                    latency_ms=0,
                    message="Health check failed",
                    last_check=asyncio.get_event_loop().time(),
                    error=str(e)
                )
        
        return health_statuses
    
    async def _monitor_health(self) -> None:
        """Background task to monitor connector health."""
        while True:
            try:
                await asyncio.sleep(self._health_check_interval)
                
                health_statuses = await self.get_health_status()
                
                for name, status in health_statuses.items():
                    if not status.healthy:
                        self.logger.warning(
                            f"Connector {name} is unhealthy: {status.error}"
                        )
                        
                        # Attempt to reconnect
                        connector = self.connectors.get(name)
                        if connector and not connector.is_connected:
                            try:
                                await connector.connect()
                                self.logger.info(f"Reconnected {name}")
                            except Exception as e:
                                self.logger.error(
                                    f"Failed to reconnect {name}: {e}"
                                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Health monitoring error: {e}")
    
    def get_statistics(self) -> Dict[str, any]:
        """
        Get statistics about all managed connections.
        
        Returns:
            Connection statistics
        """
        stats = {
            "total_connectors": len(self.connectors),
            "connected": sum(1 for c in self.connectors.values() if c.is_connected),
            "connectors": {}
        }
        
        for name, connector in self.connectors.items():
            stats["connectors"][name] = {
                "type": connector.__class__.__name__,
                "connected": connector.is_connected,
                "capabilities": len(connector.get_capabilities().sql_features),
            }
        
        return stats
    
    async def _save_connectors(self) -> None:
        """Save connector configurations to disk for persistence."""
        try:
            # Create directory if it doesn't exist
            self._persistence_dir.mkdir(parents=True, exist_ok=True)
            
            # Prepare connector data for serialization
            connectors_data = {}
            for connector_id, connector in self.connectors.items():
                # Get connector type
                connector_type = "unknown"
                if isinstance(connector, PostgreSQLConnector):
                    connector_type = "postgresql"
                elif isinstance(connector, KafkaConnector):
                    connector_type = "kafka"
                elif hasattr(connector, '__class__'):
                    # For REST API and other connectors
                    class_name = connector.__class__.__name__
                    if "REST" in class_name.upper():
                        connector_type = "rest_api"
                
                # Save connector configuration
                connectors_data[connector_id] = {
                    "id": connector_id,
                    "type": connector_type,
                    "config": {
                        "host": getattr(connector.config, 'host', None),
                        "port": getattr(connector.config, 'port', None),
                        "database": getattr(connector.config, 'database', None),
                        "username": getattr(connector.config, 'username', None),
                        "password": getattr(connector.config, 'password', None),
                        "extra_params": getattr(connector.config, 'extra_params', {}),
                    }
                }
            
            # Write to file
            with open(self._connectors_file, 'w') as f:
                json.dump(connectors_data, f, indent=2)
            
            self.logger.info(f"Saved {len(connectors_data)} connectors to {self._connectors_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save connectors: {e}")
    
    async def _load_connectors(self) -> None:
        """Load saved connector configurations from disk."""
        try:
            if not self._connectors_file.exists():
                self.logger.info("No saved connectors found")
                return
            
            with open(self._connectors_file, 'r') as f:
                connectors_data = json.load(f)
            
            self.logger.info(f"Loading {len(connectors_data)} saved connectors")
            
            for connector_id, connector_info in connectors_data.items():
                try:
                    connector_type = connector_info.get('type')
                    config_data = connector_info.get('config', {})
                    
                    if connector_type == 'postgresql':
                        config = ConnectionConfig(
                            host=config_data.get('host', 'localhost'),
                            port=config_data.get('port', 5432),
                            database=config_data.get('database'),
                            username=config_data.get('username'),
                            password=config_data.get('password'),
                            extra_params=config_data.get('extra_params', {})
                        )
                        connector = PostgreSQLConnector(connector_id, config)
                        await connector.connect()
                        self.connectors[connector_id] = connector
                        self.logger.info(f"Loaded PostgreSQL connector: {connector_id}")
                        
                    elif connector_type == 'kafka':
                        config = ConnectionConfig(
                            host=config_data.get('host', 'localhost'),
                            port=config_data.get('port', 9092),
                            extra_params=config_data.get('extra_params', {})
                        )
                        connector = KafkaConnector(connector_id, config)
                        await connector.connect()
                        self.connectors[connector_id] = connector
                        self.logger.info(f"Loaded Kafka connector: {connector_id}")
                        
                    elif connector_type == 'rest_api':
                        from laykhaus.connectors.rest_api_connector import RESTAPIConnector
                        config = ConnectionConfig(
                            host=config_data.get('host', 'localhost'),
                            port=config_data.get('port', 80),
                            extra_params=config_data.get('extra_params', {})
                        )
                        connector = RESTAPIConnector(connector_id, config)
                        await connector.connect()
                        self.connectors[connector_id] = connector
                        self.logger.info(f"Loaded REST API connector: {connector_id}")
                        
                except Exception as e:
                    self.logger.error(f"Failed to load connector {connector_id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Failed to load connectors from disk: {e}")


# Global connection manager instance
connection_manager = ConnectionManager()