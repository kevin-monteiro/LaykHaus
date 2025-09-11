"""
Connector factory for creating data source connectors.

This factory abstracts the creation of connectors and allows for easy
extension with new connector types.
"""

from typing import Optional
from laykhaus.connectors.base import BaseConnector, ConnectionConfig, ConnectorType
from laykhaus.integrations.kafka import KafkaConnector
from laykhaus.integrations.postgres import PostgreSQLConnector
from laykhaus.integrations.rest import RESTAPIConnector


class ConnectorFactory:
    """Factory for creating connector instances."""
    
    @staticmethod
    def create_connector(
        connector_id: str,
        connector_type: ConnectorType,
        config: ConnectionConfig
    ) -> Optional[BaseConnector]:
        """
        Create a connector instance based on type.
        
        Args:
            connector_id: Unique identifier for the connector
            connector_type: Type of connector to create
            config: Configuration for the connector
            
        Returns:
            Connector instance or None if type not supported
        """
        connector_map = {
            ConnectorType.POSTGRESQL: PostgreSQLConnector,
            ConnectorType.KAFKA: KafkaConnector,
            ConnectorType.REST_API: RESTAPIConnector,
        }
        
        connector_class = connector_map.get(connector_type)
        if connector_class:
            return connector_class(connector_id, config)
        
        return None
    
    @staticmethod
    def get_supported_types() -> list[ConnectorType]:
        """Get list of supported connector types."""
        return [
            ConnectorType.POSTGRESQL,
            ConnectorType.KAFKA,
            ConnectorType.REST_API,
        ]