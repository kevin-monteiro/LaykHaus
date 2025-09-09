"""
WebSocket Connection Manager for GraphQL Subscriptions.

Handles WebSocket connections, subscription lifecycle, and real-time data updates.
"""

import asyncio
import json
import uuid
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

from fastapi import WebSocket, WebSocketDisconnect
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SubscriptionInfo:
    """Information about an active subscription."""
    subscription_id: str
    user_id: str
    query: str
    variables: Dict[str, Any]
    operation_name: Optional[str]
    created_at: datetime = field(default_factory=datetime.now)


@dataclass 
class ConnectionInfo:
    """Information about a WebSocket connection."""
    connection_id: str
    websocket: WebSocket
    user_id: str
    subscriptions: Set[str] = field(default_factory=set)
    connected_at: datetime = field(default_factory=datetime.now)
    last_ping: datetime = field(default_factory=datetime.now)


class WebSocketConnectionManager:
    """
    Manages WebSocket connections for GraphQL subscriptions.
    
    Handles connection lifecycle, subscription management, and message routing.
    """
    
    def __init__(self):
        """Initialize connection manager."""
        self.connections: Dict[str, ConnectionInfo] = {}
        self.subscriptions: Dict[str, SubscriptionInfo] = {}
        self.subscription_to_connection: Dict[str, str] = {}
        self.topic_subscriptions: Dict[str, Set[str]] = {}  # topic -> subscription_ids
        self.logger = get_logger(__name__)
        
        # Start background tasks
        self._cleanup_task = None
        self._heartbeat_task = None
    
    async def connect(self, websocket: WebSocket, user_id: str = "guest") -> str:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        
        connection_id = str(uuid.uuid4())
        connection_info = ConnectionInfo(
            connection_id=connection_id,
            websocket=websocket,
            user_id=user_id
        )
        
        self.connections[connection_id] = connection_info
        
        self.logger.info(f"WebSocket connection established: {connection_id}")
        
        # Start background tasks if this is the first connection
        if len(self.connections) == 1:
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        return connection_id
    
    async def disconnect(self, connection_id: str):
        """Handle WebSocket disconnection."""
        if connection_id not in self.connections:
            return
        
        connection_info = self.connections[connection_id]
        
        # Clean up all subscriptions for this connection
        for subscription_id in list(connection_info.subscriptions):
            await self._cleanup_subscription(subscription_id)
        
        # Remove connection
        del self.connections[connection_id]
        
        self.logger.info(f"WebSocket connection closed: {connection_id}")
        
        # Stop background tasks if no more connections
        if len(self.connections) == 0:
            if self._cleanup_task:
                self._cleanup_task.cancel()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
    
    async def add_subscription(
        self,
        connection_id: str,
        subscription_id: str,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None
    ) -> bool:
        """Add a new subscription to a connection."""
        if connection_id not in self.connections:
            return False
        
        connection_info = self.connections[connection_id]
        
        subscription_info = SubscriptionInfo(
            subscription_id=subscription_id,
            user_id=connection_info.user_id,
            query=query,
            variables=variables or {},
            operation_name=operation_name
        )
        
        self.subscriptions[subscription_id] = subscription_info
        self.subscription_to_connection[subscription_id] = connection_id
        connection_info.subscriptions.add(subscription_id)
        
        # Extract topic from subscription for routing
        topic = self._extract_topic_from_query(query, variables or {})
        if topic:
            if topic not in self.topic_subscriptions:
                self.topic_subscriptions[topic] = set()
            self.topic_subscriptions[topic].add(subscription_id)
        
        self.logger.info(f"Subscription added: {subscription_id} for connection {connection_id}")
        return True
    
    async def remove_subscription(self, subscription_id: str) -> bool:
        """Remove a subscription."""
        return await self._cleanup_subscription(subscription_id)
    
    async def send_to_subscription(self, subscription_id: str, data: Dict[str, Any]):
        """Send data to a specific subscription."""
        if subscription_id not in self.subscription_to_connection:
            return False
        
        connection_id = self.subscription_to_connection[subscription_id]
        if connection_id not in self.connections:
            await self._cleanup_subscription(subscription_id)
            return False
        
        connection_info = self.connections[connection_id]
        
        message = {
            "id": subscription_id,
            "type": "next",
            "payload": data
        }
        
        try:
            await connection_info.websocket.send_text(json.dumps(message))
            return True
        except Exception as e:
            self.logger.error(f"Failed to send to subscription {subscription_id}: {e}")
            await self.disconnect(connection_id)
            return False
    
    async def broadcast_to_topic(self, topic: str, data: Dict[str, Any]):
        """Broadcast data to all subscriptions listening to a topic."""
        if topic not in self.topic_subscriptions:
            return 0
        
        sent_count = 0
        for subscription_id in list(self.topic_subscriptions[topic]):
            if await self.send_to_subscription(subscription_id, data):
                sent_count += 1
        
        self.logger.debug(f"Broadcasted to {sent_count} subscriptions on topic '{topic}'")
        return sent_count
    
    async def send_error(self, connection_id: str, subscription_id: str, error: str):
        """Send error message to a subscription."""
        if connection_id not in self.connections:
            return False
        
        connection_info = self.connections[connection_id]
        
        message = {
            "id": subscription_id,
            "type": "error",
            "payload": [{"message": error}]
        }
        
        try:
            await connection_info.websocket.send_text(json.dumps(message))
            return True
        except Exception as e:
            self.logger.error(f"Failed to send error to {connection_id}: {e}")
            await self.disconnect(connection_id)
            return False
    
    async def send_complete(self, connection_id: str, subscription_id: str):
        """Send completion message to a subscription."""
        if connection_id not in self.connections:
            return False
        
        connection_info = self.connections[connection_id]
        
        message = {
            "id": subscription_id,
            "type": "complete"
        }
        
        try:
            await connection_info.websocket.send_text(json.dumps(message))
            await self._cleanup_subscription(subscription_id)
            return True
        except Exception as e:
            self.logger.error(f"Failed to send complete to {connection_id}: {e}")
            await self.disconnect(connection_id)
            return False
    
    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return len(self.connections)
    
    def get_subscription_count(self) -> int:
        """Get total number of active subscriptions."""
        return len(self.subscriptions)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection manager statistics."""
        return {
            "active_connections": len(self.connections),
            "active_subscriptions": len(self.subscriptions),
            "topics": len(self.topic_subscriptions),
            "connections_by_user": self._get_connections_by_user(),
            "subscriptions_by_topic": {
                topic: len(subs) for topic, subs in self.topic_subscriptions.items()
            }
        }
    
    def _extract_topic_from_query(self, query: str, variables: Dict[str, Any]) -> Optional[str]:
        """Extract topic identifier from GraphQL subscription query."""
        # Simple topic extraction - in production, this would be more sophisticated
        if "dataUpdates" in query:
            source = variables.get("source")
            table = variables.get("table")
            if source and table:
                return f"data_updates.{source}.{table}"
        
        if "queryPerformanceMetrics" in query:
            return "performance_metrics"
        
        return None
    
    async def _cleanup_subscription(self, subscription_id: str) -> bool:
        """Clean up a subscription and its references."""
        if subscription_id not in self.subscriptions:
            return False
        
        subscription_info = self.subscriptions[subscription_id]
        
        # Remove from connection
        if subscription_id in self.subscription_to_connection:
            connection_id = self.subscription_to_connection[subscription_id]
            if connection_id in self.connections:
                self.connections[connection_id].subscriptions.discard(subscription_id)
            del self.subscription_to_connection[subscription_id]
        
        # Remove from topic subscriptions
        query = subscription_info.query
        variables = subscription_info.variables
        topic = self._extract_topic_from_query(query, variables)
        if topic and topic in self.topic_subscriptions:
            self.topic_subscriptions[topic].discard(subscription_id)
            if not self.topic_subscriptions[topic]:
                del self.topic_subscriptions[topic]
        
        # Remove subscription
        del self.subscriptions[subscription_id]
        
        self.logger.debug(f"Cleaned up subscription: {subscription_id}")
        return True
    
    def _get_connections_by_user(self) -> Dict[str, int]:
        """Get connection count by user."""
        user_counts = {}
        for connection in self.connections.values():
            user_id = connection.user_id
            user_counts[user_id] = user_counts.get(user_id, 0) + 1
        return user_counts
    
    async def _periodic_cleanup(self):
        """Periodic cleanup of stale connections and subscriptions."""
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Clean up stale connections
                stale_connections = []
                now = datetime.now()
                
                for connection_id, connection_info in self.connections.items():
                    # Check if connection is stale (no ping in 5 minutes)
                    if (now - connection_info.last_ping).total_seconds() > 300:
                        stale_connections.append(connection_id)
                
                for connection_id in stale_connections:
                    self.logger.info(f"Cleaning up stale connection: {connection_id}")
                    await self.disconnect(connection_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeat to all connections."""
        while True:
            try:
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                
                for connection_id, connection_info in list(self.connections.items()):
                    try:
                        ping_message = {
                            "type": "ping"
                        }
                        await connection_info.websocket.send_text(json.dumps(ping_message))
                        connection_info.last_ping = datetime.now()
                    except Exception as e:
                        self.logger.warning(f"Heartbeat failed for {connection_id}: {e}")
                        await self.disconnect(connection_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")


# Global connection manager instance
websocket_manager = WebSocketConnectionManager()