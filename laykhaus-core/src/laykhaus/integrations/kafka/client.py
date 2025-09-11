"""
Kafka connector implementation for streaming data ingestion.
"""

import asyncio
import json
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from laykhaus.connectors.base import (
    BaseConnector,
    CapabilityContract,
    ConnectionConfig,
    ConnectorType,
    ConsistencyLevel,
    DataType,
    HealthStatus,
    PushdownType,
    QueryResult,
    SQLFeature,
    Statistics,
    TransactionLevel,
)
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class KafkaConnector(BaseConnector):
    """
    Streaming data connector for Apache Kafka with real-time processing.
    Supports schema registry integration and windowed aggregations.
    """
    
    def __init__(self, connector_id: str, config: ConnectionConfig):
        """Initialize Kafka connector."""
        super().__init__(connector_id, config)
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.topics: List[str] = config.extra_params.get("topics", [])
        self.group_id: str = config.extra_params.get("group_id", "laykhaus-consumer")
        self.schema_registry_url: Optional[str] = config.extra_params.get("schema_registry_url")
        self._capability_contract = self._define_capabilities()
        self._message_buffer: List[Dict] = []
        self._consume_task: Optional[asyncio.Task] = None
    
    def _define_capabilities(self) -> CapabilityContract:
        """Define Kafka connector capabilities."""
        return CapabilityContract(
            connector_id=self.connector_id,
            connector_type=ConnectorType.KAFKA,
            sql_features={
                SQLFeature.SELECT,
                SQLFeature.WHERE,
                SQLFeature.LIMIT,
            },
            data_types={
                DataType.VARCHAR,
                DataType.TEXT,
                DataType.INTEGER,
                DataType.BIGINT,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.BOOLEAN,
                DataType.TIMESTAMP,
                DataType.JSON,
            },
            aggregate_functions=set(),  # Aggregations done in federation layer
            join_types=set(),  # Joins done in federation layer
            pushdown_capabilities={
                PushdownType.FILTER,  # Basic message filtering
                PushdownType.LIMIT,   # Limit number of messages
            },
            transaction_support=TransactionLevel.NONE,
            consistency_guarantee=ConsistencyLevel.EVENTUAL,
            max_concurrent_connections=1,  # Single consumer per connector
            supports_streaming=True,
            supports_schema_discovery=True,
            supports_statistics=True,
            supports_explain_plan=False,
        )
    
    async def connect(self) -> None:
        """Establish connection to Kafka cluster."""
        try:
            self.logger.info(
                "Connecting to Kafka",
                bootstrap_servers=f"{self.config.host}:{self.config.port}",
                topics=self.topics,
                group_id=self.group_id,
            )
            
            # Create consumer
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=f"{self.config.host}:{self.config.port}",
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                session_timeout_ms=30000,
                request_timeout_ms=60000,
            )
            
            # Create producer for testing
            self.producer = AIOKafkaProducer(
                bootstrap_servers=f"{self.config.host}:{self.config.port}",
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
            
            # Start consumer and producer
            await self.consumer.start()
            await self.producer.start()
            
            # Start background message consumption
            self._consume_task = asyncio.create_task(self._consume_messages())
            
            self._connected = True
            self.logger.info("Successfully connected to Kafka")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close connection to Kafka."""
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            await self.consumer.stop()
        
        if self.producer:
            await self.producer.stop()
        
        self._connected = False
        self.logger.info("Disconnected from Kafka")
    
    async def _consume_messages(self) -> None:
        """Background task to consume messages from Kafka."""
        max_buffer_size = 10000
        
        try:
            async for msg in self.consumer:
                # Add message to buffer
                message_data = {
                    'topic': msg.topic,
                    'partition': msg.partition,
                    'offset': msg.offset,
                    'timestamp': msg.timestamp,
                    'key': msg.key,
                    'value': msg.value,
                }
                
                self._message_buffer.append(message_data)
                
                # Keep buffer size manageable
                if len(self._message_buffer) > max_buffer_size:
                    self._message_buffer = self._message_buffer[-max_buffer_size:]
                    
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"Error consuming messages: {e}")
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict] = None
    ) -> QueryResult:
        """
        Execute a "query" against Kafka messages.
        This simulates SQL-like queries on the message buffer.
        
        Args:
            query: SQL-like query (e.g., "SELECT * FROM messages LIMIT 100")
            params: Query parameters
        
        Returns:
            QueryResult with messages as rows
        """
        if not self._connected:
            raise RuntimeError("Not connected to Kafka")
        
        # Simple query parsing (production would use proper SQL parser)
        limit = 100
        if "LIMIT" in query.upper():
            parts = query.upper().split("LIMIT")
            if len(parts) > 1:
                try:
                    limit = int(parts[1].strip())
                except:
                    pass
        
        # Return messages from buffer
        messages = self._message_buffer[-limit:] if self._message_buffer else []
        
        schema = {
            'topic': 'varchar',
            'partition': 'integer',
            'offset': 'bigint',
            'timestamp': 'timestamp',
            'key': 'varchar',
            'value': 'json',
        }
        
        return QueryResult(
            rows=messages,
            schema=schema,
            row_count=len(messages),
            execution_time_ms=0.0,  # Instant from buffer
            from_cache=True,  # From message buffer
            source_name=self.connector_id,
            metadata={
                'buffer_size': len(self._message_buffer),
                'topics': self.topics,
            }
        )
    
    def get_capabilities(self) -> CapabilityContract:
        """Get connector capabilities."""
        return self._capability_contract
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information for Kafka topics.
        
        Returns:
            Schema information for configured topics
        """
        schema = {}
        
        for topic in self.topics:
            schema[topic] = {
                'type': 'stream',
                'fields': {
                    'topic': 'varchar',
                    'partition': 'integer',
                    'offset': 'bigint',
                    'timestamp': 'timestamp',
                    'key': 'varchar',
                    'value': 'json',
                },
                'partitions': await self._get_topic_partitions(topic),
            }
        
        return schema
    
    async def _get_topic_partitions(self, topic: str) -> int:
        """Get number of partitions for a topic."""
        try:
            if self.consumer:
                partitions = self.consumer.partitions_for_topic(topic)
                return len(partitions) if partitions else 0
            return 0
        except:
            return 0
    
    async def health_check(self) -> HealthStatus:
        """
        Check Kafka connection health.
        
        Returns:
            HealthStatus with connection information
        """
        try:
            if not self.consumer:
                return HealthStatus(
                    healthy=False,
                    latency_ms=0,
                    message="Kafka consumer not initialized",
                    last_check=datetime.utcnow().isoformat(),
                    error="No consumer"
                )
            
            # Check if consumer is active by checking assignment
            assignment = self.consumer.assignment()
            subscription = self.consumer.subscription()
            
            return HealthStatus(
                healthy=True,
                latency_ms=0.0,  # Kafka doesn't have simple ping
                message="Kafka is healthy",
                last_check=datetime.utcnow().isoformat(),
                metadata={
                    'subscribed_topics': list(subscription) if subscription else [],
                    'assigned_partitions': len(assignment) if assignment else 0,
                    'buffer_size': len(self._message_buffer),
                }
            )
            
        except Exception as e:
            return HealthStatus(
                healthy=False,
                latency_ms=0,
                message="Kafka health check failed",
                last_check=datetime.utcnow().isoformat(),
                error=str(e)
            )
    
    async def get_statistics(self) -> Statistics:
        """
        Get Kafka statistics.
        
        Returns:
            Statistics about topics and messages
        """
        topic_stats = {}
        total_messages = 0
        
        for topic in self.topics:
            # Get topic metadata
            partitions = await self._get_topic_partitions(topic)
            
            # Count messages in buffer for this topic
            topic_messages = [m for m in self._message_buffer if m['topic'] == topic]
            
            topic_stats[topic] = {
                'partitions': partitions,
                'buffered_messages': len(topic_messages),
                'latest_offset': max([m['offset'] for m in topic_messages], default=0),
            }
            
            total_messages += len(topic_messages)
        
        return Statistics(
            table_count=len(self.topics),  # Topics as "tables"
            total_rows=total_messages,
            total_size_bytes=None,  # Not easily available
            last_updated=datetime.utcnow().isoformat(),
            table_statistics=topic_stats
        )
    
    async def subscribe_to_changes(self) -> AsyncIterator[Dict[str, Any]]:
        """
        Subscribe to real-time messages from Kafka.
        
        Yields:
            Messages as they arrive
        """
        if not self._connected or not self.consumer:
            raise RuntimeError("Not connected to Kafka")
        
        async for msg in self.consumer:
            yield {
                'topic': msg.topic,
                'partition': msg.partition,
                'offset': msg.offset,
                'timestamp': msg.timestamp,
                'key': msg.key,
                'value': msg.value,
            }
    
    async def send_message(
        self, 
        topic: str, 
        value: Dict, 
        key: Optional[str] = None
    ) -> None:
        """
        Send a message to Kafka (for testing).
        
        Args:
            topic: Topic to send to
            value: Message value
            key: Optional message key
        """
        if not self.producer:
            raise RuntimeError("Producer not initialized")
        
        await self.producer.send(topic, value=value, key=key)
    
    def create_spark_dataframe(self, spark, topic: str):
        """
        Create a Spark DataFrame for a Kafka topic.
        This encapsulates the integration-specific logic for Kafka.
        
        Args:
            spark: SparkSession instance
            topic: Kafka topic name
            
        Returns:
            Spark DataFrame for streaming from Kafka topic
        """
        # For batch reading (last 1000 messages as snapshot)
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{self.config.host}:{self.config.port}") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()
        
        # Parse JSON values
        from pyspark.sql.functions import col, from_json, get_json_object
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
        
        # Define a flexible schema for the JSON data
        # This should be made more dynamic in production
        parsed_df = df.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("raw_value"),
            col("timestamp"),
            col("topic"),
            col("partition"),
            col("offset")
        )
        
        # Extract common fields from JSON value using get_json_object
        # This allows flexibility for different message structures
        result_df = parsed_df.select(
            "*",
            get_json_object(col("raw_value"), "$.panel_id").alias("panel_id"),
            get_json_object(col("raw_value"), "$.timestamp").alias("timestamp"),
            get_json_object(col("raw_value"), "$.power_output_watts").cast("double").alias("power_output_watts"),
            get_json_object(col("raw_value"), "$.voltage").cast("double").alias("voltage"),
            get_json_object(col("raw_value"), "$.current_amps").cast("double").alias("current_amps"),
            get_json_object(col("raw_value"), "$.temperature_celsius").cast("double").alias("temperature_celsius"),
            get_json_object(col("raw_value"), "$.efficiency_percentage").cast("double").alias("efficiency_percentage"),
            get_json_object(col("raw_value"), "$.daily_energy_kwh").cast("double").alias("daily_energy_kwh"),
            get_json_object(col("raw_value"), "$.inverter_status").alias("inverter_status"),
            get_json_object(col("raw_value"), "$.grid_frequency_hz").cast("double").alias("grid_frequency_hz")
        ).drop("raw_value", "key", "topic", "partition", "offset")
        
        return result_df