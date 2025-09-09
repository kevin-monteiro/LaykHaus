"""
Kafka Streaming Engine - The Hero of Real-time Data Processing
Advanced Kafka integration for high-throughput streaming data
"""

import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

class StreamingMode(Enum):
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    AT_MOST_ONCE = "at_most_once"

class MessageFormat(Enum):
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    RAW = "raw"

@dataclass
class StreamConfig:
    topic_name: str
    partition_count: int = 3
    replication_factor: int = 1
    retention_hours: int = 168  # 1 week
    compression_type: str = "gzip"
    batch_size: int = 16384
    linger_ms: int = 10

@dataclass
class StreamMessage:
    key: str
    value: Any
    headers: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    partition: Optional[int] = None
    offset: Optional[int] = None

@dataclass
class ConsumerConfig:
    group_id: str
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    max_poll_records: int = 500
    session_timeout_ms: int = 30000

class KafkaStreamingEngine:
    """
    Enterprise Kafka Streaming Engine - The backbone of real-time data flow
    """
    
    def __init__(self, bootstrap_servers: List[str] = None):
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.producers = {}
        self.consumers = {}
        self.admin_client = None
        self.stream_configs = {}
        self.active_streams = set()
        
    async def initialize(self):
        """Initialize Kafka engine (simplified)"""
        try:
            # For demo purposes, simulate Kafka initialization
            self._kafka_available = False
            try:
                from aiokafka.admin import AIOKafkaAdminClient
                self.admin_client = AIOKafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers
                )
                await self.admin_client.start()
                self._kafka_available = True
                logger.info("Kafka streaming engine initialized successfully")
            except ImportError:
                logger.info("Kafka not available - running in simulation mode")
                self._kafka_available = False
            except Exception as e:
                logger.warning(f"Kafka connection failed, using simulation: {e}")
                self._kafka_available = False
        except Exception as e:
            logger.error(f"Failed to initialize Kafka engine: {e}")
            raise
    
    async def create_stream(self, config: StreamConfig) -> bool:
        """Create a new Kafka stream"""
        try:
            if self._kafka_available:
                from aiokafka.admin import NewTopic
                
                topic = NewTopic(
                    name=config.topic_name,
                    num_partitions=config.partition_count,
                    replication_factor=config.replication_factor
                )
                
                await self.admin_client.create_topics([topic])
            
            # Always track the stream config (even in simulation mode)
            self.stream_configs[config.topic_name] = config
            self.active_streams.add(config.topic_name)
            
            logger.info(f"🚀 Created Kafka stream: {config.topic_name} with {config.partition_count} partitions")
            return True
            
        except Exception as e:
            logger.warning(f"Stream creation simulated for {config.topic_name}: {e}")
            # Still add to active streams for demo
            self.stream_configs[config.topic_name] = config
            self.active_streams.add(config.topic_name)
            return True
    
    async def get_producer(self, stream_name: str) -> 'KafkaProducer':
        """Get or create a producer for the specified stream"""
        if stream_name not in self.producers:
            config = self.stream_configs.get(stream_name, StreamConfig(stream_name))
            
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                stream_config=config,
                engine=self
            )
            
            await producer.start()
            self.producers[stream_name] = producer
        
        return self.producers[stream_name]
    
    async def get_consumer(self, stream_name: str, consumer_config: ConsumerConfig) -> 'KafkaConsumer':
        """Get or create a consumer for the specified stream"""
        consumer_key = f"{stream_name}_{consumer_config.group_id}"
        
        if consumer_key not in self.consumers:
            consumer = KafkaConsumer(
                stream_name=stream_name,
                bootstrap_servers=self.bootstrap_servers,
                consumer_config=consumer_config,
                engine=self
            )
            
            await consumer.start()
            self.consumers[consumer_key] = consumer
        
        return self.consumers[consumer_key]
    
    async def list_streams(self) -> List[Dict[str, Any]]:
        """List all available Kafka streams with metadata"""
        try:
            metadata = await self.admin_client.describe_topics()
            
            streams = []
            for topic_name, topic_metadata in metadata.items():
                if topic_name in self.active_streams:
                    config = self.stream_configs.get(topic_name)
                    
                    streams.append({
                        "name": topic_name,
                        "partitions": len(topic_metadata.partitions),
                        "replication_factor": len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                        "config": config.__dict__ if config else {},
                        "status": "active"
                    })
            
            return streams
            
        except Exception as e:
            logger.error(f"Failed to list streams: {e}")
            return []
    
    async def get_stream_metrics(self, stream_name: str) -> Dict[str, Any]:
        """Get comprehensive metrics for a stream"""
        try:
            # Get topic metadata
            metadata = await self.admin_client.describe_topics([stream_name])
            topic_metadata = metadata.get(stream_name)
            
            if not topic_metadata:
                return {"error": "Stream not found"}
            
            # Calculate metrics
            partition_count = len(topic_metadata.partitions)
            
            # Get consumer group information
            consumer_groups = []
            if hasattr(self.admin_client, 'list_consumer_groups'):
                groups = await self.admin_client.list_consumer_groups()
                consumer_groups = [group.group_id for group in groups]
            
            return {
                "stream_name": stream_name,
                "partition_count": partition_count,
                "active_consumers": len([key for key in self.consumers.keys() if stream_name in key]),
                "active_producers": 1 if stream_name in self.producers else 0,
                "consumer_groups": len(consumer_groups),
                "status": "healthy",
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get metrics for stream {stream_name}: {e}")
            return {"error": str(e)}
    
    async def shutdown(self):
        """Gracefully shutdown all Kafka connections"""
        logger.info("Shutting down Kafka streaming engine...")
        
        # Stop all producers
        for producer in self.producers.values():
            await producer.stop()
        
        # Stop all consumers
        for consumer in self.consumers.values():
            await consumer.stop()
        
        # Stop admin client
        if self.admin_client:
            await self.admin_client.close()
        
        logger.info("Kafka streaming engine shutdown complete")

class KafkaProducer:
    """
    High-performance Kafka producer with batching and compression
    """
    
    def __init__(self, bootstrap_servers: List[str], stream_config: StreamConfig, engine: KafkaStreamingEngine):
        self.bootstrap_servers = bootstrap_servers
        self.stream_config = stream_config
        self.engine = engine
        self.producer = None
        self.message_count = 0
        self.bytes_sent = 0
        
    async def start(self):
        """Start the Kafka producer with optimized settings"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                batch_size=self.stream_config.batch_size,
                linger_ms=self.stream_config.linger_ms,
                compression_type=self.stream_config.compression_type,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                enable_idempotence=True,  # Exactly-once semantics
                value_serializer=self._serialize_value,
                key_serializer=self._serialize_key
            )
            
            await self.producer.start()
            logger.info(f"Started Kafka producer for stream: {self.stream_config.topic_name}")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise
    
    def _serialize_key(self, key):
        """Serialize message key"""
        if key is None:
            return None
        return str(key).encode('utf-8')
    
    def _serialize_value(self, value):
        """Serialize message value"""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            return json.dumps(value, default=str).encode('utf-8')
    
    async def send_message(self, message: StreamMessage) -> bool:
        """Send a message to the stream"""
        try:
            headers = [(k, v.encode('utf-8')) for k, v in message.headers.items()]
            
            future = await self.producer.send_and_wait(
                topic=self.stream_config.topic_name,
                value=message.value,
                key=message.key,
                partition=message.partition,
                headers=headers
            )
            
            self.message_count += 1
            
            logger.debug(f"Sent message to {self.stream_config.topic_name}: key={message.key}")
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    async def send_batch(self, messages: List[StreamMessage]) -> Dict[str, Any]:
        """Send a batch of messages efficiently"""
        sent_count = 0
        failed_count = 0
        
        try:
            # Send all messages asynchronously
            futures = []
            for message in messages:
                headers = [(k, v.encode('utf-8')) for k, v in message.headers.items()]
                
                future = self.producer.send(
                    topic=self.stream_config.topic_name,
                    value=message.value,
                    key=message.key,
                    partition=message.partition,
                    headers=headers
                )
                futures.append(future)
            
            # Wait for all messages to be sent
            results = await asyncio.gather(*futures, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    failed_count += 1
                    logger.error(f"Failed to send message in batch: {result}")
                else:
                    sent_count += 1
            
            self.message_count += sent_count
            
            return {
                "batch_size": len(messages),
                "sent_count": sent_count,
                "failed_count": failed_count,
                "success_rate": (sent_count / len(messages)) * 100
            }
            
        except Exception as e:
            logger.error(f"Batch send failed: {e}")
            return {"error": str(e)}
    
    async def stop(self):
        """Stop the producer"""
        if self.producer:
            await self.producer.stop()
            logger.info(f"Stopped Kafka producer for stream: {self.stream_config.topic_name}")

class KafkaConsumer:
    """
    High-performance Kafka consumer with automatic offset management
    """
    
    def __init__(self, stream_name: str, bootstrap_servers: List[str], consumer_config: ConsumerConfig, engine: KafkaStreamingEngine):
        self.stream_name = stream_name
        self.bootstrap_servers = bootstrap_servers
        self.consumer_config = consumer_config
        self.engine = engine
        self.consumer = None
        self.message_handlers = []
        self.running = False
        self.messages_processed = 0
        
    async def start(self):
        """Start the Kafka consumer"""
        try:
            self.consumer = AIOKafkaConsumer(
                self.stream_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_config.group_id,
                auto_offset_reset=self.consumer_config.auto_offset_reset,
                enable_auto_commit=self.consumer_config.enable_auto_commit,
                max_poll_records=self.consumer_config.max_poll_records,
                session_timeout_ms=self.consumer_config.session_timeout_ms,
                value_deserializer=self._deserialize_value,
                key_deserializer=self._deserialize_key
            )
            
            await self.consumer.start()
            logger.info(f"Started Kafka consumer for stream: {self.stream_name}, group: {self.consumer_config.group_id}")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            raise
    
    def _deserialize_key(self, key_bytes):
        """Deserialize message key"""
        if key_bytes is None:
            return None
        return key_bytes.decode('utf-8')
    
    def _deserialize_value(self, value_bytes):
        """Deserialize message value"""
        if value_bytes is None:
            return None
        
        try:
            # Try JSON first
            return json.loads(value_bytes.decode('utf-8'))
        except:
            # Fall back to string
            return value_bytes.decode('utf-8')
    
    def add_message_handler(self, handler: Callable[[StreamMessage], None]):
        """Add a message handler function"""
        self.message_handlers.append(handler)
        logger.info(f"Added message handler for stream: {self.stream_name}")
    
    async def consume_messages(self) -> AsyncGenerator[StreamMessage, None]:
        """Consume messages from the stream"""
        self.running = True
        
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                
                # Convert Kafka message to StreamMessage
                headers = {k: v.decode('utf-8') for k, v in msg.headers} if msg.headers else {}
                
                stream_message = StreamMessage(
                    key=msg.key,
                    value=msg.value,
                    headers=headers,
                    timestamp=datetime.fromtimestamp(msg.timestamp / 1000),
                    partition=msg.partition,
                    offset=msg.offset
                )
                
                # Process with handlers
                for handler in self.message_handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler(stream_message)
                        else:
                            handler(stream_message)
                    except Exception as e:
                        logger.error(f"Message handler error: {e}")
                
                self.messages_processed += 1
                yield stream_message
                
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            self.running = False
    
    async def consume_batch(self, batch_size: int = 100, timeout_ms: int = 5000) -> List[StreamMessage]:
        """Consume a batch of messages"""
        messages = []
        start_time = time.time()
        
        try:
            async for message in self.consume_messages():
                messages.append(message)
                
                # Check batch size limit
                if len(messages) >= batch_size:
                    break
                
                # Check timeout
                if (time.time() - start_time) * 1000 >= timeout_ms:
                    break
            
            return messages
            
        except Exception as e:
            logger.error(f"Error consuming batch: {e}")
            return messages
    
    async def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info(f"Stopped Kafka consumer for stream: {self.stream_name}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics"""
        return {
            "stream_name": self.stream_name,
            "group_id": self.consumer_config.group_id,
            "messages_processed": self.messages_processed,
            "running": self.running,
            "handler_count": len(self.message_handlers)
        }