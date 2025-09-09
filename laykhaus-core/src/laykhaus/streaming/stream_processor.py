"""
Real-time Stream Processing Framework for LaykHaus
Provides high-performance streaming data processing capabilities.
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional, Callable, AsyncGenerator
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class StreamStatus(Enum):
    """Stream processing status enumeration"""
    IDLE = "idle"
    RUNNING = "running" 
    PAUSED = "paused"
    ERROR = "error"
    STOPPED = "stopped"

@dataclass
class StreamMetrics:
    """Stream processing metrics"""
    messages_processed: int = 0
    messages_failed: int = 0
    processing_rate: float = 0.0
    avg_latency_ms: float = 0.0
    last_processed: Optional[datetime] = None
    status: StreamStatus = StreamStatus.IDLE

class RealTimeProcessor:
    """
    High-performance real-time data processor for streaming pipelines.
    Handles message processing, transformation, and routing.
    """
    
    def __init__(self, processor_id: str, config: Optional[Dict[str, Any]] = None):
        self.processor_id = processor_id
        self.config = config or {}
        self.status = StreamStatus.IDLE
        self.metrics = StreamMetrics()
        self.processors: Dict[str, Callable] = {}
        self.error_handlers: List[Callable] = []
        self._running = False
        
    async def register_processor(self, message_type: str, processor_func: Callable):
        """Register a processor function for a specific message type"""
        self.processors[message_type] = processor_func
        logger.info(f"Registered processor for message type: {message_type}")
        
    async def add_error_handler(self, handler: Callable):
        """Add error handler for processing failures"""
        self.error_handlers.append(handler)
        
    async def start(self):
        """Start the real-time processor"""
        self.status = StreamStatus.RUNNING
        self._running = True
        logger.info(f"RealTimeProcessor {self.processor_id} started")
        
    async def stop(self):
        """Stop the real-time processor"""
        self._running = False
        self.status = StreamStatus.STOPPED
        logger.info(f"RealTimeProcessor {self.processor_id} stopped")
        
    async def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single message"""
        if not self._running:
            return None
            
        start_time = datetime.utcnow()
        
        try:
            message_type = message.get('type', 'default')
            processor = self.processors.get(message_type)
            
            if processor:
                result = await processor(message)
                self.metrics.messages_processed += 1
                
                # Update metrics
                processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                self.metrics.avg_latency_ms = (
                    (self.metrics.avg_latency_ms * (self.metrics.messages_processed - 1) + processing_time)
                    / self.metrics.messages_processed
                )
                self.metrics.last_processed = datetime.utcnow()
                
                return result
            else:
                logger.warning(f"No processor registered for message type: {message_type}")
                return message
                
        except Exception as e:
            self.metrics.messages_failed += 1
            logger.error(f"Error processing message: {e}")
            
            # Execute error handlers
            for handler in self.error_handlers:
                try:
                    await handler(message, e)
                except Exception as handler_error:
                    logger.error(f"Error in error handler: {handler_error}")
                    
            return None
            
    def get_metrics(self) -> StreamMetrics:
        """Get current processing metrics"""
        return self.metrics

class StreamingPipeline:
    """
    Manages a pipeline of real-time processors for complex data workflows.
    """
    
    def __init__(self, pipeline_id: str, config: Optional[Dict[str, Any]] = None):
        self.pipeline_id = pipeline_id
        self.config = config or {}
        self.processors: List[RealTimeProcessor] = []
        self.status = StreamStatus.IDLE
        self._running = False
        
    async def add_processor(self, processor: RealTimeProcessor):
        """Add a processor to the pipeline"""
        self.processors.append(processor)
        logger.info(f"Added processor {processor.processor_id} to pipeline {self.pipeline_id}")
        
    async def start(self):
        """Start the entire pipeline"""
        self.status = StreamStatus.RUNNING
        self._running = True
        
        # Start all processors
        for processor in self.processors:
            await processor.start()
            
        logger.info(f"StreamingPipeline {self.pipeline_id} started with {len(self.processors)} processors")
        
    async def stop(self):
        """Stop the entire pipeline"""
        self._running = False
        self.status = StreamStatus.STOPPED
        
        # Stop all processors
        for processor in self.processors:
            await processor.stop()
            
        logger.info(f"StreamingPipeline {self.pipeline_id} stopped")
        
    async def process_through_pipeline(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process message through the entire pipeline"""
        if not self._running:
            return None
            
        current_message = message
        
        for processor in self.processors:
            if current_message is None:
                break
                
            current_message = await processor.process_message(current_message)
            
        return current_message
        
    def get_pipeline_metrics(self) -> Dict[str, StreamMetrics]:
        """Get metrics for all processors in the pipeline"""
        return {
            processor.processor_id: processor.get_metrics()
            for processor in self.processors
        }

class DataStreamManager:
    """
    Manages multiple streaming pipelines and provides centralized control.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.pipelines: Dict[str, StreamingPipeline] = {}
        self.processors: Dict[str, RealTimeProcessor] = {}
        self._running = False
        
    async def create_pipeline(self, pipeline_id: str, config: Optional[Dict[str, Any]] = None) -> StreamingPipeline:
        """Create a new streaming pipeline"""
        pipeline = StreamingPipeline(pipeline_id, config)
        self.pipelines[pipeline_id] = pipeline
        logger.info(f"Created streaming pipeline: {pipeline_id}")
        return pipeline
        
    async def create_processor(self, processor_id: str, config: Optional[Dict[str, Any]] = None) -> RealTimeProcessor:
        """Create a new real-time processor"""
        processor = RealTimeProcessor(processor_id, config)
        self.processors[processor_id] = processor
        logger.info(f"Created real-time processor: {processor_id}")
        return processor
        
    async def start_all(self):
        """Start all pipelines and processors"""
        self._running = True
        
        # Start all processors
        for processor in self.processors.values():
            await processor.start()
            
        # Start all pipelines
        for pipeline in self.pipelines.values():
            await pipeline.start()
            
        logger.info(f"DataStreamManager started with {len(self.pipelines)} pipelines and {len(self.processors)} processors")
        
    async def stop_all(self):
        """Stop all pipelines and processors"""
        self._running = False
        
        # Stop all pipelines
        for pipeline in self.pipelines.values():
            await pipeline.stop()
            
        # Stop all processors
        for processor in self.processors.values():
            await processor.stop()
            
        logger.info("DataStreamManager stopped")
        
    async def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics for all pipelines and processors"""
        return {
            "pipelines": {
                pipeline_id: pipeline.get_pipeline_metrics()
                for pipeline_id, pipeline in self.pipelines.items()
            },
            "processors": {
                processor_id: processor.get_metrics()
                for processor_id, processor in self.processors.items()
            },
            "overall": {
                "total_pipelines": len(self.pipelines),
                "total_processors": len(self.processors),
                "status": "running" if self._running else "stopped",
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all streaming components"""
        pipeline_health = {}
        processor_health = {}
        
        for pipeline_id, pipeline in self.pipelines.items():
            pipeline_health[pipeline_id] = {
                "status": pipeline.status.value,
                "processors": len(pipeline.processors),
                "healthy": pipeline.status == StreamStatus.RUNNING
            }
            
        for processor_id, processor in self.processors.items():
            processor_health[processor_id] = {
                "status": processor.status.value,
                "messages_processed": processor.metrics.messages_processed,
                "messages_failed": processor.metrics.messages_failed,
                "healthy": processor.status == StreamStatus.RUNNING and processor.metrics.messages_failed == 0
            }
            
        return {
            "overall_healthy": all(
                p.get("healthy", False) for p in pipeline_health.values()
            ) and all(
                p.get("healthy", False) for p in processor_health.values()
            ),
            "pipelines": pipeline_health,
            "processors": processor_health,
            "timestamp": datetime.utcnow().isoformat()
        }

# Demo processors for LaykHaus use cases
async def data_transformation_processor(message: Dict[str, Any]) -> Dict[str, Any]:
    """Demo processor for data transformation tasks"""
    if message.get('type') == 'raw_data':
        # Simulate data transformation
        raw_value = message.get('value', 0.0)
        transformed_value = raw_value * 1.15  # Simple transformation
        
        return {
            **message,
            'transformed_value': transformed_value,
            'transformation_factor': 1.15,
            'processed_at': datetime.utcnow().isoformat(),
            'processor': 'data_transformation_processor'
        }
    return message

async def data_enrichment_processor(message: Dict[str, Any]) -> Dict[str, Any]:
    """Demo processor for data enrichment"""
    if message.get('type') == 'event_data':
        # Simulate data enrichment
        source = message.get('source', 'UNKNOWN')
        category = message.get('category', 'general')
        
        return {
            **message,
            'normalized_source': source.upper(),
            'enriched_category': f"{category}_enriched",
            'metadata': {'enriched': True, 'version': '1.0'},
            'processed_at': datetime.utcnow().isoformat(),
            'processor': 'data_enrichment_processor'
        }
    return message