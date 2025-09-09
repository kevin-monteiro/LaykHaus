"""
Real-time Analytics Engine for LaykHaus Streaming Platform
Provides analytics, metrics collection, and dashboard capabilities.
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json

logger = logging.getLogger(__name__)

@dataclass
class StreamingMetrics:
    """Comprehensive streaming metrics"""
    # Processing metrics
    total_messages: int = 0
    messages_per_second: float = 0.0
    successful_messages: int = 0
    failed_messages: int = 0
    
    # Latency metrics
    avg_processing_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    
    # Throughput metrics
    bytes_processed: int = 0
    bytes_per_second: float = 0.0
    
    # Error metrics
    error_rate: float = 0.0
    last_error: Optional[str] = None
    error_count_by_type: Dict[str, int] = field(default_factory=dict)
    
    # Timestamps
    start_time: Optional[datetime] = None
    last_update: Optional[datetime] = None
    
    def calculate_derived_metrics(self):
        """Calculate derived metrics"""
        if self.total_messages > 0:
            self.error_rate = self.failed_messages / self.total_messages
        
        if self.start_time and self.last_update:
            duration = (self.last_update - self.start_time).total_seconds()
            if duration > 0:
                self.messages_per_second = self.total_messages / duration
                self.bytes_per_second = self.bytes_processed / duration

class RealTimeAnalytics:
    """
    Real-time analytics engine for streaming data processing.
    Provides metrics collection, aggregation, and analysis capabilities.
    """
    
    def __init__(self, analytics_id: str, config: Optional[Dict[str, Any]] = None):
        self.analytics_id = analytics_id
        self.config = config or {}
        self.metrics = StreamingMetrics()
        self.metrics.start_time = datetime.utcnow()
        
        # Time-series data storage (in-memory for demo)
        self.time_series_data = defaultdict(lambda: deque(maxlen=1000))
        self.latency_samples = deque(maxlen=1000)
        self.error_log = deque(maxlen=100)
        
        # Analytics state
        self._running = False
        self._analysis_tasks: Set[asyncio.Task] = set()
        
    async def start(self):
        """Start the analytics engine"""
        self._running = True
        
        # Start background analytics tasks
        task = asyncio.create_task(self._analytics_loop())
        self._analysis_tasks.add(task)
        task.add_done_callback(self._analysis_tasks.discard)
        
        logger.info(f"RealTimeAnalytics {self.analytics_id} started")
        
    async def stop(self):
        """Stop the analytics engine"""
        self._running = False
        
        # Cancel all background tasks
        for task in self._analysis_tasks:
            task.cancel()
        
        if self._analysis_tasks:
            await asyncio.gather(*self._analysis_tasks, return_exceptions=True)
            
        logger.info(f"RealTimeAnalytics {self.analytics_id} stopped")
        
    async def record_message_processed(self, message_size: int = 0, processing_time_ms: float = 0.0):
        """Record a successfully processed message"""
        self.metrics.total_messages += 1
        self.metrics.successful_messages += 1
        self.metrics.bytes_processed += message_size
        self.metrics.last_update = datetime.utcnow()
        
        # Record latency sample
        if processing_time_ms > 0:
            self.latency_samples.append(processing_time_ms)
            
        # Update derived metrics
        self.metrics.calculate_derived_metrics()
        
        # Store time-series data
        timestamp = datetime.utcnow()
        self.time_series_data['messages_per_minute'].append((timestamp, self.metrics.messages_per_second * 60))
        self.time_series_data['avg_latency'].append((timestamp, processing_time_ms))
        
    async def record_message_failed(self, error_type: str, error_message: str):
        """Record a failed message"""
        self.metrics.total_messages += 1
        self.metrics.failed_messages += 1
        self.metrics.last_error = error_message
        self.metrics.last_update = datetime.utcnow()
        
        # Track error by type
        if error_type not in self.metrics.error_count_by_type:
            self.metrics.error_count_by_type[error_type] = 0
        self.metrics.error_count_by_type[error_type] += 1
        
        # Log error
        self.error_log.append({
            'timestamp': datetime.utcnow().isoformat(),
            'error_type': error_type,
            'error_message': error_message
        })
        
        # Update derived metrics
        self.metrics.calculate_derived_metrics()
        
    def calculate_latency_percentiles(self) -> Dict[str, float]:
        """Calculate latency percentiles"""
        if not self.latency_samples:
            return {'p50': 0.0, 'p95': 0.0, 'p99': 0.0}
            
        sorted_samples = sorted(self.latency_samples)
        length = len(sorted_samples)
        
        return {
            'p50': sorted_samples[int(length * 0.5)] if length > 0 else 0.0,
            'p95': sorted_samples[int(length * 0.95)] if length > 0 else 0.0,
            'p99': sorted_samples[int(length * 0.99)] if length > 0 else 0.0
        }
        
    async def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get current real-time metrics"""
        percentiles = self.calculate_latency_percentiles()
        
        return {
            'analytics_id': self.analytics_id,
            'metrics': {
                'total_messages': self.metrics.total_messages,
                'successful_messages': self.metrics.successful_messages,
                'failed_messages': self.metrics.failed_messages,
                'messages_per_second': round(self.metrics.messages_per_second, 2),
                'bytes_per_second': self.metrics.bytes_per_second,
                'error_rate': round(self.metrics.error_rate * 100, 2),
                'avg_latency_ms': round(self.metrics.avg_processing_latency_ms, 2),
                'p95_latency_ms': round(percentiles['p95'], 2),
                'p99_latency_ms': round(percentiles['p99'], 2),
                'uptime_seconds': (datetime.utcnow() - self.metrics.start_time).total_seconds() if self.metrics.start_time else 0
            },
            'errors': {
                'last_error': self.metrics.last_error,
                'error_count_by_type': dict(self.metrics.error_count_by_type),
                'recent_errors': list(self.error_log)[-5:]  # Last 5 errors
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
    async def get_time_series_data(self, metric_name: str, duration_minutes: int = 60) -> List[Dict[str, Any]]:
        """Get time-series data for a specific metric"""
        if metric_name not in self.time_series_data:
            return []
            
        cutoff_time = datetime.utcnow() - timedelta(minutes=duration_minutes)
        
        return [
            {'timestamp': timestamp.isoformat(), 'value': value}
            for timestamp, value in self.time_series_data[metric_name]
            if timestamp >= cutoff_time
        ]
        
    async def _analytics_loop(self):
        """Background analytics processing loop"""
        while self._running:
            try:
                # Update average latency from samples
                if self.latency_samples:
                    self.metrics.avg_processing_latency_ms = sum(self.latency_samples) / len(self.latency_samples)
                
                # Clean up old time-series data (keep last hour)
                cutoff_time = datetime.utcnow() - timedelta(hours=1)
                for metric_name, data_points in self.time_series_data.items():
                    while data_points and data_points[0][0] < cutoff_time:
                        data_points.popleft()
                
                # Sleep for analytics update interval
                await asyncio.sleep(self.config.get('analytics_interval', 5))
                
            except Exception as e:
                logger.error(f"Error in analytics loop: {e}")
                await asyncio.sleep(1)

class LiveDashboard:
    """
    Live dashboard for real-time streaming analytics visualization.
    Provides web-based dashboard capabilities.
    """
    
    def __init__(self, dashboard_id: str, config: Optional[Dict[str, Any]] = None):
        self.dashboard_id = dashboard_id
        self.config = config or {}
        self.analytics_engines: Dict[str, RealTimeAnalytics] = {}
        self.dashboard_data = {}
        self._running = False
        
    async def register_analytics_engine(self, analytics: RealTimeAnalytics):
        """Register an analytics engine with the dashboard"""
        self.analytics_engines[analytics.analytics_id] = analytics
        logger.info(f"Registered analytics engine {analytics.analytics_id} with dashboard")
        
    async def start(self):
        """Start the live dashboard"""
        self._running = True
        logger.info(f"LiveDashboard {self.dashboard_id} started")
        
    async def stop(self):
        """Stop the live dashboard"""
        self._running = False
        logger.info(f"LiveDashboard {self.dashboard_id} stopped")
        
    async def get_dashboard_data(self) -> Dict[str, Any]:
        """Get complete dashboard data"""
        if not self._running:
            return {}
            
        dashboard_data = {
            'dashboard_id': self.dashboard_id,
            'timestamp': datetime.utcnow().isoformat(),
            'analytics_engines': {},
            'summary': {
                'total_engines': len(self.analytics_engines),
                'total_messages': 0,
                'total_errors': 0,
                'avg_throughput': 0.0
            }
        }
        
        # Collect data from all analytics engines
        total_messages = 0
        total_errors = 0
        total_throughput = 0.0
        
        for engine_id, analytics in self.analytics_engines.items():
            engine_data = await analytics.get_real_time_metrics()
            dashboard_data['analytics_engines'][engine_id] = engine_data
            
            # Aggregate for summary
            metrics = engine_data.get('metrics', {})
            total_messages += metrics.get('total_messages', 0)
            total_errors += metrics.get('failed_messages', 0)
            total_throughput += metrics.get('messages_per_second', 0.0)
            
        # Update summary
        dashboard_data['summary']['total_messages'] = total_messages
        dashboard_data['summary']['total_errors'] = total_errors
        dashboard_data['summary']['avg_throughput'] = round(total_throughput, 2)
        
        return dashboard_data
        
    async def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summarized metrics across all engines"""
        summary = {
            'total_engines': len(self.analytics_engines),
            'engines_running': sum(1 for _ in self.analytics_engines.values()),  # Simplified
            'total_messages_processed': 0,
            'total_messages_failed': 0,
            'avg_messages_per_second': 0.0,
            'max_latency_p99': 0.0,
            'error_rate': 0.0,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if not self.analytics_engines:
            return summary
            
        total_messages = 0
        total_failed = 0
        total_throughput = 0.0
        max_p99 = 0.0
        
        for analytics in self.analytics_engines.values():
            metrics_data = await analytics.get_real_time_metrics()
            metrics = metrics_data.get('metrics', {})
            
            total_messages += metrics.get('total_messages', 0)
            total_failed += metrics.get('failed_messages', 0)
            total_throughput += metrics.get('messages_per_second', 0.0)
            max_p99 = max(max_p99, metrics.get('p99_latency_ms', 0.0))
            
        summary.update({
            'total_messages_processed': total_messages,
            'total_messages_failed': total_failed,
            'avg_messages_per_second': round(total_throughput, 2),
            'max_latency_p99': round(max_p99, 2),
            'error_rate': round((total_failed / total_messages * 100) if total_messages > 0 else 0.0, 2)
        })
        
        return summary
        
    async def generate_report(self, duration_hours: int = 1) -> Dict[str, Any]:
        """Generate analytics report for specified duration"""
        report = {
            'dashboard_id': self.dashboard_id,
            'report_period_hours': duration_hours,
            'generated_at': datetime.utcnow().isoformat(),
            'engines': {},
            'summary': await self.get_metrics_summary()
        }
        
        # Get detailed data for each engine
        for engine_id, analytics in self.analytics_engines.items():
            engine_report = {
                'engine_id': engine_id,
                'metrics': await analytics.get_real_time_metrics(),
                'time_series': {
                    'messages_per_minute': await analytics.get_time_series_data('messages_per_minute', duration_hours * 60),
                    'avg_latency': await analytics.get_time_series_data('avg_latency', duration_hours * 60)
                }
            }
            report['engines'][engine_id] = engine_report
            
        return report

# Demo analytics setup for LaykHaus
async def create_demo_analytics() -> Dict[str, Any]:
    """Create demo analytics setup for LaykHaus"""
    
    # Create analytics engines
    streaming_analytics = RealTimeAnalytics("streaming_analytics", {
        'analytics_interval': 2
    })
    
    batch_analytics = RealTimeAnalytics("batch_analytics", {
        'analytics_interval': 3
    })
    
    federation_analytics = RealTimeAnalytics("federation_analytics", {
        'analytics_interval': 1
    })
    
    # Create dashboard
    dashboard = LiveDashboard("laykhaus_dashboard", {
        'refresh_interval': 5
    })
    
    # Register analytics engines with dashboard
    await dashboard.register_analytics_engine(streaming_analytics)
    await dashboard.register_analytics_engine(batch_analytics)
    await dashboard.register_analytics_engine(federation_analytics)
    
    # Start everything
    await streaming_analytics.start()
    await batch_analytics.start()
    await federation_analytics.start()
    await dashboard.start()
    
    # Simulate some demo data
    await streaming_analytics.record_message_processed(1024, 15.5)
    await streaming_analytics.record_message_processed(2048, 22.1)
    await batch_analytics.record_message_processed(512, 8.3)
    await federation_analytics.record_message_processed(4096, 35.7)
    
    return {
        'analytics_engines': {
            'streaming': streaming_analytics,
            'batch': batch_analytics,
            'federation': federation_analytics
        },
        'dashboard': dashboard
    }