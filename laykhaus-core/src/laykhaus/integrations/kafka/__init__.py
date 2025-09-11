"""
Kafka integration for LaykHaus.

Provides Kafka connection and streaming data access capabilities.
"""

from .client import KafkaConnector

__all__ = ["KafkaConnector"]