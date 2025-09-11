"""
External integrations layer for LaykHaus.

This module isolates all external system dependencies (Spark, Kafka, PostgreSQL, REST)
from the core business logic, providing clean interfaces and easy testing.
"""

__all__ = ["spark", "kafka", "postgres", "rest"]