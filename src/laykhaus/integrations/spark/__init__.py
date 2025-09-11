"""
Spark integration for LaykHaus.

Provides Apache Spark execution engine for federated queries.
"""

from .client import SparkExecutionEngine
from .config import SparkConfig

__all__ = ["SparkExecutionEngine", "SparkConfig"]