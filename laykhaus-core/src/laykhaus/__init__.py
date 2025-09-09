"""
LaykHaus - Federated Data Lakehouse Platform

A unified data virtualization platform providing SQL and GraphQL interfaces
to heterogeneous data sources without data movement.
"""

__version__ = "0.1.0"
__author__ = "LaykHaus Team"

from laykhaus.core.config import settings
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)

__all__ = ["settings", "logger", "__version__"]