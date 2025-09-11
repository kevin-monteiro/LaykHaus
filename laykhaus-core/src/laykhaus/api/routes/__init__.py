"""
API route modules.
"""

from .connectors import router as connectors_router
from .query import router as query_router
from .health import router as health_router
from .spark import router as spark_router

__all__ = ["connectors_router", "query_router", "health_router", "spark_router"]