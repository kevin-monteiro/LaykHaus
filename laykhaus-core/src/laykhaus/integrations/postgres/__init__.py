"""
PostgreSQL integration for LaykHaus.

Provides PostgreSQL connection and query execution capabilities.
"""

from .client import PostgreSQLConnector

__all__ = ["PostgreSQLConnector"]