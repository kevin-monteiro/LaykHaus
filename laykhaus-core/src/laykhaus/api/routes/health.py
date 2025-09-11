"""
Health check API endpoints.

This module provides health check and monitoring endpoints for the LaykHaus platform.
These endpoints are essential for:
- Kubernetes health probes and readiness checks
- Load balancer health monitoring
- Application monitoring and alerting systems
- Debugging and troubleshooting

The health checks verify the status of:
- API service availability
- Connector configurations and connections
- Spark federation engine status
- Overall system readiness

Endpoints:
- GET /: Root endpoint with basic service info
- GET /health: Comprehensive health check of all components

Author: LaykHaus Team
"""

from fastapi import APIRouter
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.config import settings

# Create router for health-related endpoints
router = APIRouter(tags=["health"])


@router.get("/")
async def root():
    """
    Root endpoint providing basic service information.
    
    This endpoint serves as a simple check that the API is reachable and provides
    basic metadata about the service. It's useful for quick verification that the
    service is deployed and responding.
    
    Returns:
        dict: Service information containing:
            - name: Application name from configuration
            - version: Current application version
            - status: Always "operational" if responding
            
    Example Response:
        {
            "name": "LaykHaus Core",
            "version": "1.0.0",
            "status": "operational"
        }
    """
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "status": "operational"
    }


@router.get("/health")
async def health_check():
    """
    Comprehensive health check endpoint for all system components.
    
    This endpoint performs health checks on critical system components:
    - API service status (always healthy if responding)
    - Connector availability and count
    - Spark federation engine status
    
    The health check is designed to be lightweight while still providing
    useful diagnostic information about the system state.
    
    Returns:
        dict: Detailed health status containing:
            - status: Overall health status ("healthy" if API is responding)
            - components: Individual component health statuses
                - api: API service status
                - connectors: Connector subsystem status and count
                - spark: Spark engine availability
            - version: Application version
            - environment: Current environment (dev/staging/prod)
            
    Example Response:
        {
            "status": "healthy",
            "components": {
                "api": "healthy",
                "connectors": {
                    "status": "healthy",
                    "count": 3
                },
                "spark": "healthy"
            },
            "version": "1.0.0",
            "environment": "development"
        }
        
    Note:
        The Spark status check is wrapped in try/except to handle cases where
        Spark might not be initialized or available. This prevents the health
        check from failing if Spark is temporarily unavailable.
    """
    # Check how many connectors are configured
    connector_count = len(connection_manager.list_connectors())
    
    # Check Spark federation engine status
    # We wrap this in try/except as Spark might not always be available
    spark_status = "unknown"
    try:
        # Try to access the Spark executor from app state
        # Note: This import path needs to be updated to use app.py instead of main.py
        from laykhaus.app import app
        if hasattr(app.state, 'spark_federated_executor'):
            executor = app.state.spark_federated_executor
            if executor and hasattr(executor, 'spark_engine'):
                # Check if Spark session exists
                spark_status = "healthy" if executor.spark_engine.spark else "unavailable"
    except Exception as e:
        # If any error occurs, mark Spark as error state
        # This could happen during startup or if Spark fails
        spark_status = "error"
    
    return {
        "status": "healthy",  # API is healthy if it can respond
        "components": {
            "api": "healthy",  # API component is always healthy if responding
            "connectors": {
                "status": "healthy" if connector_count > 0 else "no_connectors",
                "count": connector_count
            },
            "spark": spark_status
        },
        "version": settings.app_version,
        "environment": settings.environment
    }