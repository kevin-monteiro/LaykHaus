"""
LaykHaus Application Entry Point.

This module serves as the main entry point for the LaykHaus federated data lakehouse platform.
It initializes the FastAPI application, configures middleware, sets up the Spark federation
engine, and manages the application lifecycle including startup and shutdown operations.

The application provides REST API endpoints for:
- Connector management (PostgreSQL, Kafka, REST API)
- Federated query execution across heterogeneous data sources
- Health monitoring and statistics

Key Components:
- FastAPI application with CORS middleware
- Spark-based federation engine for distributed query processing
- Connection manager for handling multiple data source connections
- Modular API routes for different functionalities

Author: LaykHaus Team
Version: 1.0.0
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from laykhaus.api.routes import connectors_router, query_router, health_router
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.config import settings
from laykhaus.core.logging import get_logger
from laykhaus.federation.spark_federated_executor import SparkFederatedExecutor

# Initialize logger for this module
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager for handling startup and shutdown operations.
    
    This context manager ensures proper initialization and cleanup of resources:
    - Initializes the connection manager and loads saved connectors
    - Creates and configures the Spark federation engine
    - Handles graceful shutdown of all connections and Spark resources
    
    Args:
        app: FastAPI application instance to configure
        
    Yields:
        Control back to FastAPI after startup operations
        
    Raises:
        Exception: If startup fails, preventing the application from starting
    """
    # ========== STARTUP OPERATIONS ==========
    logger.info("Starting LaykHaus platform")
    
    try:
        # Initialize connection manager which handles all data source connections
        # This loads any previously saved connectors from persistence
        await connection_manager.initialize()
        logger.info("Connection manager initialized")
        
        # Initialize Spark-based federation engine for distributed query processing
        # This creates a SparkSession and prepares it for federated queries
        app.state.spark_federated_executor = SparkFederatedExecutor()
        app.state.federation_executor = app.state.spark_federated_executor
        logger.info("Spark-based federation engine initialized")
        
        logger.info("LaykHaus platform started successfully")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    # Yield control back to FastAPI
    yield
    
    # ========== SHUTDOWN OPERATIONS ==========
    logger.info("Shutting down LaykHaus platform")
    
    try:
        # Stop Spark engine to release cluster resources
        if hasattr(app.state, 'spark_federated_executor'):
            app.state.spark_federated_executor.stop()
            logger.info("Spark engine stopped")
        
        # Shutdown connection manager which disconnects all data sources
        await connection_manager.shutdown()
        logger.info("Connection manager shut down")
        
    except Exception as e:
        logger.error(f"Shutdown error: {e}")
    
    logger.info("LaykHaus platform shut down complete")


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application instance.
    
    This factory function creates a new FastAPI application with:
    - Configured metadata (title, version, description)
    - CORS middleware for cross-origin requests
    - Lifecycle management for startup/shutdown
    - All API route modules included
    
    Returns:
        FastAPI: Configured FastAPI application instance ready to run
        
    Note:
        CORS is currently configured to allow all origins (*) for development.
        This should be restricted in production environments.
    """
    # Create FastAPI instance with metadata
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="Federated Data Lakehouse Platform",
        lifespan=lifespan  # Attach lifecycle manager
    )
    
    # Configure CORS middleware for cross-origin requests
    # TODO: Restrict origins in production
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Allow all origins (development only)
        allow_credentials=True,
        allow_methods=["*"],  # Allow all HTTP methods
        allow_headers=["*"],  # Allow all headers
    )
    
    # Include API route modules
    # Each router handles a specific domain of functionality
    app.include_router(health_router)      # Health checks and monitoring
    app.include_router(connectors_router)  # Connector CRUD operations
    app.include_router(query_router)       # Federated query execution
    
    return app


# Create the application instance
# This is what uvicorn will import and run
app = create_app()


if __name__ == "__main__":
    """
    Development server entry point.
    
    This allows running the application directly with Python for development.
    In production, use uvicorn directly or a production ASGI server.
    
    Example:
        python app.py
    """
    import uvicorn
    uvicorn.run(
        "laykhaus.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True  # Enable auto-reload for development
    )