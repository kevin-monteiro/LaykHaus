"""
Query execution API endpoints.
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1", tags=["query"])


class QueryRequest(BaseModel):
    """Request model for query execution."""
    query: str
    target_format: str = "json"
    max_rows: Optional[int] = 10000


@router.post("/query")
async def execute_federated_query(query_request: QueryRequest, request: Request):
    """
    Execute a federated SQL query across multiple data sources.
    
    This is the main query endpoint that uses Spark to federate queries
    across PostgreSQL, Kafka, and REST API sources.
    """
    try:
        # Get the Spark federated executor from app state
        app = request.app
        
        if not hasattr(app.state, 'federation_executor'):
            raise HTTPException(
                status_code=503,
                detail="Federation engine not initialized"
            )
        
        executor = app.state.federation_executor
        
        # Execute the federated query
        logger.info(f"Executing federated query: {query_request.query[:100]}...")
        
        result = executor.execute_federated_query(query_request.query)
        
        # Format response based on target format
        if query_request.target_format == "json":
            return {
                "success": True,
                "data": result.data[:query_request.max_rows] if query_request.max_rows else result.data,
                "columns": result.columns,
                "schema": result.columns,  # Keep for backwards compatibility
                "row_count": min(result.row_count, query_request.max_rows) if query_request.max_rows else result.row_count,
                "execution_time_ms": result.execution_time_ms,
                "metadata": {
                    "execution_plan": result.execution_plan,
                    "statistics": result.statistics
                }
            }
        else:
            # Could support other formats like CSV, Parquet, etc.
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported target format: {query_request.target_format}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))