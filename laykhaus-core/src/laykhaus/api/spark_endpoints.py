"""
Spark-powered API endpoints for federated query execution.
These endpoints leverage Apache Spark as the internal execution engine.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, List, Any, Optional
import logging
import time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/spark", tags=["spark-federation"])


@router.post("/federated-query")
async def execute_federated_query(request: Dict[str, Any]):
    """
    Execute a federated SQL query using Spark as the internal execution engine.
    
    This endpoint:
    - Parses the SQL query
    - Creates an optimized execution plan
    - Executes across multiple data sources using Spark
    - Returns unified results
    
    Request body:
    {
        "query": "SELECT * FROM postgres.users JOIN kafka.events ON ...",
        "max_rows": 10000,
        "explain": false
    }
    """
    try:
        query = request.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="Query is required")
        
        max_rows = request.get("max_rows", 10000)
        explain_only = request.get("explain", False)
        
        # Get Spark federated executor from app state
        from laykhaus.main import app
        executor = app.state.spark_federated_executor
        
        if explain_only:
            # Just explain the plan without executing
            plan = executor.explain_federation_plan(query)
            return {
                "success": True,
                "execution_plan": plan,
                "message": "Query plan generated (not executed)"
            }
        
        # Execute the federated query with Spark
        context = {"max_rows": max_rows}
        result = executor.execute_federated_query(query, context)
        
        return {
            "success": True,
            "data": result.data,
            "columns": result.columns,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "statistics": result.statistics,
            "message": f"Query executed using Spark engine in {result.execution_time_ms:.2f}ms"
        }
        
    except Exception as e:
        logger.error(f"Spark federated query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/streaming-federation")
async def execute_streaming_federation(request: Dict[str, Any]):
    """
    Execute federated query combining streaming and batch sources.
    This unifies real-time streams (Kafka) with batch data using Spark.
    
    Request body:
    {
        "stream_sql": "SELECT * FROM kafka.events WHERE ...",
        "batch_sources": {
            "users": "SELECT * FROM postgres.users",
            "products": "SELECT * FROM mysql.products"
        },
        "output_config": {
            "format": "console",
            "mode": "append",
            "trigger": "10 seconds"
        }
    }
    """
    try:
        stream_sql = request.get("stream_sql")
        batch_sources = request.get("batch_sources", {})
        output_config = request.get("output_config", {})
        
        if not stream_sql:
            raise HTTPException(status_code=400, detail="stream_sql is required")
        
        # Get Spark federated executor
        from laykhaus.main import app
        executor = app.state.spark_federated_executor
        
        # Execute streaming federation
        query_handle = executor.execute_streaming_federation(
            stream_sql=stream_sql,
            batch_sources=batch_sources,
            output_config=output_config
        )
        
        return {
            "success": True,
            "query_id": query_handle.id if hasattr(query_handle, 'id') else "streaming_query",
            "status": "running",
            "message": "Streaming federation query started with Spark"
        }
        
    except Exception as e:
        logger.error(f"Streaming federation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml-enriched-query")
async def execute_ml_enriched_query(request: Dict[str, Any]):
    """
    Execute federated query with ML model enrichment.
    Applies machine learning models to query results using Spark MLlib.
    
    Request body:
    {
        "base_sql": "SELECT * FROM data_source WHERE ...",
        "ml_model_path": "/models/customer_churn_model",
        "features": ["age", "usage", "complaints"],
        "prediction_col": "churn_probability"
    }
    """
    try:
        base_sql = request.get("base_sql")
        ml_model_path = request.get("ml_model_path")
        features = request.get("features", [])
        prediction_col = request.get("prediction_col", "prediction")
        
        if not base_sql:
            raise HTTPException(status_code=400, detail="base_sql is required")
        
        # Get Spark federated executor
        from laykhaus.main import app
        executor = app.state.spark_federated_executor
        
        # Execute ML-enriched query
        result = executor.execute_ml_enrichment(
            base_sql=base_sql,
            ml_model_path=ml_model_path or "/tmp/default_model",
            features=features,
            prediction_col=prediction_col
        )
        
        return {
            "success": True,
            "data": result.data,
            "columns": result.columns,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "ml_enriched": True,
            "message": f"ML-enriched query executed in {result.execution_time_ms:.2f}ms"
        }
        
    except Exception as e:
        logger.error(f"ML enrichment failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/federation-statistics")
async def get_federation_statistics():
    """
    Get statistics about the Spark federation engine performance.
    """
    try:
        from laykhaus.main import app
        executor = app.state.spark_federated_executor
        
        stats = executor.get_federation_statistics()
        
        return {
            "success": True,
            "statistics": stats,
            "message": "Spark federation engine statistics retrieved"
        }
        
    except Exception as e:
        logger.error(f"Failed to get federation statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize-query")
async def optimize_federated_query(request: Dict[str, Any]):
    """
    Optimize a federated query plan without executing it.
    Shows optimizations like predicate pushdown, join reordering, etc.
    
    Request body:
    {
        "query": "SELECT * FROM source1 JOIN source2 ON ..."
    }
    """
    try:
        query = request.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="Query is required")
        
        from laykhaus.main import app
        executor = app.state.spark_federated_executor
        
        # Get optimized plan
        plan = executor.explain_federation_plan(query)
        
        return {
            "success": True,
            "original_sql": plan["original_sql"],
            "optimized_plan": plan["optimized_plan"],
            "optimizations_applied": plan["optimizations_applied"],
            "estimated_cost": plan["estimated_cost"],
            "pushdown_predicates": plan["pushdown_predicates"],
            "join_order": plan["join_order"],
            "data_sources": plan["data_sources"],
            "message": "Query plan optimized using Spark Catalyst optimizer"
        }
        
    except Exception as e:
        logger.error(f"Query optimization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch-federation")
async def execute_batch_federation(
    background_tasks: BackgroundTasks,
    request: Dict[str, Any]
):
    """
    Execute large batch federated queries asynchronously.
    Uses Spark's distributed processing for handling large datasets.
    
    Request body:
    {
        "queries": [
            "SELECT * FROM source1 WHERE date > '2024-01-01'",
            "SELECT * FROM source2 JOIN source3 ON ..."
        ],
        "output_path": "/data/results/batch_output",
        "format": "parquet"
    }
    """
    try:
        queries = request.get("queries", [])
        output_path = request.get("output_path")
        output_format = request.get("format", "parquet")
        
        if not queries:
            raise HTTPException(status_code=400, detail="At least one query is required")
        
        # Schedule batch execution in background
        job_id = f"batch_{int(time.time())}"
        
        background_tasks.add_task(
            _execute_batch_queries,
            job_id=job_id,
            queries=queries,
            output_path=output_path,
            output_format=output_format
        )
        
        return {
            "success": True,
            "job_id": job_id,
            "status": "submitted",
            "query_count": len(queries),
            "message": f"Batch federation job {job_id} submitted to Spark"
        }
        
    except Exception as e:
        logger.error(f"Batch federation submission failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _execute_batch_queries(
    job_id: str,
    queries: List[str],
    output_path: Optional[str],
    output_format: str
):
    """Background task to execute batch queries."""
    try:
        from laykhaus.main import app
        executor = app.state.spark_federated_executor
        
        results = []
        for query in queries:
            result = executor.execute_federated_query(query)
            results.append(result)
        
        # Save results if output path provided
        if output_path:
            # In production, would save to specified format
            logger.info(f"Batch job {job_id} completed, {len(results)} queries processed")
        
    except Exception as e:
        logger.error(f"Batch job {job_id} failed: {e}")


@router.get("/health")
async def spark_federation_health():
    """
    Check health of Spark federation engine.
    """
    try:
        from laykhaus.main import app
        
        if not hasattr(app.state, 'spark_federated_executor'):
            return {
                "status": "unavailable",
                "error": "Spark federated executor not initialized"
            }
        
        executor = app.state.spark_federated_executor
        stats = executor.get_federation_statistics()
        
        return {
            "status": "healthy",
            "spark_version": stats.get("spark_version"),
            "federation_enabled": stats.get("federation_enabled"),
            "optimization_rules": stats.get("optimization_rules"),
            "message": "Spark federation engine is operational"
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }