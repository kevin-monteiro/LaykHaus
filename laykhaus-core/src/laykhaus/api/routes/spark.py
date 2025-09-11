"""
Spark diagnostics and monitoring API endpoints.

This module provides Spark-related diagnostic endpoints that proxy Spark UI information
through the LaykHaus Core API, making it accessible to the frontend UI.
"""

from typing import Dict, Any, Optional
from fastapi import APIRouter
import requests

router = APIRouter(tags=["spark"])

def fetch_spark_ui_data(endpoint: str) -> Optional[Dict]:
    """
    Fetch data from Spark UI REST API.
    
    Args:
        endpoint: The Spark UI REST API endpoint
        
    Returns:
        JSON response from Spark UI or None if error
    """
    try:
        response = requests.get(f"http://localhost:4040{endpoint}", timeout=5)
        return response.json() if response.status_code == 200 else None
    except Exception:
        return None

@router.get("/spark/status")
async def get_spark_status():
    """Get Spark application status and cluster information."""
    try:
        apps = fetch_spark_ui_data("/api/v1/applications")
        if not apps:
            return {"status": "unavailable", "message": "Spark UI not accessible"}
        
        app_info = apps[0] if apps else {}
        app_id = app_info.get("id", "")
        
        executors = fetch_spark_ui_data(f"/api/v1/applications/{app_id}/executors") or []
        total_cores = sum(int(e.get("totalCores", 0)) for e in executors)
        active_executors = len([e for e in executors if e.get("isActive", False)])
        
        return {
            "status": "healthy",
            "application": {
                "id": app_id,
                "name": app_info.get("name", "LaykHaus-Federation-Engine"),
                "started_time": app_info.get("attempts", [{}])[0].get("startTime"),
                "spark_version": app_info.get("attempts", [{}])[0].get("sparkVersion")
            },
            "cluster": {
                "total_cores": total_cores,
                "active_executors": active_executors,
                "total_executors": len(executors)
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/spark/jobs")
async def get_spark_jobs():
    """Get recent Spark jobs (federated queries)."""
    try:
        apps = fetch_spark_ui_data("/api/v1/applications")
        if not apps:
            return {"jobs": []}
        
        app_id = apps[0].get("id", "")
        jobs = fetch_spark_ui_data(f"/api/v1/applications/{app_id}/jobs") or []
        
        recent_jobs = []
        for job in jobs[:10]:  # Last 10 jobs
            recent_jobs.append({
                "job_id": job.get("jobId"),
                "name": job.get("name", "Federated Query"),
                "status": job.get("status"),
                "duration": job.get("duration"),
                "completion_time": job.get("completionTime"),
                "num_tasks": job.get("numTasks", 0),
                "num_completed_tasks": job.get("numCompletedTasks", 0),
                "num_failed_tasks": job.get("numFailedTasks", 0)
            })
        
        return {"jobs": recent_jobs, "total": len(jobs)}
    except Exception as e:
        return {"jobs": [], "error": str(e)}

@router.get("/spark/queries")
async def get_spark_queries():
    """Get SQL query execution history."""
    try:
        apps = fetch_spark_ui_data("/api/v1/applications")
        if not apps:
            return {"queries": []}
        
        app_id = apps[0].get("id", "")
        sql_queries = fetch_spark_ui_data(f"/api/v1/applications/{app_id}/sql") or []
        
        recent_queries = []
        for query in sql_queries[:15]:  # Last 15 queries
            recent_queries.append({
                "query_id": query.get("id"),
                "description": query.get("description", ""),
                "status": query.get("status"),
                "duration": query.get("duration"),
                "submission_time": query.get("submissionTime"),
                "completed_jobs": query.get("completedJobs", 0),
                "failed_jobs": query.get("failedJobs", 0),
                "is_federated": any(source in query.get("description", "").lower() 
                                  for source in ["rest_api", "kafka", "postgres"])
            })
        
        return {"queries": recent_queries, "total": len(sql_queries)}
    except Exception as e:
        return {"queries": [], "error": str(e)}

@router.get("/spark/executors")
async def get_spark_executors():
    """Get Spark executor resource information."""
    try:
        apps = fetch_spark_ui_data("/api/v1/applications")
        if not apps:
            return {"executors": []}
        
        app_id = apps[0].get("id", "")
        executors = fetch_spark_ui_data(f"/api/v1/applications/{app_id}/executors") or []
        
        executor_info = []
        for executor in executors:
            executor_info.append({
                "executor_id": executor.get("id"),
                "host_port": executor.get("hostPort"),
                "is_active": executor.get("isActive"),
                "total_cores": executor.get("totalCores"),
                "active_tasks": executor.get("activeTasks"),
                "completed_tasks": executor.get("completedTasks"),
                "failed_tasks": executor.get("failedTasks"),
                "memory_used_mb": executor.get("memoryUsed", 0) // (1024 * 1024),
                "max_memory_mb": executor.get("maxMemory", 0) // (1024 * 1024)
            })
        
        return {"executors": executor_info, "total": len(executors)}
    except Exception as e:
        return {"executors": [], "error": str(e)}