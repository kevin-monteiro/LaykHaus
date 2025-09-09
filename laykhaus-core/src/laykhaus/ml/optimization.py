"""
Performance optimization using machine learning for LaykHaus.

Provides query optimization and performance prediction capabilities.
"""

import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple, Union
from enum import Enum
import hashlib

from laykhaus.core.logging import get_logger
from .framework import MLModel, ModelType

logger = get_logger(__name__)


class QueryType(Enum):
    """Types of database queries."""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    JOIN = "join"
    AGGREGATE = "aggregate"
    COMPLEX = "complex"


@dataclass
class QueryProfile:
    """Profile of a database query for optimization."""
    query_hash: str
    query_type: QueryType
    table_count: int
    column_count: int
    join_count: int
    where_clauses: int
    aggregate_functions: int
    subquery_count: int
    data_size_estimate: int
    historical_performance: List[float] = field(default_factory=list)
    
    def to_features(self) -> Dict[str, float]:
        """Convert to feature vector for ML model."""
        avg_performance = np.mean(self.historical_performance) if self.historical_performance else 0.0
        
        return {
            "table_count": float(self.table_count),
            "column_count": float(self.column_count),
            "join_count": float(self.join_count),
            "where_clauses": float(self.where_clauses),
            "aggregate_functions": float(self.aggregate_functions),
            "subquery_count": float(self.subquery_count),
            "data_size_estimate": float(self.data_size_estimate),
            "query_type_select": 1.0 if self.query_type == QueryType.SELECT else 0.0,
            "query_type_join": 1.0 if self.query_type == QueryType.JOIN else 0.0,
            "query_type_aggregate": 1.0 if self.query_type == QueryType.AGGREGATE else 0.0,
            "query_type_complex": 1.0 if self.query_type == QueryType.COMPLEX else 0.0,
            "avg_historical_performance": avg_performance
        }


@dataclass
class OptimizationSuggestion:
    """Query optimization suggestion."""
    suggestion_type: str
    description: str
    estimated_improvement: float  # Percentage improvement
    confidence: float
    implementation_effort: str  # "low", "medium", "high"
    sql_hint: Optional[str] = None


@dataclass
class PerformancePrediction:
    """Performance prediction for a query."""
    predicted_execution_time: float  # seconds
    confidence_interval: Tuple[float, float]
    resource_usage: Dict[str, float]
    bottlenecks: List[str]
    suggestions: List[OptimizationSuggestion]


class SimplePerformanceModel(MLModel):
    """Simple performance prediction model using linear regression."""
    
    def __init__(self):
        """Initialize performance model."""
        self.coefficients = {}
        self.intercept = 0.0
        self.feature_names = []
        self.trained = False
        
    def train(self, data: pd.DataFrame, target: Optional[str] = None) -> None:
        """Train the performance model."""
        if target is None or target not in data.columns:
            raise ValueError("Target column for performance (execution time) must be specified")
        
        # Separate features and target
        X = data.drop(columns=[target])
        y = data[target]
        
        self.feature_names = list(X.columns)
        
        # Simple linear regression using normal equation
        # Add bias term
        X_with_bias = np.c_[np.ones(X.shape[0]), X.values]
        
        try:
            # Normal equation: theta = (X^T * X)^-1 * X^T * y
            XtX = X_with_bias.T @ X_with_bias
            XtX_inv = np.linalg.inv(XtX + 1e-8 * np.eye(XtX.shape[0]))  # Ridge regularization
            theta = XtX_inv @ X_with_bias.T @ y.values
            
            self.intercept = theta[0]
            self.coefficients = {name: theta[i+1] for i, name in enumerate(self.feature_names)}
            self.trained = True
            
        except np.linalg.LinAlgError:
            # Fallback to mean prediction
            self.intercept = float(np.mean(y))
            self.coefficients = {name: 0.0 for name in self.feature_names}
            self.trained = True
    
    def predict(self, data: pd.DataFrame) -> Union[np.ndarray, pd.Series]:
        """Predict performance for new queries."""
        if not self.trained:
            raise ValueError("Model not trained")
        
        predictions = []
        
        for _, row in data.iterrows():
            prediction = self.intercept
            
            for feature_name in self.feature_names:
                if feature_name in row:
                    prediction += self.coefficients[feature_name] * row[feature_name]
            
            # Ensure positive prediction (execution time can't be negative)
            predictions.append(max(prediction, 0.001))
        
        return np.array(predictions)
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance based on coefficient magnitudes."""
        if not self.trained:
            return {}
        
        total_importance = sum(abs(coef) for coef in self.coefficients.values())
        if total_importance == 0:
            return {name: 0.0 for name in self.feature_names}
        
        return {
            name: abs(coef) / total_importance 
            for name, coef in self.coefficients.items()
        }


class QueryProfiler:
    """Profiles SQL queries for optimization."""
    
    def __init__(self):
        """Initialize query profiler."""
        self.logger = get_logger(__name__)
        self.query_profiles = {}
    
    def profile_query(self, query: str, execution_time: Optional[float] = None) -> QueryProfile:
        """Create profile for a SQL query."""
        try:
            query_hash = hashlib.md5(query.encode()).hexdigest()
            
            # Simple SQL parsing (would use proper SQL parser in production)
            query_lower = query.lower().strip()
            
            # Determine query type
            if query_lower.startswith("select"):
                if "join" in query_lower:
                    query_type = QueryType.JOIN
                elif any(agg in query_lower for agg in ["count(", "sum(", "avg(", "max(", "min("]):
                    query_type = QueryType.AGGREGATE
                elif query_lower.count("select") > 1:
                    query_type = QueryType.COMPLEX
                else:
                    query_type = QueryType.SELECT
            elif query_lower.startswith("insert"):
                query_type = QueryType.INSERT
            elif query_lower.startswith("update"):
                query_type = QueryType.UPDATE
            elif query_lower.startswith("delete"):
                query_type = QueryType.DELETE
            else:
                query_type = QueryType.COMPLEX
            
            # Count query components (simple heuristics)
            table_count = query_lower.count("from ") + query_lower.count("join ")
            column_count = query_lower.count(",") + 1 if "select" in query_lower else 0
            join_count = query_lower.count("join ")
            where_clauses = query_lower.count("where ") + query_lower.count("and ") + query_lower.count("or ")
            aggregate_functions = sum(query_lower.count(f"{agg}(") for agg in ["count", "sum", "avg", "max", "min"])
            subquery_count = query_lower.count("select") - 1  # Subtract main query
            
            # Estimate data size (very rough heuristic)
            data_size_estimate = max(1000, table_count * 10000)  # Base estimate
            
            # Check if we have existing profile
            if query_hash in self.query_profiles:
                profile = self.query_profiles[query_hash]
                if execution_time is not None:
                    profile.historical_performance.append(execution_time)
            else:
                profile = QueryProfile(
                    query_hash=query_hash,
                    query_type=query_type,
                    table_count=table_count,
                    column_count=column_count,
                    join_count=join_count,
                    where_clauses=where_clauses,
                    aggregate_functions=aggregate_functions,
                    subquery_count=subquery_count,
                    data_size_estimate=data_size_estimate,
                    historical_performance=[execution_time] if execution_time is not None else []
                )
                self.query_profiles[query_hash] = profile
            
            return profile
            
        except Exception as e:
            self.logger.error(f"Query profiling failed: {e}")
            # Return default profile
            return QueryProfile(
                query_hash="unknown",
                query_type=QueryType.SELECT,
                table_count=1,
                column_count=1,
                join_count=0,
                where_clauses=0,
                aggregate_functions=0,
                subquery_count=0,
                data_size_estimate=1000
            )
    
    def get_profile_features(self, query_hash: str) -> Optional[Dict[str, float]]:
        """Get feature vector for a query profile."""
        if query_hash in self.query_profiles:
            return self.query_profiles[query_hash].to_features()
        return None
    
    def get_all_profiles(self) -> Dict[str, QueryProfile]:
        """Get all stored query profiles."""
        return self.query_profiles.copy()


class QueryOptimizer:
    """ML-based query optimizer."""
    
    def __init__(self):
        """Initialize query optimizer."""
        self.logger = get_logger(__name__)
        self.profiler = QueryProfiler()
        self.performance_model = None
        self.optimization_rules = self._init_optimization_rules()
    
    def _init_optimization_rules(self) -> Dict[str, Any]:
        """Initialize rule-based optimization suggestions."""
        return {
            "join_optimization": {
                "condition": lambda profile: profile.join_count > 2,
                "suggestion": "Consider using indexed joins and optimal join order",
                "improvement": 30.0,
                "effort": "medium"
            },
            "where_clause_optimization": {
                "condition": lambda profile: profile.where_clauses > 5,
                "suggestion": "Optimize WHERE clauses and consider composite indexes",
                "improvement": 20.0,
                "effort": "low"
            },
            "aggregate_optimization": {
                "condition": lambda profile: profile.aggregate_functions > 3,
                "suggestion": "Consider using materialized views for complex aggregations",
                "improvement": 40.0,
                "effort": "high"
            },
            "subquery_optimization": {
                "condition": lambda profile: profile.subquery_count > 1,
                "suggestion": "Convert correlated subqueries to JOINs when possible",
                "improvement": 25.0,
                "effort": "medium"
            }
        }
    
    def train_performance_model(self, query_data: List[Tuple[str, float]]) -> bool:
        """Train the performance prediction model."""
        try:
            if len(query_data) < 10:
                self.logger.warning("Insufficient data for training performance model")
                return False
            
            # Create profiles and features
            features_list = []
            execution_times = []
            
            for query, exec_time in query_data:
                profile = self.profiler.profile_query(query, exec_time)
                features = profile.to_features()
                
                features_list.append(features)
                execution_times.append(exec_time)
            
            # Convert to DataFrame
            df = pd.DataFrame(features_list)
            df['execution_time'] = execution_times
            
            # Train model
            self.performance_model = SimplePerformanceModel()
            self.performance_model.train(df, target='execution_time')
            
            self.logger.info("Performance model trained successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to train performance model: {e}")
            return False
    
    def predict_performance(self, query: str) -> PerformancePrediction:
        """Predict query performance and provide optimization suggestions."""
        try:
            profile = self.profiler.profile_query(query)
            
            # Get performance prediction
            if self.performance_model:
                features_df = pd.DataFrame([profile.to_features()])
                predicted_time = self.performance_model.predict(features_df)[0]
                
                # Simple confidence interval (±20%)
                confidence_lower = predicted_time * 0.8
                confidence_upper = predicted_time * 1.2
                confidence_interval = (confidence_lower, confidence_upper)
                
            else:
                # Fallback heuristic-based prediction
                base_time = 0.1  # 100ms base
                complexity_factor = (
                    profile.table_count * 0.05 +
                    profile.join_count * 0.1 +
                    profile.where_clauses * 0.02 +
                    profile.aggregate_functions * 0.08 +
                    profile.subquery_count * 0.15
                )
                predicted_time = base_time * (1 + complexity_factor)
                confidence_interval = (predicted_time * 0.5, predicted_time * 2.0)
            
            # Generate optimization suggestions
            suggestions = []
            for rule_name, rule in self.optimization_rules.items():
                if rule["condition"](profile):
                    suggestions.append(OptimizationSuggestion(
                        suggestion_type=rule_name,
                        description=rule["suggestion"],
                        estimated_improvement=rule["improvement"],
                        confidence=0.7,  # Default confidence
                        implementation_effort=rule["effort"]
                    ))
            
            # Identify potential bottlenecks
            bottlenecks = []
            if profile.join_count > 3:
                bottlenecks.append("Multiple JOINs may cause performance issues")
            if profile.data_size_estimate > 100000:
                bottlenecks.append("Large dataset may require optimization")
            if profile.subquery_count > 2:
                bottlenecks.append("Complex subqueries detected")
            
            # Estimate resource usage
            resource_usage = {
                "cpu_utilization": min(0.1 + profile.table_count * 0.1 + profile.join_count * 0.15, 1.0),
                "memory_usage": min(0.05 + profile.data_size_estimate / 1000000, 1.0),
                "io_operations": min(0.1 + profile.table_count * 0.2, 1.0)
            }
            
            return PerformancePrediction(
                predicted_execution_time=predicted_time,
                confidence_interval=confidence_interval,
                resource_usage=resource_usage,
                bottlenecks=bottlenecks,
                suggestions=suggestions
            )
            
        except Exception as e:
            self.logger.error(f"Performance prediction failed: {e}")
            return PerformancePrediction(
                predicted_execution_time=1.0,
                confidence_interval=(0.5, 2.0),
                resource_usage={"cpu_utilization": 0.1, "memory_usage": 0.1, "io_operations": 0.1},
                bottlenecks=["Unable to analyze query"],
                suggestions=[]
            )
    
    def optimize_query(self, query: str) -> Dict[str, Any]:
        """Provide comprehensive query optimization analysis."""
        try:
            profile = self.profiler.profile_query(query)
            prediction = self.predict_performance(query)
            
            # Calculate optimization score (0-100, higher is better)
            optimization_score = max(0, 100 - len(prediction.suggestions) * 15 - len(prediction.bottlenecks) * 10)
            
            return {
                "query_hash": profile.query_hash,
                "query_type": profile.query_type.value,
                "complexity_metrics": {
                    "table_count": profile.table_count,
                    "join_count": profile.join_count,
                    "where_clauses": profile.where_clauses,
                    "aggregate_functions": profile.aggregate_functions,
                    "subquery_count": profile.subquery_count
                },
                "performance_prediction": {
                    "estimated_time": prediction.predicted_execution_time,
                    "confidence_range": prediction.confidence_interval,
                    "resource_usage": prediction.resource_usage
                },
                "optimization_score": optimization_score,
                "bottlenecks": prediction.bottlenecks,
                "suggestions": [
                    {
                        "type": s.suggestion_type,
                        "description": s.description,
                        "improvement": s.estimated_improvement,
                        "effort": s.implementation_effort,
                        "confidence": s.confidence
                    }
                    for s in prediction.suggestions
                ],
                "historical_performance": profile.historical_performance
            }
            
        except Exception as e:
            self.logger.error(f"Query optimization analysis failed: {e}")
            return {"error": str(e)}


class PerformancePredictor:
    """System performance prediction and monitoring."""
    
    def __init__(self):
        """Initialize performance predictor."""
        self.logger = get_logger(__name__)
        self.query_optimizer = QueryOptimizer()
        self.system_metrics = {
            "query_count": 0,
            "total_execution_time": 0.0,
            "average_execution_time": 0.0,
            "slow_queries": 0,
            "optimization_suggestions_given": 0
        }
    
    def record_query_execution(self, query: str, execution_time: float) -> None:
        """Record query execution for learning."""
        try:
            self.query_optimizer.profiler.profile_query(query, execution_time)
            
            # Update system metrics
            self.system_metrics["query_count"] += 1
            self.system_metrics["total_execution_time"] += execution_time
            self.system_metrics["average_execution_time"] = (
                self.system_metrics["total_execution_time"] / self.system_metrics["query_count"]
            )
            
            if execution_time > 2.0:  # Slow query threshold
                self.system_metrics["slow_queries"] += 1
            
        except Exception as e:
            self.logger.error(f"Failed to record query execution: {e}")
    
    def predict_system_load(self, time_horizon_hours: int = 24) -> Dict[str, Any]:
        """Predict system load for the next time period."""
        try:
            current_avg_time = self.system_metrics["average_execution_time"]
            query_rate = self.system_metrics["query_count"] / max(1, time_horizon_hours)  # Simplified
            
            # Simple load prediction
            predicted_queries = int(query_rate * time_horizon_hours)
            predicted_total_time = predicted_queries * current_avg_time
            
            # Load classification
            if predicted_total_time < 3600:  # 1 hour of compute
                load_level = "low"
            elif predicted_total_time < 7200:  # 2 hours of compute
                load_level = "medium"
            else:
                load_level = "high"
            
            return {
                "time_horizon_hours": time_horizon_hours,
                "predicted_queries": predicted_queries,
                "predicted_total_execution_time": predicted_total_time,
                "predicted_average_time": current_avg_time,
                "load_level": load_level,
                "recommendations": self._get_load_recommendations(load_level)
            }
            
        except Exception as e:
            self.logger.error(f"System load prediction failed: {e}")
            return {"error": str(e)}
    
    def _get_load_recommendations(self, load_level: str) -> List[str]:
        """Get recommendations based on predicted load."""
        if load_level == "high":
            return [
                "Consider scaling up compute resources",
                "Review and optimize slow queries",
                "Implement query result caching",
                "Consider read replicas for query distribution"
            ]
        elif load_level == "medium":
            return [
                "Monitor system performance closely",
                "Optimize queries with suggestions",
                "Consider connection pooling optimization"
            ]
        else:
            return [
                "System load is optimal",
                "Continue monitoring query performance"
            ]
    
    def get_performance_insights(self) -> Dict[str, Any]:
        """Get comprehensive performance insights."""
        profiles = self.query_optimizer.profiler.get_all_profiles()
        
        # Analyze query patterns
        query_types = {}
        for profile in profiles.values():
            query_type = profile.query_type.value
            query_types[query_type] = query_types.get(query_type, 0) + 1
        
        # Find most complex queries
        complex_queries = [
            {
                "hash": profile.query_hash,
                "type": profile.query_type.value,
                "complexity_score": (
                    profile.table_count +
                    profile.join_count * 2 +
                    profile.subquery_count * 3 +
                    profile.aggregate_functions
                ),
                "avg_performance": np.mean(profile.historical_performance) if profile.historical_performance else 0
            }
            for profile in profiles.values()
        ]
        complex_queries.sort(key=lambda x: x["complexity_score"], reverse=True)
        
        return {
            "system_metrics": self.system_metrics,
            "query_distribution": query_types,
            "total_unique_queries": len(profiles),
            "most_complex_queries": complex_queries[:5],
            "performance_trends": {
                "slow_query_ratio": self.system_metrics["slow_queries"] / max(1, self.system_metrics["query_count"]),
                "average_execution_time": self.system_metrics["average_execution_time"]
            },
            "optimization_opportunities": len([
                p for p in profiles.values() 
                if p.join_count > 2 or p.subquery_count > 1 or p.aggregate_functions > 2
            ])
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of performance prediction system."""
        return {
            "status": "healthy",
            "components": {
                "query_profiler": "ready",
                "performance_model": "ready" if self.query_optimizer.performance_model else "not_trained",
                "optimization_engine": "ready"
            },
            "metrics": self.system_metrics,
            "capabilities": [
                "query_profiling",
                "performance_prediction", 
                "optimization_suggestions",
                "load_prediction",
                "performance_insights"
            ]
        }