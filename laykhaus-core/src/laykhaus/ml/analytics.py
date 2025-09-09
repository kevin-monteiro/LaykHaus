"""
Advanced Analytics capabilities for LaykHaus.

Provides time series analysis, anomaly detection, and statistical analytics.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple, Union
from enum import Enum
import warnings

from laykhaus.core.logging import get_logger
from .framework import MLModel, ModelType, ModelMetadata

logger = get_logger(__name__)


class AnomalyType(Enum):
    """Types of anomalies that can be detected."""
    POINT = "point"          # Single data point anomaly
    CONTEXTUAL = "contextual"  # Anomaly in specific context
    COLLECTIVE = "collective"  # Group of points forming anomaly


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    timestamp: datetime
    value: float
    anomaly_score: float
    anomaly_type: AnomalyType
    confidence: float
    context: Dict[str, Any]


@dataclass
class TimeSeriesForecast:
    """Time series forecast result."""
    timestamps: List[datetime]
    values: List[float]
    confidence_lower: List[float]
    confidence_upper: List[float]
    model_info: Dict[str, Any]


class SimpleTimeSeriesModel(MLModel):
    """Simple time series model using moving averages and trend analysis."""
    
    def __init__(self, window_size: int = 10, trend_window: int = 30):
        """Initialize simple time series model."""
        self.window_size = window_size
        self.trend_window = trend_window
        self.trained_data = None
        self.trend_slope = 0.0
        self.seasonal_pattern = None
        self.mean_value = 0.0
        self.std_value = 1.0
        
    def train(self, data: pd.DataFrame, target: Optional[str] = None) -> None:
        """Train the time series model."""
        if target and target in data.columns:
            values = data[target].values
        else:
            # Use first numeric column
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                raise ValueError("No numeric columns found for time series training")
            values = data[numeric_cols[0]].values
        
        self.trained_data = values
        self.mean_value = np.mean(values)
        self.std_value = np.std(values)
        
        # Calculate trend
        if len(values) >= self.trend_window:
            x = np.arange(len(values))
            self.trend_slope = np.polyfit(x, values, 1)[0]
        
        # Simple seasonal pattern (daily pattern assumption)
        if len(values) >= 24:  # Assume hourly data
            hourly_pattern = []
            for hour in range(24):
                hour_values = values[hour::24]
                if len(hour_values) > 0:
                    hourly_pattern.append(np.mean(hour_values))
                else:
                    hourly_pattern.append(self.mean_value)
            self.seasonal_pattern = np.array(hourly_pattern)
    
    def predict(self, data: pd.DataFrame) -> Union[np.ndarray, pd.Series]:
        """Make time series predictions."""
        if self.trained_data is None:
            raise ValueError("Model not trained")
        
        n_predictions = len(data)
        predictions = []
        
        for i in range(n_predictions):
            # Base prediction using trend
            trend_component = self.mean_value + (self.trend_slope * i)
            
            # Add seasonal component if available
            seasonal_component = 0
            if self.seasonal_pattern is not None:
                hour_of_day = i % 24
                seasonal_component = self.seasonal_pattern[hour_of_day] - self.mean_value
            
            prediction = trend_component + seasonal_component
            predictions.append(prediction)
        
        return np.array(predictions)
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance (trend and seasonal components)."""
        return {
            "trend": abs(self.trend_slope),
            "seasonal": np.std(self.seasonal_pattern) if self.seasonal_pattern is not None else 0,
            "baseline": abs(self.mean_value)
        }


class SimpleAnomalyDetector(MLModel):
    """Simple anomaly detector using statistical methods."""
    
    def __init__(self, contamination: float = 0.1, window_size: int = 50):
        """Initialize anomaly detector."""
        self.contamination = contamination
        self.window_size = window_size
        self.threshold_upper = None
        self.threshold_lower = None
        self.mean = 0.0
        self.std = 1.0
        
    def train(self, data: pd.DataFrame, target: Optional[str] = None) -> None:
        """Train the anomaly detector."""
        if target and target in data.columns:
            values = data[target].values
        else:
            # Use first numeric column
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                raise ValueError("No numeric columns found for anomaly detection training")
            values = data[numeric_cols[0]].values
        
        self.mean = np.mean(values)
        self.std = np.std(values)
        
        # Use z-score based thresholds
        z_threshold = 2.5  # Roughly corresponds to 99% confidence
        self.threshold_upper = self.mean + (z_threshold * self.std)
        self.threshold_lower = self.mean - (z_threshold * self.std)
    
    def predict(self, data: pd.DataFrame) -> Union[np.ndarray, pd.Series]:
        """Detect anomalies in new data."""
        if self.threshold_upper is None:
            raise ValueError("Model not trained")
        
        # Get first numeric column
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            return np.zeros(len(data))
        
        values = data[numeric_cols[0]].values
        
        # Calculate anomaly scores (distance from normal range)
        anomaly_scores = np.zeros(len(values))
        
        for i, value in enumerate(values):
            if value > self.threshold_upper:
                anomaly_scores[i] = (value - self.threshold_upper) / self.std
            elif value < self.threshold_lower:
                anomaly_scores[i] = (self.threshold_lower - value) / self.std
            else:
                anomaly_scores[i] = 0.0
        
        return anomaly_scores
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance."""
        return {
            "statistical_deviation": 1.0,
            "mean_baseline": abs(self.mean),
            "std_sensitivity": self.std
        }


class TimeSeriesAnalyzer:
    """Advanced time series analysis capabilities."""
    
    def __init__(self):
        """Initialize time series analyzer."""
        self.logger = get_logger(__name__)
    
    def analyze_trend(self, data: pd.Series, window: int = 30) -> Dict[str, Any]:
        """Analyze trend in time series data."""
        try:
            values = data.values
            timestamps = data.index if hasattr(data.index, 'to_pydatetime') else range(len(data))
            
            # Linear trend
            x = np.arange(len(values))
            slope, intercept = np.polyfit(x, values, 1)
            
            # Moving average trend
            if len(values) >= window:
                ma = data.rolling(window=window).mean()
                ma_trend = (ma.iloc[-1] - ma.iloc[window]) / (len(ma) - window)
            else:
                ma_trend = slope
            
            # Trend strength (R-squared)
            y_pred = slope * x + intercept
            ss_res = np.sum((values - y_pred) ** 2)
            ss_tot = np.sum((values - np.mean(values)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            return {
                "slope": float(slope),
                "intercept": float(intercept),
                "r_squared": float(r_squared),
                "trend_strength": "strong" if r_squared > 0.7 else "moderate" if r_squared > 0.3 else "weak",
                "direction": "increasing" if slope > 0 else "decreasing" if slope < 0 else "stable",
                "moving_average_trend": float(ma_trend)
            }
            
        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            return {"error": str(e)}
    
    def detect_seasonality(self, data: pd.Series, max_period: int = 168) -> Dict[str, Any]:
        """Detect seasonal patterns in time series data."""
        try:
            values = data.values
            
            if len(values) < max_period * 2:
                return {"seasonal": False, "reason": "insufficient_data"}
            
            # Simple autocorrelation-based seasonality detection
            autocorrelations = []
            periods = []
            
            for period in range(2, min(max_period + 1, len(values) // 2)):
                # Calculate autocorrelation at this lag
                mean_val = np.mean(values)
                shifted = np.roll(values, period)
                
                # Skip incomplete periods
                valid_length = len(values) - period
                if valid_length < period:
                    continue
                
                numerator = np.sum((values[:valid_length] - mean_val) * (shifted[:valid_length] - mean_val))
                denominator = np.sqrt(np.sum((values[:valid_length] - mean_val) ** 2) * 
                                    np.sum((shifted[:valid_length] - mean_val) ** 2))
                
                if denominator > 0:
                    correlation = numerator / denominator
                    autocorrelations.append(abs(correlation))
                    periods.append(period)
            
            if not autocorrelations:
                return {"seasonal": False, "reason": "no_valid_periods"}
            
            # Find strongest seasonal period
            max_correlation_idx = np.argmax(autocorrelations)
            best_period = periods[max_correlation_idx]
            best_correlation = autocorrelations[max_correlation_idx]
            
            # Determine if seasonal (threshold)
            seasonal_threshold = 0.3
            is_seasonal = best_correlation > seasonal_threshold
            
            return {
                "seasonal": is_seasonal,
                "period": int(best_period) if is_seasonal else None,
                "strength": float(best_correlation),
                "confidence": "high" if best_correlation > 0.7 else "medium" if best_correlation > 0.5 else "low"
            }
            
        except Exception as e:
            self.logger.error(f"Seasonality detection failed: {e}")
            return {"seasonal": False, "error": str(e)}
    
    def forecast(
        self,
        data: pd.Series,
        periods: int,
        confidence_level: float = 0.95
    ) -> TimeSeriesForecast:
        """Generate time series forecast."""
        try:
            # Use simple exponential smoothing for forecast
            values = data.values
            
            # Simple exponential smoothing parameters
            alpha = 0.3  # Smoothing parameter
            
            # Initialize
            smoothed = [values[0]]
            
            # Calculate smoothed values
            for i in range(1, len(values)):
                smoothed.append(alpha * values[i] + (1 - alpha) * smoothed[i-1])
            
            # Generate forecasts
            forecasts = []
            last_smoothed = smoothed[-1]
            
            # Simple trend component
            if len(values) > 10:
                trend = (values[-1] - values[-10]) / 10
            else:
                trend = 0
            
            for i in range(periods):
                forecast = last_smoothed + trend * i
                forecasts.append(forecast)
            
            # Calculate confidence intervals (simple approach)
            residuals = np.array(values[1:]) - np.array(smoothed[1:])
            residual_std = np.std(residuals)
            
            z_score = 1.96 if confidence_level == 0.95 else 2.58  # 95% or 99%
            
            confidence_lower = [f - z_score * residual_std * np.sqrt(i + 1) for i, f in enumerate(forecasts)]
            confidence_upper = [f + z_score * residual_std * np.sqrt(i + 1) for i, f in enumerate(forecasts)]
            
            # Generate future timestamps
            if hasattr(data.index, 'freq') and data.index.freq:
                freq = data.index.freq
                future_timestamps = pd.date_range(
                    start=data.index[-1] + freq,
                    periods=periods,
                    freq=freq
                ).tolist()
            else:
                # Assume hourly frequency if no frequency detected
                future_timestamps = [
                    data.index[-1] + timedelta(hours=i+1) for i in range(periods)
                ] if hasattr(data.index, 'to_pydatetime') else list(range(len(data), len(data) + periods))
            
            return TimeSeriesForecast(
                timestamps=future_timestamps,
                values=forecasts,
                confidence_lower=confidence_lower,
                confidence_upper=confidence_upper,
                model_info={
                    "method": "exponential_smoothing",
                    "alpha": alpha,
                    "trend": trend,
                    "confidence_level": confidence_level,
                    "residual_std": residual_std
                }
            )
            
        except Exception as e:
            self.logger.error(f"Forecasting failed: {e}")
            # Return empty forecast on error
            return TimeSeriesForecast(
                timestamps=[],
                values=[],
                confidence_lower=[],
                confidence_upper=[],
                model_info={"error": str(e)}
            )


class AnomalyDetector:
    """Advanced anomaly detection capabilities."""
    
    def __init__(self):
        """Initialize anomaly detector."""
        self.logger = get_logger(__name__)
    
    def detect_point_anomalies(
        self,
        data: pd.Series,
        method: str = "zscore",
        threshold: float = 3.0,
        window_size: int = 50
    ) -> List[AnomalyResult]:
        """Detect point anomalies in data."""
        try:
            anomalies = []
            values = data.values
            timestamps = data.index if hasattr(data.index, 'to_pydatetime') else range(len(data))
            
            if method == "zscore":
                mean_val = np.mean(values)
                std_val = np.std(values)
                
                for i, (timestamp, value) in enumerate(zip(timestamps, values)):
                    z_score = abs((value - mean_val) / std_val) if std_val > 0 else 0
                    
                    if z_score > threshold:
                        confidence = min(z_score / threshold, 2.0) / 2.0  # Normalize to 0-1
                        anomalies.append(AnomalyResult(
                            timestamp=timestamp,
                            value=float(value),
                            anomaly_score=float(z_score),
                            anomaly_type=AnomalyType.POINT,
                            confidence=confidence,
                            context={"method": "zscore", "threshold": threshold}
                        ))
            
            elif method == "iqr":
                q1, q3 = np.percentile(values, [25, 75])
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                for i, (timestamp, value) in enumerate(zip(timestamps, values)):
                    if value < lower_bound or value > upper_bound:
                        distance = min(abs(value - lower_bound), abs(value - upper_bound))
                        anomaly_score = distance / iqr if iqr > 0 else 0
                        confidence = min(anomaly_score / 2, 1.0)
                        
                        anomalies.append(AnomalyResult(
                            timestamp=timestamp,
                            value=float(value),
                            anomaly_score=float(anomaly_score),
                            anomaly_type=AnomalyType.POINT,
                            confidence=confidence,
                            context={"method": "iqr", "q1": q1, "q3": q3, "iqr": iqr}
                        ))
            
            elif method == "rolling":
                if len(values) >= window_size:
                    rolling_mean = pd.Series(values).rolling(window=window_size).mean()
                    rolling_std = pd.Series(values).rolling(window=window_size).std()
                    
                    for i, (timestamp, value) in enumerate(zip(timestamps, values)):
                        if i >= window_size:  # Skip initial window
                            mean_val = rolling_mean.iloc[i]
                            std_val = rolling_std.iloc[i]
                            
                            if std_val > 0:
                                z_score = abs((value - mean_val) / std_val)
                                
                                if z_score > threshold:
                                    confidence = min(z_score / threshold, 2.0) / 2.0
                                    anomalies.append(AnomalyResult(
                                        timestamp=timestamp,
                                        value=float(value),
                                        anomaly_score=float(z_score),
                                        anomaly_type=AnomalyType.CONTEXTUAL,
                                        confidence=confidence,
                                        context={"method": "rolling", "window_size": window_size}
                                    ))
            
            self.logger.info(f"Detected {len(anomalies)} anomalies using {method} method")
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Point anomaly detection failed: {e}")
            return []
    
    def detect_collective_anomalies(
        self,
        data: pd.Series,
        min_size: int = 5,
        max_size: int = 50
    ) -> List[AnomalyResult]:
        """Detect collective anomalies (unusual subsequences)."""
        try:
            anomalies = []
            values = data.values
            timestamps = data.index if hasattr(data.index, 'to_pydatetime') else range(len(data))
            
            # Simple collective anomaly detection using subsequence deviation
            overall_mean = np.mean(values)
            overall_std = np.std(values)
            
            for size in range(min_size, min(max_size + 1, len(values) - min_size)):
                for start in range(len(values) - size + 1):
                    subsequence = values[start:start + size]
                    subseq_mean = np.mean(subsequence)
                    subseq_std = np.std(subsequence)
                    
                    # Check if subsequence is anomalous
                    mean_deviation = abs(subseq_mean - overall_mean) / overall_std if overall_std > 0 else 0
                    std_deviation = abs(subseq_std - overall_std) / overall_std if overall_std > 0 else 0
                    
                    combined_score = (mean_deviation + std_deviation) / 2
                    
                    if combined_score > 2.0:  # Threshold for collective anomaly
                        mid_point = start + size // 2
                        confidence = min(combined_score / 3.0, 1.0)
                        
                        anomalies.append(AnomalyResult(
                            timestamp=timestamps[mid_point],
                            value=float(np.mean(subsequence)),
                            anomaly_score=float(combined_score),
                            anomaly_type=AnomalyType.COLLECTIVE,
                            confidence=confidence,
                            context={
                                "method": "collective",
                                "subsequence_size": size,
                                "start_index": start,
                                "end_index": start + size - 1
                            }
                        ))
            
            # Remove overlapping anomalies (keep highest scoring)
            if anomalies:
                anomalies.sort(key=lambda x: x.anomaly_score, reverse=True)
                filtered_anomalies = []
                
                for anomaly in anomalies:
                    # Check if this anomaly overlaps with already selected ones
                    start_idx = anomaly.context["start_index"]
                    end_idx = anomaly.context["end_index"]
                    
                    overlaps = False
                    for selected in filtered_anomalies:
                        selected_start = selected.context["start_index"]
                        selected_end = selected.context["end_index"]
                        
                        if (start_idx <= selected_end and end_idx >= selected_start):
                            overlaps = True
                            break
                    
                    if not overlaps:
                        filtered_anomalies.append(anomaly)
                
                anomalies = filtered_anomalies
            
            self.logger.info(f"Detected {len(anomalies)} collective anomalies")
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Collective anomaly detection failed: {e}")
            return []


class AdvancedAnalytics:
    """Main advanced analytics interface."""
    
    def __init__(self):
        """Initialize advanced analytics."""
        self.logger = get_logger(__name__)
        self.time_series_analyzer = TimeSeriesAnalyzer()
        self.anomaly_detector = AnomalyDetector()
    
    def analyze_data(self, data: pd.DataFrame, target_column: str = None) -> Dict[str, Any]:
        """Perform comprehensive data analysis."""
        try:
            results = {
                "data_shape": data.shape,
                "columns": list(data.columns),
                "data_types": data.dtypes.to_dict(),
                "missing_values": data.isnull().sum().to_dict(),
                "numeric_summary": {},
                "time_series_analysis": {},
                "anomaly_detection": {}
            }
            
            # Basic descriptive statistics for numeric columns
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                results["numeric_summary"][col] = {
                    "mean": float(data[col].mean()),
                    "std": float(data[col].std()),
                    "min": float(data[col].min()),
                    "max": float(data[col].max()),
                    "median": float(data[col].median()),
                    "q25": float(data[col].quantile(0.25)),
                    "q75": float(data[col].quantile(0.75))
                }
            
            # Time series analysis if applicable
            if target_column and target_column in data.columns:
                target_series = data[target_column]
                
                # Trend analysis
                results["time_series_analysis"]["trend"] = self.time_series_analyzer.analyze_trend(target_series)
                
                # Seasonality detection
                results["time_series_analysis"]["seasonality"] = self.time_series_analyzer.detect_seasonality(target_series)
                
                # Anomaly detection
                point_anomalies = self.anomaly_detector.detect_point_anomalies(target_series)
                collective_anomalies = self.anomaly_detector.detect_collective_anomalies(target_series)
                
                results["anomaly_detection"] = {
                    "point_anomalies": len(point_anomalies),
                    "collective_anomalies": len(collective_anomalies),
                    "total_anomalies": len(point_anomalies) + len(collective_anomalies),
                    "anomaly_details": [
                        {
                            "timestamp": str(a.timestamp),
                            "value": a.value,
                            "score": a.anomaly_score,
                            "type": a.anomaly_type.value,
                            "confidence": a.confidence
                        }
                        for a in (point_anomalies + collective_anomalies)[:10]  # Top 10 anomalies
                    ]
                }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Data analysis failed: {e}")
            return {"error": str(e)}
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of analytics components."""
        return {
            "status": "healthy",
            "components": {
                "time_series_analyzer": "ready",
                "anomaly_detector": "ready"
            },
            "capabilities": [
                "trend_analysis",
                "seasonality_detection", 
                "forecasting",
                "point_anomaly_detection",
                "collective_anomaly_detection",
                "statistical_analysis"
            ]
        }