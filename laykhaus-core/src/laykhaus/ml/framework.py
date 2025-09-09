"""
Machine Learning Framework for LaykHaus.

Provides model management, registry, and integration capabilities.
"""

import asyncio
import json
import pickle
import hashlib
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union, Protocol
from pathlib import Path
from enum import Enum
import numpy as np
import pandas as pd

from laykhaus.core.logging import get_logger
from laykhaus.core.config import get_settings

logger = get_logger(__name__)


class ModelType(Enum):
    """Supported machine learning model types."""
    REGRESSION = "regression"
    CLASSIFICATION = "classification" 
    TIME_SERIES = "time_series"
    ANOMALY_DETECTION = "anomaly_detection"
    CLUSTERING = "clustering"
    NLP = "nlp"


class ModelStatus(Enum):
    """Model lifecycle status."""
    TRAINING = "training"
    READY = "ready"
    DEPLOYED = "deployed"
    RETIRED = "retired"
    ERROR = "error"


@dataclass
class ModelMetadata:
    """Metadata for ML models."""
    name: str
    version: str
    model_type: ModelType
    description: str
    created_at: datetime
    updated_at: datetime
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    features: List[str] = field(default_factory=list)
    target: Optional[str] = None
    status: ModelStatus = ModelStatus.TRAINING
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "version": self.version,
            "model_type": self.model_type.value,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metrics": self.metrics,
            "parameters": self.parameters,
            "features": self.features,
            "target": self.target,
            "status": self.status.value
        }


class MLModel(Protocol):
    """Protocol for machine learning models."""
    
    def train(self, data: pd.DataFrame, target: Optional[str] = None) -> None:
        """Train the model on provided data."""
        ...
    
    def predict(self, data: pd.DataFrame) -> Union[np.ndarray, pd.Series]:
        """Make predictions on new data."""
        ...
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance scores."""
        ...


class ModelRegistry:
    """Registry for managing ML models."""
    
    def __init__(self, storage_path: str = "models/"):
        """Initialize model registry."""
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        self.logger = get_logger(__name__)
        self.models: Dict[str, ModelMetadata] = {}
        self._load_registry()
    
    def _get_model_key(self, name: str, version: str) -> str:
        """Generate unique key for model."""
        return f"{name}:{version}"
    
    def _get_model_path(self, name: str, version: str) -> Path:
        """Get storage path for model."""
        return self.storage_path / f"{name}_{version}.pkl"
    
    def _get_metadata_path(self, name: str, version: str) -> Path:
        """Get metadata path for model."""
        return self.storage_path / f"{name}_{version}_metadata.json"
    
    def _load_registry(self):
        """Load existing models from storage."""
        try:
            for metadata_file in self.storage_path.glob("*_metadata.json"):
                with open(metadata_file, 'r') as f:
                    metadata_dict = json.load(f)
                
                metadata = ModelMetadata(
                    name=metadata_dict["name"],
                    version=metadata_dict["version"],
                    model_type=ModelType(metadata_dict["model_type"]),
                    description=metadata_dict["description"],
                    created_at=datetime.fromisoformat(metadata_dict["created_at"]),
                    updated_at=datetime.fromisoformat(metadata_dict["updated_at"]),
                    metrics=metadata_dict.get("metrics", {}),
                    parameters=metadata_dict.get("parameters", {}),
                    features=metadata_dict.get("features", []),
                    target=metadata_dict.get("target"),
                    status=ModelStatus(metadata_dict.get("status", "ready"))
                )
                
                key = self._get_model_key(metadata.name, metadata.version)
                self.models[key] = metadata
                
            self.logger.info(f"Loaded {len(self.models)} models from registry")
                
        except Exception as e:
            self.logger.error(f"Failed to load model registry: {e}")
    
    def register_model(
        self,
        model: MLModel,
        metadata: ModelMetadata
    ) -> bool:
        """Register a new model in the registry."""
        try:
            key = self._get_model_key(metadata.name, metadata.version)
            
            # Save model
            model_path = self._get_model_path(metadata.name, metadata.version)
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            
            # Save metadata
            metadata_path = self._get_metadata_path(metadata.name, metadata.version)
            with open(metadata_path, 'w') as f:
                json.dump(metadata.to_dict(), f, indent=2)
            
            # Update registry
            metadata.status = ModelStatus.READY
            metadata.updated_at = datetime.utcnow()
            self.models[key] = metadata
            
            self.logger.info(f"Registered model {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register model {metadata.name}:{metadata.version}: {e}")
            return False
    
    def get_model(self, name: str, version: str = "latest") -> Optional[MLModel]:
        """Retrieve a model from the registry."""
        try:
            if version == "latest":
                # Find latest version
                matching_models = [
                    (k, v) for k, v in self.models.items()
                    if v.name == name and v.status == ModelStatus.READY
                ]
                if not matching_models:
                    return None
                
                # Sort by created_at and get latest
                latest = max(matching_models, key=lambda x: x[1].created_at)
                version = latest[1].version
            
            key = self._get_model_key(name, version)
            if key not in self.models:
                return None
            
            model_path = self._get_model_path(name, version)
            if not model_path.exists():
                self.logger.error(f"Model file not found: {model_path}")
                return None
            
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            
            return model
            
        except Exception as e:
            self.logger.error(f"Failed to load model {name}:{version}: {e}")
            return None
    
    def get_metadata(self, name: str, version: str = "latest") -> Optional[ModelMetadata]:
        """Get model metadata."""
        if version == "latest":
            matching_models = [
                v for v in self.models.values()
                if v.name == name and v.status == ModelStatus.READY
            ]
            if not matching_models:
                return None
            return max(matching_models, key=lambda x: x.created_at)
        
        key = self._get_model_key(name, version)
        return self.models.get(key)
    
    def list_models(self) -> List[ModelMetadata]:
        """List all registered models."""
        return list(self.models.values())
    
    def delete_model(self, name: str, version: str) -> bool:
        """Delete a model from the registry."""
        try:
            key = self._get_model_key(name, version)
            if key not in self.models:
                return False
            
            # Remove files
            model_path = self._get_model_path(name, version)
            metadata_path = self._get_metadata_path(name, version)
            
            if model_path.exists():
                model_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()
            
            # Remove from registry
            del self.models[key]
            
            self.logger.info(f"Deleted model {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete model {name}:{version}: {e}")
            return False


class ModelManager:
    """Manages ML model lifecycle and operations."""
    
    def __init__(self, registry: Optional[ModelRegistry] = None):
        """Initialize model manager."""
        self.registry = registry or ModelRegistry()
        self.logger = get_logger(__name__)
        self.active_models: Dict[str, MLModel] = {}
    
    async def train_model(
        self,
        model_class: type,
        name: str,
        data: pd.DataFrame,
        model_type: ModelType,
        description: str = "",
        features: Optional[List[str]] = None,
        target: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Train a new model asynchronously."""
        try:
            # Generate version based on data and parameters hash
            data_hash = hashlib.md5(str(data.values.tobytes()).encode()).hexdigest()[:8]
            param_hash = hashlib.md5(str(parameters or {}).encode()).hexdigest()[:8]
            version = f"v{datetime.utcnow().strftime('%Y%m%d')}_{data_hash}_{param_hash}"
            
            # Create model instance
            model = model_class(**(parameters or {}))
            
            # Create metadata
            metadata = ModelMetadata(
                name=name,
                version=version,
                model_type=model_type,
                description=description,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                parameters=parameters or {},
                features=features or list(data.columns),
                target=target,
                status=ModelStatus.TRAINING
            )
            
            self.logger.info(f"Starting training for model {name}:{version}")
            
            # Train model (run in thread pool for CPU-bound task)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, model.train, data, target)
            
            # Calculate metrics if possible
            try:
                if hasattr(model, 'score') and target:
                    score = model.score(data, data[target] if target in data.columns else None)
                    metadata.metrics["training_score"] = float(score)
                
                if hasattr(model, 'get_feature_importance'):
                    importance = model.get_feature_importance()
                    metadata.metrics["feature_importance"] = importance
                    
            except Exception as e:
                self.logger.warning(f"Could not calculate metrics: {e}")
            
            # Register model
            success = self.registry.register_model(model, metadata)
            
            if success:
                self.logger.info(f"Successfully trained and registered model {name}:{version}")
                return version
            else:
                self.logger.error(f"Failed to register model {name}:{version}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to train model {name}: {e}")
            return None
    
    def load_model(self, name: str, version: str = "latest") -> Optional[MLModel]:
        """Load a model into memory."""
        key = f"{name}:{version}"
        
        if key in self.active_models:
            return self.active_models[key]
        
        model = self.registry.get_model(name, version)
        if model:
            self.active_models[key] = model
            self.logger.info(f"Loaded model {key} into memory")
        
        return model
    
    async def predict(
        self,
        name: str,
        data: pd.DataFrame,
        version: str = "latest"
    ) -> Optional[Union[np.ndarray, pd.Series]]:
        """Make predictions using a model."""
        try:
            model = self.load_model(name, version)
            if not model:
                self.logger.error(f"Model {name}:{version} not found")
                return None
            
            # Run prediction in thread pool for CPU-bound task
            loop = asyncio.get_event_loop()
            predictions = await loop.run_in_executor(None, model.predict, data)
            
            return predictions
            
        except Exception as e:
            self.logger.error(f"Prediction failed for {name}:{version}: {e}")
            return None
    
    def get_model_info(self, name: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """Get comprehensive model information."""
        metadata = self.registry.get_metadata(name, version)
        if not metadata:
            return None
        
        return {
            "metadata": metadata.to_dict(),
            "loaded": f"{name}:{version}" in self.active_models,
            "storage_path": str(self.registry._get_model_path(name, version))
        }


class MLFramework:
    """Main ML framework for LaykHaus."""
    
    def __init__(self):
        """Initialize ML framework."""
        self.logger = get_logger(__name__)
        self.registry = ModelRegistry()
        self.manager = ModelManager(self.registry)
        self.logger.info("ML Framework initialized")
    
    def get_manager(self) -> ModelManager:
        """Get model manager instance."""
        return self.manager
    
    def get_registry(self) -> ModelRegistry:
        """Get model registry instance."""
        return self.registry
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of ML framework."""
        models = self.registry.list_models()
        
        return {
            "status": "healthy",
            "total_models": len(models),
            "ready_models": len([m for m in models if m.status == ModelStatus.READY]),
            "deployed_models": len([m for m in models if m.status == ModelStatus.DEPLOYED]),
            "active_models": len(self.manager.active_models),
            "storage_path": str(self.registry.storage_path)
        }