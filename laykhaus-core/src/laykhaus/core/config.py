"""
Configuration management for LaykHaus platform.
"""

from functools import lru_cache
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with validation."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Environment
    environment: str = Field(default="development", description="Deployment environment")
    debug: bool = Field(default=False, description="Debug mode")

    # Application
    app_name: str = "LaykHaus"
    app_version: str = "0.1.0"
    api_prefix: str = "/api/v1"
    
    # File-based storage for connectors
    connectors_file: str = Field(
        default="/data/connectors.json",
        description="Path to connectors JSON file"
    )
    
    # Default host for connectors (container names in Docker)
    default_postgres_host: str = Field(
        default="demo-postgres",
        description="Default PostgreSQL host"
    )
    default_kafka_host: str = Field(
        default="kafka",
        description="Default Kafka host"
    )
    default_rest_api_host: str = Field(
        default="demo-rest-api",
        description="Default REST API host"
    )
    
    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="kafka:29092",
        description="Kafka bootstrap servers"
    )
    kafka_consumer_group: str = Field(
        default="laykhaus-consumer",
        description="Default Kafka consumer group ID"
    )
    
    # Spark
    SPARK_MASTER_URL: Optional[str] = Field(
        default=None,
        description="Spark master URL (None for local mode)"
    )
    kafka_topics: List[str] = Field(
        default=["stock-prices", "news-articles", "social-posts", "user-activities"],
        description="Kafka topics to subscribe"
    )
    schema_registry_url: str = Field(
        default="http://schema-registry:8081",
        description="Schema Registry URL"
    )
    
    # MinIO/S3
    minio_endpoint: str = Field(default="minio:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_secure: bool = Field(default=False)
    
    # SQL Gateway
    sql_gateway_host: str = Field(default="0.0.0.0")
    sql_gateway_port: int = Field(default=5432)
    sql_max_connections: int = Field(default=100, ge=1, le=1000)
    
    # GraphQL/REST API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    
    # Security
    jwt_secret_key: str = Field(
        default="your-secret-key-change-in-production",
        description="Secret key for JWT tokens"
    )
    jwt_algorithm: str = Field(default="HS256")
    access_token_expire_minutes: int = Field(default=30, ge=1)
    
    # Performance
    max_query_timeout_seconds: int = Field(default=30, ge=1, le=300)
    max_concurrent_queries: int = Field(default=200, ge=1, le=1000)
    connection_pool_size: int = Field(default=50, ge=1, le=200)
    
    # Query Optimization
    enable_query_optimization: bool = Field(default=True)
    pushdown_coverage_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    cost_estimation_enabled: bool = Field(default=True)
    
    # Observability
    jaeger_agent_host: str = Field(default="jaeger")
    jaeger_agent_port: int = Field(default=6831)
    prometheus_metrics_port: int = Field(default=9091)
    enable_tracing: bool = Field(default=True)
    enable_metrics: bool = Field(default=True)
    log_level: str = Field(default="INFO")
    
    # Feature Flags
    enable_real_time_subscriptions: bool = Field(default=True)
    enable_security_policies: bool = Field(default=True)
    enable_cost_tracking: bool = Field(default=True)
    enable_ai_features: bool = Field(default=False)
    
    # Testing
    test_mode: bool = Field(default=False)
    use_test_containers: bool = Field(default=False)
    
    @field_validator("environment")
    def validate_environment(cls, v: str) -> str:
        """Validate environment value."""
        allowed = ["development", "staging", "production", "testing"]
        if v not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v
    
    @field_validator("log_level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v
    
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == "development"
    
    @property
    def is_testing(self) -> bool:
        """Check if running in test mode."""
        return self.test_mode or self.environment == "testing"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Global settings instance
settings = get_settings()