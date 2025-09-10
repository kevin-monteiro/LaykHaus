"""
REST API Connector for LaykHaus Platform.
Provides connectivity to REST API endpoints with pagination and authentication support.
"""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import httpx
from pydantic import BaseModel

from laykhaus.connectors.base import (
    BaseConnector,
    ConnectionConfig,
    CapabilityContract,
    HealthStatus,
    QueryResult,
    SQLFeature,
    PushdownType,
    ConnectorType,
    TransactionLevel,
)
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class RESTAPIConnector(BaseConnector):
    """
    REST API connector for external HTTP/HTTPS endpoints.
    Supports various authentication methods and data formats.
    """
    
    def __init__(self, connector_id: str, config: ConnectionConfig):
        """Initialize REST API connector."""
        super().__init__(connector_id, config)
        self.client: Optional[httpx.AsyncClient] = None
        self.base_url = config.extra_params.get("base_url", f"http://{config.host}:{config.port}")
        self.auth_type = config.extra_params.get("auth_type", "none")
        self.auth_config = config.extra_params.get("auth_config", {})
        self.timeout = httpx.Timeout(config.query_timeout)
        self._capability_contract = self._define_capabilities()
    
    def get_capabilities(self) -> CapabilityContract:
        """Get connector capabilities."""
        return self._capability_contract
    
    def _define_capabilities(self) -> CapabilityContract:
        """Define REST API connector capabilities."""
        return CapabilityContract(
            connector_id=self.connector_id,
            connector_type=ConnectorType.REST_API,
            sql_features={
                SQLFeature.SELECT,
                SQLFeature.WHERE,
                SQLFeature.LIMIT,
                SQLFeature.ORDER_BY,
            },
            pushdown_capabilities={
                PushdownType.FILTER,
                PushdownType.LIMIT,
                PushdownType.PROJECTION,
            },
            supports_streaming=False,
            supports_schema_discovery=True,
            transaction_support=TransactionLevel.NONE,
            max_concurrent_connections=10,
        )
    
    async def connect(self) -> None:
        """Establish connection to REST API endpoint."""
        try:
            logger.info(f"Connecting to REST API: {self.base_url}")
            
            # Create client with authentication
            headers = {}
            auth = None
            
            if self.auth_type == "bearer":
                token = self.auth_config.get("token")
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            elif self.auth_type == "api_key":
                api_key = self.auth_config.get("api_key")
                key_header = self.auth_config.get("header_name", "X-API-Key")
                if api_key:
                    headers[key_header] = api_key
            elif self.auth_type == "basic":
                username = self.auth_config.get("username")
                password = self.auth_config.get("password")
                if username and password:
                    auth = httpx.BasicAuth(username, password)
            
            self.client = httpx.AsyncClient(
                headers=headers,
                auth=auth,
                timeout=self.timeout,
                base_url=self.base_url
            )
            
            # Test connection with a simple request
            response = await self.client.get("/")
            if response.status_code >= 400:
                raise ConnectionError(f"Failed to connect: HTTP {response.status_code}")
            
            self._connected = True
            logger.info(f"Successfully connected to REST API: {self.base_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to REST API: {e}")
            self._connected = False
            if self.client:
                await self.client.aclose()
                self.client = None
            raise
    
    async def disconnect(self) -> None:
        """Close REST API connection."""
        try:
            if self.client:
                await self.client.aclose()
                self.client = None
            self._connected = False
            logger.info(f"Disconnected from REST API: {self.base_url}")
        except Exception as e:
            logger.error(f"Error disconnecting from REST API: {e}")
            raise
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> QueryResult:
        """
        Execute a REST API request.
        The query should be a JSON string with endpoint and parameters.
        """
        if not self._connected or not self.client:
            raise ConnectionError("Not connected to REST API")
        
        try:
            # Parse query as JSON with endpoint and method
            if query.startswith("{"):
                query_config = json.loads(query)
                endpoint = query_config.get("endpoint", "/")
                method = query_config.get("method", "GET").upper()
                query_params = query_config.get("params", {})
                body = query_config.get("body", None)
            else:
                # Simple endpoint string
                endpoint = query
                method = "GET"
                query_params = params or {}
                body = None
            
            url = urljoin(self.base_url, endpoint)
            
            # Execute request
            if endpoint.startswith("/"):
                # Use relative URL for base_url
                response = await self.client.request(
                    method=method,
                    url=endpoint,
                    params=query_params,
                    json=body
                )
            else:
                # Full URL
                response = await self.client.request(
                    method=method,
                    url=url,
                    params=query_params,
                    json=body
                )
            response.raise_for_status()
            data = response.json()
            
            # Format response as table-like result
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                # If dict has a data field, use it
                if "data" in data:
                    rows = data["data"] if isinstance(data["data"], list) else [data["data"]]
                else:
                    rows = [data]
            else:
                rows = [{"result": data}]
            
            # Extract columns from first row
            columns = list(rows[0].keys()) if rows else []
            
            return QueryResult(
                data=rows,
                columns=columns,
                row_count=len(rows),
                execution_time_ms=0,  # Would need timing logic
                statistics={}
            )
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid query format: {e}")
            raise ValueError(f"Query must be valid JSON or endpoint string: {e}")
        except httpx.HTTPError as e:
            logger.error(f"REST API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing REST API request: {e}")
            raise
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get available endpoints and their schemas.
        Schema can be provided in connector configuration or discovered automatically.
        """
        # Check if schema was provided in extra_params during connector registration
        if self.config.extra_params and 'schema' in self.config.extra_params:
            return self.config.extra_params['schema']
        
        # Try to discover schema from OpenAPI spec
        schema = await self._discover_schema()
        if schema:
            return schema
        
        # Return minimal default schema
        return {
            "endpoints": {
                "default": {
                    "description": "REST API endpoint",
                    "endpoint": "/",
                    "method": "GET"
                }
            }
        }
    
    async def _discover_schema(self) -> Optional[Dict[str, Any]]:
        """
        Try to discover schema from OpenAPI/Swagger spec.
        """
        openapi_endpoints = ["/openapi.json", "/swagger.json", "/api-docs", "/docs/api.json"]
        
        for endpoint in openapi_endpoints:
            try:
                response = await self.client.get(endpoint)
                if response.status_code == 200:
                    spec = response.json()
                    # Parse OpenAPI spec to extract clean schema
                    return self._parse_openapi_spec(spec)
            except:
                continue
        
        return None
    
    def _parse_openapi_spec(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse OpenAPI spec into a clean schema for SQL queries.
        """
        schema = {"endpoints": {}}
        
        if "paths" in spec:
            for path, methods in spec["paths"].items():
                # Skip internal/technical endpoints
                if path in ["/", "/health", "/openapi.json", "/docs", "/redoc"]:
                    continue
                
                # Extract table name from path (e.g., /api/users -> users)
                table_name = path.split('/')[-1].replace('-', '_')
                if '{' in table_name:  # Skip parameterized endpoints
                    continue
                
                for method, details in methods.items():
                    if method.upper() == "GET":
                        schema["endpoints"][table_name] = {
                            "endpoint": path,
                            "method": "GET",
                            "description": details.get("summary", "")
                        }
                        
                        # Try to extract response schema
                        if "responses" in details and "200" in details["responses"]:
                            response = details["responses"]["200"]
                            if "content" in response and "application/json" in response["content"]:
                                if "schema" in response["content"]["application/json"]:
                                    response_schema = response["content"]["application/json"]["schema"]
                                    # Extract column information if available
                                    columns = self._extract_columns_from_schema(response_schema)
                                    if columns:
                                        schema["endpoints"][table_name]["columns"] = columns
        
        return schema if schema["endpoints"] else None
    
    def _extract_columns_from_schema(self, schema_def: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Extract column definitions from OpenAPI schema.
        """
        columns = {}
        
        # Handle array responses
        if schema_def.get("type") == "array" and "items" in schema_def:
            schema_def = schema_def["items"]
        
        # Extract properties
        if "properties" in schema_def:
            for prop_name, prop_def in schema_def["properties"].items():
                prop_type = prop_def.get("type", "string")
                # Map OpenAPI types to SQL types
                sql_type = {
                    "integer": "integer",
                    "number": "float",
                    "string": "varchar",
                    "boolean": "boolean",
                    "array": "json",
                    "object": "json"
                }.get(prop_type, "varchar")
                columns[prop_name] = sql_type
        
        return columns if columns else None
    
    async def health_check(self) -> HealthStatus:
        """Check REST API health."""
        if not self._connected or not self.client:
            return HealthStatus(
                healthy=False,
                message="Not connected",
                latency_ms=0,
                last_check=datetime.utcnow().isoformat()
            )
        
        try:
            import time
            start = time.time()
            
            # Try health endpoint
            response = await self.client.get(
                "/health",
                timeout=5.0
            )
            if response.status_code < 400:
                latency = (time.time() - start) * 1000
                return HealthStatus(
                    healthy=True,
                    message=f"REST API is healthy (HTTP {response.status_code})",
                    latency_ms=latency,
                    last_check=datetime.utcnow().isoformat()
                )
            
            return HealthStatus(
                healthy=False,
                message="No healthy endpoint found",
                latency_ms=0,
                last_check=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            return HealthStatus(
                healthy=False,
                message=f"Health check failed: {str(e)}",
                latency_ms=0,
                last_check=datetime.utcnow().isoformat()
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get connector statistics."""
        return {
            "connector_id": self.connector_id,
            "connected": self._connected,
            "base_url": self.base_url,
            "auth_type": self.auth_type,
            "total_requests": 0,  # Would need to track this
            "failed_requests": 0,  # Would need to track this
        }
    
    async def validate_contract(self) -> List[str]:
        """Validate REST API connector contract."""
        errors = []
        
        if not self.base_url:
            errors.append("Base URL is required")
        
        if self.auth_type not in ["none", "bearer", "api_key", "basic"]:
            errors.append(f"Unsupported auth type: {self.auth_type}")
        
        if self.auth_type == "bearer" and not self.auth_config.get("token"):
            errors.append("Bearer token is required for bearer auth")
        
        if self.auth_type == "api_key" and not self.auth_config.get("api_key"):
            errors.append("API key is required for api_key auth")
        
        if self.auth_type == "basic":
            if not self.auth_config.get("username") or not self.auth_config.get("password"):
                errors.append("Username and password are required for basic auth")
        
        return errors